#!/usr/bin/env python3
"""
Book-stream vs GET /order max size, plotted over time.   [v6 — tokens]

    pip install websocket-client requests matplotlib
    python book_order_v6_tokens.py

Phase 1 (10s)  listen to the book stream, take the median max as x
Phase 2        book thread + order thread record independently to CSV
Phase 3        two line charts: book max vs order max, buy and sell

v6: BOTH sides are now measured in TOKENS, so no price conversion sits between
    the book line and the order line.
      buy  — request is still CASH via ExactIn (sizes unchanged), but the chart
             plots outAmount (tokens received) against the ask ladder's max_tokens
      sell — unchanged: tokens in via ExactIn, against the bid ladder's max_tokens

    If you see "1x = 5,075" and "CASH in" on the buy axis, you are running v5.
    v6 prints "ladder token total" during phase 1 — check for that line.
"""
import csv, json, os, sys, threading, time
from concurrent.futures import ThreadPoolExecutor, wait as futures_wait
from datetime import datetime, timezone

try:
    import requests
    from websocket import create_connection, WebSocketTimeoutException
except ImportError:
    sys.exit("Run first:  pip install websocket-client requests matplotlib")

# ============================== FILL THESE IN ==============================
API_KEY = "7JdilITpkIj0D2pfKVq6"

MINT = "67r1PciCLNHrT5iK15jy2mj3KEByTbgGHFgqB3eNBE36"
# ===========================================================================

CALIBRATE_SEC = 10

# log-spaced low end (so a collapsing book is still measurable) + fine high end
MULTIPLIERS = [0.01, 0.03, 0.05, 0.1, 0.2, 0.3, 0.5, 0.7,
               1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.5, 3.0]

ORDER_EVERY_SEC = 3.0        # gap between rounds
ROUND_TIMEOUT   = 1.2        # hard cap on a round: anything still in flight is
                             # recorded as round_timeout instead of dragging the window
CONNECT_TIMEOUT = 1.0        # TCP/TLS connect cap
READ_TIMEOUT    = 1.2        # server response cap
LOG_Y           = False      # True = log scale (better for the 200x grid range)
Y_TICK_STEP     = None       # y gridline interval. None = auto, picks a round number
                             # giving ~16 lines. Only hardcode if you know the range.
X_TICK_MS       = 300        # x gridline interval in ms. Gridlines are always drawn
                             # at this spacing; labels thin out so they stay readable.
ZOOM_LAST_SEC   = None       # None = plot the WHOLE run. Set e.g. 30 to zoom the tail.

QUOTE_API = "https://quote-api.world.xyz"
WORLD_API = "https://api.world.xyz"
STREAM    = "wss://quote-api.dflow.net/book-stream"
CASH      = "CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH"
DEC       = 10**6

BOOK_COLS  = ["ts", "iso", "mint", "side", "book_u", "n_levels",
              "max_tokens", "max_cash", "vwap", "ladder_json"]
ORDER_COLS = ["round_id", "round_ts", "round_iso", "send_iso", "recv_iso", "mint",
              "side", "mult", "size_human", "size_base", "ok", "http",
              "in_human", "out_human", "avg_price", "latency_ms", "note"]

latest, lock, stop = {}, threading.Lock(), threading.Event()
HDRS = {}
SESSION = None


def build_session(n):
    """One keep-alive session with a big pool. Without this every request pays
    a fresh TCP + TLS handshake, which is most of the straggler latency."""
    s = requests.Session()
    ad = requests.adapters.HTTPAdapter(pool_connections=n + 4, pool_maxsize=n + 4,
                                       max_retries=0)
    s.mount("https://", ad)
    s.headers.update(HDRS)
    s.headers["Connection"] = "keep-alive"
    return s


def utc(t=None):
    t = t or time.time()
    return t, datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="milliseconds")


def ladder_totals(levels):
    tok = cash = prev = 0.0
    for price, cum in levels:
        step = cum - prev
        if step > 0:
            tok += step
            cash += step * price
            prev = cum
    return tok, cash, (cash / tok if tok else 0.0)


def parse_side(raw, reverse):
    return sorted([(float(l["price"]), float(l["cumulative_size_human"]))
                   for l in (raw or [])], key=lambda x: x[0], reverse=reverse)


# ---------------------------------------------------------------- book thread
def book_thread(bw):
    backoff = 1
    while not stop.is_set():
        try:
            ws = create_connection(STREAM, header=HDRS, timeout=30)
            ws.send(json.dumps({"op": "subscribe", "base_mint": MINT, "quote_mint": CASH}))
            print("  [book] connected")
            backoff = 1
            while not stop.is_set():
                ws.settimeout(5)
                try:
                    msg = ws.recv()
                except WebSocketTimeoutException:
                    continue
                if not msg:
                    break
                frame = json.loads(msg)
                ts, iso = utc()
                for b in frame.get("updates") or []:
                    if b.get("sb") != MINT:
                        continue
                    if b.get("e"):
                        print(f"  [book] unavailable: {b['e']}")
                        continue
                    ask, bid = parse_side(b.get("a"), False), parse_side(b.get("b"), True)
                    at, ac, av = ladder_totals(ask)
                    bt_, bc, bv = ladder_totals(bid)
                    with lock:
                        latest.update(ts=ts, buy_max=ac, buy_max_tok=at, sell_max=bt_)
                    for side, lv, tok, cash, v in (("ask", ask, at, ac, av),
                                                   ("bid", bid, bt_, bc, bv)):
                        bw.writerow(dict(ts=round(ts, 3), iso=iso, mint=MINT, side=side,
                                         book_u=frame.get("u"), n_levels=len(lv),
                                         max_tokens=round(tok, 6), max_cash=round(cash, 6),
                                         vwap=round(v, 8), ladder_json=json.dumps(lv)))
            try:
                ws.close()
            except Exception:
                pass
        except Exception as e:
            if stop.is_set():
                return
            print(f"  [book] {type(e).__name__}: {e} — retry in {backoff}s")
            stop.wait(backoff)
            backoff = min(backoff * 2, 30)


# --------------------------------------------------------------- order thread
def one_order(job):
    _, job["send_iso"] = utc()
    t0 = time.time()
    try:
        r = SESSION.get(f"{QUOTE_API}/order",
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), params={
            "inputMint": job["_im"], "outputMint": job["_om"],
            "amount": str(job["size_base"]), "slippageBps": "auto",
            "swapMode": "ExactIn"})
        try:
            j = r.json()
        except Exception:
            j = {}
        ok = r.ok and "inAmount" in j and "outAmount" in j
        ia = int(j["inAmount"]) if ok else None
        oa = int(j["outAmount"]) if ok else None
        job.update(ok=ok, http=r.status_code,
                   in_human=round(ia / DEC, 6) if ia else None,
                   out_human=round(oa / DEC, 6) if oa else None,
                   avg_price=round(oa / ia, 8) if (ia and oa) else None,
                   note="" if ok else str(j.get("code") or j.get("errorCode")
                                          or j.get("msg") or r.text[:60]))
    except requests.RequestException as e:
        job.update(ok=False, http=0, in_human=None, out_human=None,
                   avg_price=None, note=f"transport {type(e).__name__}")
    job["latency_ms"] = int((time.time() - t0) * 1000)
    job["recv_iso"] = utc()[1]
    return job


def order_thread(ow, sizes, deadline, pool):
    rid = 0
    while time.time() < deadline and not stop.is_set():
        rid += 1
        rts, riso = utc()
        jobs = []
        for side, im, om in (("buy", CASH, MINT), ("sell", MINT, CASH)):
            for mult, amt in sizes[side]:
                jobs.append(dict(round_id=rid, round_ts=round(rts, 3), round_iso=riso,
                                 mint=MINT, side=side, mult=mult,
                                 size_human=round(amt, 6), size_base=int(amt * DEC),
                                 _im=im, _om=om))
        futs = {pool.submit(one_order, j): j for j in jobs}
        fin, pend = futures_wait(futs, timeout=ROUND_TIMEOUT)
        done = [f.result() for f in fin]
        for f in pend:                      # never came back inside the window
            f.cancel()
            j = futs[f]
            j.update(ok=False, http=0, in_human=None, out_human=None,
                     avg_price=None, note="round_timeout",
                     latency_ms=int(ROUND_TIMEOUT * 1000),
                     recv_iso=utc()[1])
            done.append(j)
        for j in done:
            ow.writerow(j)
        lats = sorted(j["latency_ms"] for j in done)
        window = max(lats) if lats else 0
        line = []
        for side in ("buy", "sell"):
            g = [j for j in done if j["side"] == side]
            pat = "".join("X" if next((x["ok"] for x in g
                                       if abs(x["mult"] - m) < 1e-9), False) else "."
                          for m in MULTIPLIERS)
            mx = max([j["size_human"] for j in g if j["ok"]], default=0)
            line.append(f"{side} {pat} max {mx:>9,.0f}")
        print(f"  round {rid:>3}  " + "   ".join(line) +
              f"   | window {window}ms (p50 {lats[len(lats)//2]}ms, "
              f"{len(pend)} timeout)  {max(0, deadline - time.time())/60:.1f}m left")
        stop.wait(ORDER_EVERY_SEC)


# ------------------------------------------------------------------- charting
def plot(bpath, opath, base):
    try:
        import matplotlib
        matplotlib.use("TkAgg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib.ticker as mticker
        import math
    except Exception as e:
        print(f"\n(charts skipped: {e})")
        return

    def nice_step(hi, target=16):
        if hi <= 0:
            return 1
        raw = hi / target
        mag = 10 ** math.floor(math.log10(raw))
        for m in (1, 2, 2.5, 5):
            if raw <= m * mag:
                return m * mag
        return 10 * mag

    bk = list(csv.DictReader(open(bpath)))
    od = list(csv.DictReader(open(opath)))
    if not od:
        print("no order rows to plot")
        return

    def dt(ts):
        return datetime.fromtimestamp(float(ts), timezone.utc)

    # ---- optional zoom to the last N seconds
    t_end = max(float(r["ts"]) for r in bk)
    t_from = (t_end - ZOOM_LAST_SEC) if ZOOM_LAST_SEC else 0

    # Both sides are now measured in TOKENS, so the book line and the order line
    # share a unit and no price conversion sits between them.
    #   buy : request is still CASH (ExactIn), but we plot outAmount = tokens received
    #   sell: request is tokens (ExactIn), plotted as-is
    series = {}
    for side in ("buy", "sell"):
        tag = "ask" if side == "buy" else "bid"
        okcol = "out_human" if side == "buy" else "size_human"
        bx = [dt(r["ts"]) for r in bk if r["side"] == tag and float(r["ts"]) >= t_from]
        by = [float(r["max_tokens"]) for r in bk
              if r["side"] == tag and float(r["ts"]) >= t_from]
        ox, oy = [], []
        for rid in sorted({r["round_id"] for r in od}, key=int):
            g = [r for r in od if r["round_id"] == rid and r["side"] == side]
            if not g or float(g[0]["round_ts"]) < t_from:
                continue
            f = [float(r[okcol]) for r in g if r["ok"] == "True" and r[okcol]]
            ox.append(dt(g[0]["round_ts"]))
            oy.append(max(f) if f else float("nan"))
        series[side] = (bx, by, ox, oy)

    fig, axes = plt.subplots(2, 1, figsize=(19, 11), sharex=True)
    fig.suptitle(f"Book-stream max vs /order max  —  {MINT[:12]}…"
                 + (f"   (last {ZOOM_LAST_SEC}s)" if ZOOM_LAST_SEC else ""),
                 fontsize=16, fontweight="bold", y=0.985)
    print(f"\n  1x reference: buy = {base['buy_tok']:,.2f} tokens out "
          f"(sent as {base['buy']:,.2f} CASH)   "
          f"sell = {base['sell']:,.2f} tokens in")

    for ax, side, unit in ((axes[0], "buy", "tokens out"),
                           (axes[1], "sell", "tokens in")):
        bx, by, ox, oy = series[side]
        ax.plot(bx, by, lw=2.0, color="#1f77b4", label="book stream max", zorder=2)
        ax.plot(ox, oy, lw=2.6, color="#d62728", marker="o", ms=7,
                label="/order max filled", zorder=3)

        ref = base["buy_tok"] if side == "buy" else base["sell"]
        ax.set_ylabel(f"{side.upper()}  ({unit})\n1x = {ref:,.0f}",
                      fontsize=13, fontweight="bold")
        ax.tick_params(labelsize=10)
        vals = [v for v in by + oy if v == v]
        hi = max(vals) if vals else 1
        if LOG_Y:
            ax.set_yscale("log")
            lo = min(v for v in vals if v > 0) if any(v > 0 for v in vals) else 1
            ax.set_ylim(lo * 0.7, hi * 1.4)
        else:
            ax.set_ylim(0, hi * 1.05)
            step = Y_TICK_STEP or nice_step(hi * 1.05)
            if hi / step > 30:               # would be an unreadable smear
                auto = nice_step(hi * 1.05)
                print(f"  y ({side}): step {step:g} gives {hi/step:.0f} lines on a "
                      f"{hi:,.0f} range — using {auto:g} instead")
                step = auto
            ax.yaxis.set_major_locator(mticker.MultipleLocator(step))
            ax.yaxis.set_minor_locator(mticker.MultipleLocator(step / 2))
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(
            lambda v, _: f"{v:,.0f}" if v >= 1 else f"{v:g}"))

        ax.grid(True, which="major", axis="y", alpha=0.40, ls="-", lw=0.7)
        ax.grid(True, which="minor", axis="y", alpha=0.16, ls=":", lw=0.6)
        ax.grid(True, which="major", axis="x", alpha=0.30, ls="-", lw=0.6)
        ax.legend(loc="lower left", bbox_to_anchor=(0, 1.01), ncol=2,
                  frameon=False, fontsize=11, handlelength=2.4, borderaxespad=0)

    # ---- x axis at X_TICK_MS resolution
    span_s = (t_end - t_from) if ZOOM_LAST_SEC else (
        t_end - min(float(r["ts"]) for r in bk))
    n_ticks = span_s * 1000 / X_TICK_MS
    MAX_LABELS = 26
    if n_ticks <= 1200:
        # gridlines at the requested resolution, labels thinned to stay legible
        axes[1].xaxis.set_major_locator(
            mdates.MicrosecondLocator(interval=int(X_TICK_MS * 1000)))
        every = max(1, int(round(n_ticks / MAX_LABELS)))

        def msfmt(x, pos):
            if pos is None or pos % every:
                return ""
            d = mdates.num2date(x)
            return d.strftime("%H:%M:%S.") + f"{d.microsecond // 1000:03d}"
        axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(msfmt))
        print(f"  x: {n_ticks:.0f} gridlines at {X_TICK_MS}ms over {span_s:.0f}s, "
              f"labelling every {every}")
    else:
        axes[1].xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=20))
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        print(f"  x: {n_ticks:.0f} gridlines at {X_TICK_MS}ms is too dense over "
              f"{span_s:.0f}s — using auto ticks. Set ZOOM_LAST_SEC to zoom in.")

    axes[1].set_xlabel("time (UTC)", fontsize=12, labelpad=8)
    for lbl in axes[1].get_xticklabels():
        lbl.set_rotation(90)
        lbl.set_ha("center")
        lbl.set_fontsize(8.5)
    fig.subplots_adjust(left=0.07, right=0.99, top=0.91, bottom=0.14, hspace=0.30)

    png = opath.replace("orders_", "chart_").replace(".csv", ".png")
    fig.savefig(png, dpi=140)
    print(f"\nchart saved: {os.path.abspath(png)}")
    plt.show()


def text_summary(bpath, opath):
    od = list(csv.DictReader(open(opath)))
    bk = list(csv.DictReader(open(bpath)))
    if not od:
        return

    print(f"\n{'='*78}\nFILL RATE PER SIZE\n{'='*78}")
    print(f"{'side':<6}" + "".join(f"{m:>8g}x" for m in MULTIPLIERS))
    for side in ("buy", "sell"):
        cells = []
        for m in MULTIPLIERS:
            g = [r for r in od if r["side"] == side and abs(float(r["mult"]) - m) < 1e-9]
            cells.append(f"{sum(r['ok']=='True' for r in g)/len(g)*100:>7.0f}%" if g else "       -")
        print(f"{side:<6}" + "".join(cells))

    # book max at each round, for the ratio
    book = {"buy": [], "sell": []}
    for r in bk:
        book["buy" if r["side"] == "ask" else "sell"].append(
            (float(r["ts"]), float(r["max_tokens"])))
    for k in book:
        book[k].sort()

    def book_at(side, ts):
        best = None
        for t, v in book[side]:
            if t <= ts:
                best = v
            else:
                break
        return best if best is not None else (book[side][0][1] if book[side] else None)

    print(f"\n{'='*78}\nVERDICT  (order max / book max, per round)\n{'='*78}")
    for side in ("buy", "sell"):
        rs, dead, ceil = [], 0, 0
        for rid in sorted({r["round_id"] for r in od}, key=int):
            g = [r for r in od if r["round_id"] == rid and r["side"] == side]
            if not g:
                continue
            vcol = "out_human" if side == "buy" else "size_human"
            f = [(float(r["mult"]), float(r[vcol])) for r in g
                 if r["ok"] == "True" and r[vcol]]
            if not f:
                dead += 1
                continue
            if max(m for m, _ in f) >= max(MULTIPLIERS) - 1e-9:
                ceil += 1
            bm = book_at(side, float(g[0]["round_ts"]))   # tokens on both sides
            if bm:
                rs.append(max(s for _, s in f) / bm)
        if not rs:
            print(f"  {side:<5} nothing ever filled ({dead} dead rounds)")
            continue
        rs.sort()
        med = rs[len(rs) // 2]
        v = ("MATCHES" if 0.9 <= med <= 1.15 else
             f"UNDERSTATES by ~{med:.1f}x" if med > 1.15 else
             f"OVERSTATES — only {med:.2f}x fills")
        print(f"  {side:<5} median {med:>5.2f}x   p10 {rs[int(len(rs)*.1)]:>5.2f}x   "
              f"p90 {rs[int(len(rs)*.9)]:>5.2f}x   n={len(rs)}   book {v}")
        print(f"        {dead} rounds filled nothing at all")

    bad = [r["note"][:34] for r in od if r["ok"] != "True" and r["note"]]
    if bad:
        print("\nrejection reasons:")
        for n in sorted(set(bad), key=bad.count, reverse=True)[:6]:
            print(f"  {bad.count(n):>5}  {n}")


# ----------------------------------------------------------------------- main
def main():
    global HDRS
    if "PASTE" in API_KEY or "PASTE" in MINT:
        sys.exit("Set API_KEY and MINT at the top of the file.")
    HDRS = {"x-api-key": API_KEY.strip()}
    print("=" * 66)
    print("  v6 — both sides measured in TOKENS (buy plots outAmount)")
    print("=" * 66)

    d = input("Duration in minutes (after the 10s calibration) [10]: ").strip()
    dur_s = (float(d) if d else 10.0) * 60

    os.makedirs("data", exist_ok=True)
    tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    bpath, opath = f"data/book_{tag}.csv", f"data/orders_{tag}.csv"
    bfh = open(bpath, "w", newline="", buffering=1)
    ofh = open(opath, "w", newline="", buffering=1)
    bw = csv.DictWriter(bfh, BOOK_COLS, extrasaction="ignore")
    ow = csv.DictWriter(ofh, ORDER_COLS, extrasaction="ignore")
    bw.writeheader(); ow.writeheader()

    try:
        mk = requests.get(f"{WORLD_API}/api/v1/market/by-mint/{MINT}",
                          headers=HDRS, timeout=15).json()
        acc = (mk.get("accounts") or {}).get(CASH, {})
        print(f"\n{mk['ticker']} [{'YES' if acc.get('yesMint') == MINT else 'NO'}] "
              f"status={mk['status']}\n{mk['title']}")
    except Exception as e:
        print(f"\nmarket lookup failed: {e}")
    print(f"\nwriting {os.path.abspath(bpath)}\n        {os.path.abspath(opath)}\n")

    bt = threading.Thread(target=book_thread, args=(bw,), daemon=True)
    bt.start()

    print(f"  [phase 1] listening {CALIBRATE_SEC}s to set order sizes…")
    t_end = time.time() + CALIBRATE_SEC
    samples = {"buy": [], "buy_tok": [], "sell": []}
    while time.time() < t_end and not stop.is_set():
        with lock:
            if latest:
                samples["buy"].append(latest["buy_max"])
                samples["buy_tok"].append(latest["buy_max_tok"])
                samples["sell"].append(latest["sell_max"])
        time.sleep(0.2)
    if not samples["buy"]:
        print("  no book frames in calibration window — check book-stream access")
        stop.set(); bfh.close(); ofh.close(); return

    sizes, base = {}, {}
    for key in ("buy", "buy_tok", "sell"):
        s = sorted(samples[key])
        base[key] = s[len(s) // 2]
    for side in ("buy", "sell"):                 # requests still sized as before
        sizes[side] = [(m, base[side] * m) for m in MULTIPLIERS]
        u = "CASH" if side == "buy" else "tokens"
        print(f"    {side:<5} book max over 10s: min {min(samples[side]):,.2f}  "
              f"median {base[side]:,.2f}  max {max(samples[side]):,.2f}  {u}")
    print(f"    buy  ladder token total (the comparison baseline): "
          f"{base['buy_tok']:,.2f} tokens")
    print(f"  [phase 1] x_buy = {base['buy']:,.2f} CASH   "
          f"x_sell = {base['sell']:,.2f} tokens")
    print(f"    sizes {base['buy']*min(MULTIPLIERS):,.1f} .. "
          f"{base['buy']*max(MULTIPLIERS):,.0f} CASH  /  "
          f"{base['sell']*min(MULTIPLIERS):,.1f} .. "
          f"{base['sell']*max(MULTIPLIERS):,.0f} tokens\n")

    print(f"  [phase 2] recording {dur_s/60:.1f} min, "
          f"{len(MULTIPLIERS)*2} parallel calls every {ORDER_EVERY_SEC}s")
    print(f"    pattern shows {'  '.join(f'{m:g}' for m in MULTIPLIERS)}\n")
    global SESSION
    SESSION = build_session(len(MULTIPLIERS) * 2)
    try:                                    # warm the TLS handshake off the clock
        SESSION.get(f"{QUOTE_API}/order", timeout=(2, 3), params={
            "inputMint": CASH, "outputMint": MINT, "amount": "1000",
            "slippageBps": "auto", "swapMode": "ExactIn"})
        print("    connection pool warmed")
    except Exception:
        pass

    pool = ThreadPoolExecutor(max_workers=len(MULTIPLIERS) * 2)
    ot = threading.Thread(target=order_thread,
                          args=(ow, sizes, time.time() + dur_s, pool), daemon=True)
    ot.start()
    try:
        ot.join()
    except KeyboardInterrupt:
        print("\n  interrupted")
    finally:
        stop.set()
        pool.shutdown(wait=True)
        bt.join(timeout=6)
        bfh.close(); ofh.close()

    text_summary(bpath, opath)
    plot(bpath, opath, base)
    print(f"\nCSVs:\n  {os.path.abspath(bpath)}\n  {os.path.abspath(opath)}")


if __name__ == "__main__":
    main()
