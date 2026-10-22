#!/usr/bin/env python3
"""
Radar Long Bot
--------------
Memindai futures USDT di timeframe 15m (dengan konfirmasi tren 1 jam), mencari
dua setup long yang umum dipakai trader tren:

  A. Pullback ke EMA21 di dalam uptrend, lalu candle bullish yang merebut
     kembali EMA21 (RSI mendingin lalu berbalik naik).
  B. Breakout segar di atas range 25 candle terakhir dengan volume besar,
     belum terlalu jauh dari EMA21 (tidak mengejar harga).

Tiap sinyal dikirim ke Telegram lengkap dengan entry, SL, TP1, TP2, ukuran posisi.
Sinyal ditahan kalau BTC sedang risk-off (BTC_FILTER=0 untuk mematikan).

Hanya memakai library bawaan Python 3.8+.

Perintah:
  python3 long_bot.py test    # kirim pesan uji ke Telegram
  python3 long_bot.py once    # scan sekali, cetak hasil, tanpa kirim
  python3 long_bot.py scan    # scan sekali lalu kirim (untuk cron / GitHub Actions)

Token dan chat id diambil dari environment variable. Jangan ditulis di kode.
"""
import os, sys, json, time, html, logging, urllib.request
from concurrent.futures import ThreadPoolExecutor

# ---------------- Pengaturan (environment variable) ----------------
TOKEN      = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
SOURCE     = os.getenv("SOURCE", "bitget").lower()       # bitget | binance
MIN_VOL    = float(os.getenv("MIN_VOL_USDT", "3000000"))  # volume min 24 jam
TOP_N      = int(os.getenv("TOP_N", "250"))
MIN_SCORE  = float(os.getenv("MIN_SCORE", "60"))
COOLDOWN_H = float(os.getenv("COOLDOWN_HOURS", "4"))     # jeda sinyal per koin
MODAL      = float(os.getenv("MODAL_USDT", "100"))
RISK_PCT   = float(os.getenv("RISK_PCT", "2"))
BTC_FILTER = os.getenv("BTC_FILTER", "1") == "1"
SUMMARY    = os.getenv("SUMMARY", "1") == "1"            # ringkasan tiap 15 menit
STATE_FILE = os.getenv("STATE_FILE", "long_state.json")
# --------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("long")


def http(url, payload=None, timeout=15):
    headers = {"User-Agent": "radar-long-bot/1.0"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def retry(fn, tries=3):
    for a in range(tries):
        try:
            return fn()
        except Exception as e:
            if a == tries - 1:
                raise
            time.sleep(6 * (a + 1) if "429" in str(e) else 0.6 * (a + 1))


def f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


# ---------------- Sumber data: (t_ms, o, h, l, c, v) ----------------
def bitget_list():
    d = http("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")["data"]
    return [dict(id=x["symbol"], name=x["symbol"].replace("USDT", ""), price=f(x["lastPr"]),
                 vol=f(x.get("usdtVolume") or x.get("quoteVolume")), fund8=f(x.get("fundingRate")) * 100,
                 link="https://www.bitget.com/futures/usdt/" + x["symbol"]) for x in d]


def bitget_candles(sym):
    url = ("https://api.bitget.com/api/v2/mix/market/candles?symbol=%s"
           "&productType=USDT-FUTURES&granularity=15m&limit=500" % sym)
    d = sorted(http(url)["data"], key=lambda k: int(k[0]))
    return [(int(k[0]), f(k[1]), f(k[2]), f(k[3]), f(k[4]), f(k[5])) for k in d]


def binance_list():
    t = http("https://fapi.binance.com/fapi/v1/ticker/24hr")
    p = http("https://fapi.binance.com/fapi/v1/premiumIndex")
    fm = {x["symbol"]: f(x.get("lastFundingRate")) * 100 for x in p}
    return [dict(id=x["symbol"], name=x["symbol"][:-4], price=f(x["lastPrice"]), vol=f(x["quoteVolume"]),
                 fund8=fm.get(x["symbol"], 0.0), link="https://www.binance.com/en/futures/" + x["symbol"])
            for x in t if x["symbol"].endswith("USDT")]


def binance_candles(sym):
    d = http("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=15m&limit=500" % sym)
    return [(int(k[0]), f(k[1]), f(k[2]), f(k[3]), f(k[4]), f(k[5])) for k in d]


ADAPTERS = {"bitget": (bitget_list, bitget_candles), "binance": (binance_list, binance_candles)}


# ---------------- Indikator ----------------
def ema(c, n):
    k = 2 / (n + 1)
    o = [c[0]]
    for x in c[1:]:
        o.append(x * k + o[-1] * (1 - k))
    return o


def rsi(c, n=14):
    out = [None] * len(c)
    if len(c) <= n:
        return out
    g = l = 0.0
    for i in range(1, n + 1):
        d = c[i] - c[i - 1]
        if d >= 0:
            g += d
        else:
            l -= d
    g /= n; l /= n
    out[n] = 100.0 if l == 0 else 100 - 100 / (1 + g / l)
    for i in range(n + 1, len(c)):
        d = c[i] - c[i - 1]
        g = (g * (n - 1) + max(d, 0)) / n
        l = (l * (n - 1) + max(-d, 0)) / n
        out[i] = 100.0 if l == 0 else 100 - 100 / (1 + g / l)
    return out


def clamp(x, a, b):
    return max(a, min(b, x))


def to_1h(cs):
    out, cur = [], None
    for t, o, h, l, c, v in cs:
        b = t // 3600000
        if cur is None or cur[0] != b:
            if cur:
                out.append(cur)
            cur = [b, o, h, l, c, v, 1]
        else:
            cur[2] = max(cur[2], h); cur[3] = min(cur[3], l); cur[4] = c; cur[5] += v; cur[6] += 1
    if cur:
        out.append(cur)
    if out and out[0][6] < 4:
        out = out[1:]            # buang jam pertama yang tidak lengkap
    return out


def htf(cs):
    """Tren 1 jam dari candle 15m."""
    h1 = to_1h(cs)
    if len(h1) < 60:
        return None
    c = [x[4] for x in h1]
    e20, e50 = ema(c, 20), ema(c, 50)
    return dict(up=c[-1] > e50[-1] and e20[-1] > e50[-1] and e50[-1] > e50[-4],
                down=e20[-1] < e50[-1] and c[-1] < e20[-1])


def near_resistance(h, entry, look=96, k=3):
    n, best = len(h), None
    for i in range(max(k, n - look), n - k):
        if h[i] == max(h[i - k:i + k + 1]) and h[i] > entry * 1.001:
            best = h[i] if best is None else min(best, h[i])
    return best


# ---------------- Analisis satu koin ----------------
def analyze(raw, fund8):
    """Return dict(kind='A'|'B'|'W', ...) atau None."""
    cs = raw[:-1]                                   # buang candle yang masih berjalan
    n = len(cs)
    if n < 150 or fund8 > 0.08:
        return None
    t = [x[0] for x in cs]; o = [x[1] for x in cs]; h = [x[2] for x in cs]
    l = [x[3] for x in cs]; c = [x[4] for x in cs]; v = [x[5] for x in cs]
    trend = htf(cs)
    if not trend or not trend["up"]:
        return None

    price = c[-1]
    e21, e50 = ema(c, 21), ema(c, 50)
    tr = [h[0] - l[0]] + [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, n)]
    atr = sum(tr[-14:]) / 14
    if atr / price < 0.002 or atr / price > 0.05:
        return None
    rs = rsi(c, 14)
    rnow, rprev = rs[-1], rs[-2]
    vma = sum(v[-21:-1]) / 20
    if rnow is None or rprev is None or vma <= 0:
        return None
    volx = v[-1] / vma
    rng = h[-1] - l[-1]
    cpos = (c[-1] - l[-1]) / rng if rng > 0 else 0.5
    body = (c[-1] - o[-1]) / rng if rng > 0 else 0

    # ----- Setup A: pullback ke EMA21 lalu reclaim
    LB = 8
    touched = any(l[i] <= e21[i] + 0.1 * atr for i in range(n - LB, n))
    no_break = all(c[i] >= e50[i] - 0.3 * atr for i in range(n - LB, n))
    iH = max(range(n - 32, n), key=lambda i: h[i])
    iLo = min(range(n - LB, n), key=lambda i: l[i])
    depth = (h[iH] - l[iLo]) / atr
    pullback = touched and no_break and iH < iLo and depth >= 1.5
    rmin = min(x for x in rs[n - LB:n] if x is not None)
    trigger = c[-1] > o[-1] and body >= 0.5 and cpos >= 0.65 and c[-1] > e21[-1] and c[-1] > h[-2]
    rsi_ok = rmin <= 52 and rnow > rprev and rnow <= 72

    # ----- Setup B: breakout segar
    res = max(h[n - 26:n - 1])
    fresh = c[-1] > res * 1.001 and c[-2] <= res
    ext = (price - e21[-1]) / atr
    strong = body >= 0.55 and cpos >= 0.75
    brk = fresh and volx >= 1.8 and strong and ext <= 3.2 and rnow <= 76

    kind = None
    if pullback and rsi_ok and trigger and volx >= 0.9:
        kind = "A"
        sl = l[iLo] - 0.25 * atr
    elif brk:
        kind = "B"
        sl = min(l[-1], res) - 0.3 * atr

    if kind is None:
        # daftar pantauan: hampir jadi tapi belum terkonfirmasi
        if pullback and rmin <= 52 and price <= e21[-1] + 0.5 * atr and not trigger:
            return dict(kind="W", score=40 + clamp((depth - 1.5) / 3, 0, 1) * 10,
                        note="pullback ke EMA21, tunggu candle bullish (RSI %.0f)" % rnow)
        if res - 0.6 * atr <= price <= res and volx >= 1.2:
            return dict(kind="W", score=35 + clamp((volx - 1.2) / 1.5, 0, 1) * 10,
                        note="dekat breakout %s, volume %.1fx" % (fmt(res), volx))
        return None

    entry = price
    sl = min(sl, entry - 0.8 * atr)                  # stop tidak boleh lebih sempit dari 0,8 ATR
    risk = entry - sl
    if risk / entry > 0.06:
        return None
    tp2 = entry + 3.0 * risk
    if kind == "A":
        # TP1 di high sebelum pullback; harus minimal 1R dari entry, kalau tidak, rasio untung terlalu kecil
        swing = h[iH]
        if swing - entry < 1.0 * risk:
            return None
        tp1 = min(entry + 1.5 * risk, swing)
        rz = near_resistance(h, swing * 1.001)       # resistance berikutnya setelah high tersebut
        if rz is not None and (rz - entry) < 1.5 * risk:
            return None
    else:
        tp1 = entry + 1.5 * risk
        rz = near_resistance(h, entry)
        if rz is not None and (rz - entry) < 1.2 * risk:
            return None                              # terhalang resistance dekat
    room = 10 if rz is None else clamp(((rz - entry) / risk - 1.2) / 2, 0, 1) * 10

    if kind == "A":
        s = (25 + (10 if price > e50[-1] else 0) + clamp((depth - 1.5) / 3, 0, 1) * 8 + 8
             + (10 if rmin <= 42 else 6) + clamp((body - 0.5) / 0.4, 0, 1) * 7
             + clamp((volx - 0.9) / 1.1, 0, 1) * 12 + room)
        label = "Pullback ke EMA21"
        tags = ["Uptrend 1J", "Pullback %.1f ATR ke EMA21" % depth, "RSI %.0f naik dari %.0f" % (rnow, rmin),
                "Candle bullish kuat", "Volume %.1fx" % volx]
    else:
        s = (25 + (10 if price > e50[-1] else 0) + 10 + clamp((volx - 1.8) / 2.2, 0, 1) * 18
             + clamp((body - 0.55) / 0.35, 0, 1) * 7 + (1 - ext / 3.2) * 10 + room)
        label = "Breakout segar"
        tags = ["Uptrend 1J", "Tembus %s" % fmt(res), "Volume %.1fx" % volx,
                "Jarak EMA21 %.1f ATR" % ext, "RSI %.0f" % rnow]
    if rnow > 70:
        s -= 8
    if fund8 > 0.04:
        s -= 8
        tags.append("Funding tinggi %.3f%%" % fund8)
    if kind == "A" and tp1 == swing:
        tags.append("TP1 di high sebelum pullback")
    tags.append("Resistance terdekat %s" % fmt(rz) if rz else "Tidak ada resistance dekat")

    return dict(kind=kind, label=label, score=min(100.0, s), entry=entry, sl=sl, tp1=tp1, tp2=tp2,
                risk=risk, sl_pct=risk / entry * 100, r1=(tp1 - entry) / risk, r2=(tp2 - entry) / risk,
                tags=tags, candle_t=t[-1])


# ---------------- Konteks BTC ----------------
def btc_context(candler):
    try:
        raw = candler("BTCUSDT")
        cs = raw[:-1]
        tr_ = htf(cs)
        chg1h = (cs[-1][4] / cs[-5][4] - 1) * 100
        down = bool(tr_ and tr_["down"])
        dump = chg1h <= -1.5
        txt = "BTC 1J %s, 1 jam terakhir %+.1f%%" % ("turun" if down else "naik" if tr_ and tr_["up"] else "sideways", chg1h)
        return dict(risk_off=down or dump, text=txt)
    except Exception as e:
        log.warning("Konteks BTC gagal: %s", e)
        return dict(risk_off=False, text="BTC tidak terbaca")


# ---------------- Telegram ----------------
def fmt(p):
    return "%.2f" % p if p >= 100 else "%.3f" % p if p >= 1 else "%.5f" % p if p >= 0.01 else "%.3g" % p


def send(text):
    url = "https://api.telegram.org/bot%s/sendMessage" % TOKEN
    payload = dict(chat_id=CHAT_ID, text=text, parse_mode="HTML", disable_web_page_preview=True)
    for a in range(3):
        try:
            http(url, payload)
            return True
        except Exception as e:
            log.warning("Kirim Telegram gagal (%s)", type(e).__name__)   # tidak mencetak URL yang berisi token
            time.sleep(2 + a * 3)
    return False


def signal_msg(r, btc):
    risk_usdt = MODAL * RISK_PCT / 100
    notional = risk_usdt / (r["sl_pct"] / 100)
    return "\n".join([
        "🟢 <b>LONG %s</b> | %s | skor %d" % (html.escape(r["name"]), r["label"], round(r["score"])),
        "",
        "Entry: <b>%s</b>" % fmt(r["entry"]),
        "SL: <b>%s</b> (-%.1f%%)" % (fmt(r["sl"]), r["sl_pct"]),
        "TP1: %s (%.1fR)   TP2: %s (%.1fR)" % (fmt(r["tp1"]), r["r1"], fmt(r["tp2"]), r["r2"]),
        "Posisi: %.0f USDT (%.1fx), risiko %.2f USDT" % (notional, notional / MODAL, risk_usdt),
        "",
        html.escape(", ".join(r["tags"])),
        html.escape(btc["text"]),
        "Setelah TP1 kena, geser SL ke entry.",
        r["link"],
    ])


def summary_msg(btc, scanned, new_sig, watch, held):
    lines = ["🕒 <b>Ringkasan 15 menit</b>",
             html.escape(btc["text"]) + (" (risk-off, sinyal ditahan)" if btc["risk_off"] and BTC_FILTER else ""),
             "%d koin dicek, sinyal long baru: %d, pantauan: %d" % (scanned, new_sig, len(watch))]
    if held:
        lines.append("Ditahan karena BTC: " + ", ".join(held[:6]))
    if watch:
        lines.append("")
        lines.append("Pantauan:")
        for r in watch[:6]:
            lines.append("• %s: %s" % (html.escape(r["name"]), html.escape(r["note"])))
    return "\n".join(lines)


# ---------------- State ----------------
def load_state():
    try:
        with open(STATE_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_state(st):
    cut = time.time() - 2 * 86400
    st["coins"] = {k: v for k, v in st.get("coins", {}).items() if v.get("t", 0) > cut}
    with open(STATE_FILE, "w") as fh:
        json.dump(st, fh)


# ---------------- Scan ----------------
def scan(dry=False):
    lister, candler = ADAPTERS[SOURCE]
    coins = [x for x in retry(lister) if x["price"] > 0 and x["vol"] >= MIN_VOL and x["id"] != "BTCUSDT"]
    coins.sort(key=lambda x: -x["vol"])
    coins = coins[:TOP_N]
    btc = btc_context(lambda s: retry(lambda: candler(s)))

    def work(x):
        try:
            time.sleep(0.1)
            r = analyze(retry(lambda: candler(x["id"])), x["fund8"])
            return dict(x, **r) if r else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as ex:
        res = [r for r in ex.map(work, coins) if r]
    sig = sorted([r for r in res if r["kind"] in "AB" and r["score"] + (10 if not btc["risk_off"] else 0) >= MIN_SCORE],
                 key=lambda r: -r["score"])
    watch = sorted([r for r in res if r["kind"] == "W"], key=lambda r: -r["score"])
    log.info("%d koin dicek | sinyal %d | pantauan %d | %s", len(coins), len(sig), len(watch), btc["text"])

    if dry:
        print(btc["text"], "(risk-off)" if btc["risk_off"] else "")
        for r in sig:
            print("LONG %-10s %-18s skor %3d  entry %s  SL %s  TP1 %s  TP2 %s" % (
                r["name"], r["label"], r["score"] + (0 if btc["risk_off"] else 10), fmt(r["entry"]),
                fmt(r["sl"]), fmt(r["tp1"]), fmt(r["tp2"])))
        for r in watch[:10]:
            print("pantau %-10s %s" % (r["name"], r["note"]))
        return

    state = load_state()
    coins_state = state.setdefault("coins", {})
    now = time.time()
    sent, held = 0, []
    for r in sig:
        r["score"] += 10 if not btc["risk_off"] else 0
        prev = coins_state.get(r["id"])
        if prev and now - prev["t"] < COOLDOWN_H * 3600:
            continue
        if BTC_FILTER and btc["risk_off"]:
            held.append(r["name"])
            continue
        if send(signal_msg(r, btc)):
            coins_state[r["id"]] = dict(t=now, kind=r["kind"])
            sent += 1
            log.info("Terkirim: %s %s", r["name"], r["label"])
            time.sleep(1)

    if SUMMARY:
        slot = int(now // 900)
        if state.get("summary_slot") != slot:
            if send(summary_msg(btc, len(coins), sent, watch, held)):
                state["summary_slot"] = slot
    save_state(state)
    log.info("%d sinyal baru dikirim", sent)


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if SOURCE not in ADAPTERS:
        sys.exit("SOURCE harus bitget atau binance")
    if cmd == "once":
        scan(dry=True)
        return
    if not (TOKEN and CHAT_ID):
        sys.exit("Isi TELEGRAM_TOKEN dan TELEGRAM_CHAT_ID dulu.")
    if cmd == "test":
        print("Terkirim" if send("Radar Long Bot terhubung. Tes berhasil.") else "Gagal mengirim, cek token dan chat id")
        return
    scan()


if __name__ == "__main__":
    main()
