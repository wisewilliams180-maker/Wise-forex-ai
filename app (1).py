import json
import queue
import threading
import time
from collections import deque

import streamlit as st
import websocket

APP_TITLE = "WISE FOREX AI"
DEFAULT_APP_ID = "1089"  # Deriv public example app ID; replace with your own registered app ID.
WS_URL = "wss://ws.derivws.com/websockets/v3?app_id={}"

st.set_page_config(page_title=APP_TITLE, page_icon="📈", layout="wide")

# ---------- Styling ----------
st.markdown("""
<style>
.stApp { background: #080d18; color: #e8eefc; }
[data-testid="stHeader"] { background: rgba(8,13,24,.92); }
.block-container { padding-top: 1.4rem; max-width: 1450px; }
.hero { padding: 24px 28px; border: 1px solid #1d2a42; border-radius: 18px;
        background: linear-gradient(120deg,#111d33,#101827 65%,#142b37); }
.hero h1 { margin: 0; color: #f5f8ff; letter-spacing: .08em; font-size: 30px; }
.hero p { color: #9eacc4; margin: 8px 0 0; }
.metric { background:#111a2b; border:1px solid #202d45; padding:16px 18px;
          border-radius:14px; min-height:100px; }
.metric small { color:#93a3bd; text-transform:uppercase; letter-spacing:.08em; }
.metric h2 { color:#f2f6ff; margin:8px 0 0; font-size:25px; }
.panel { background:#101827; border:1px solid #202d45; padding:18px; border-radius:16px; }
.good { color:#37d6a0; } .bad { color:#ff7185; }
div.stButton > button { border-radius:10px; min-height:42px; font-weight:650; }
</style>
""", unsafe_allow_html=True)

# ---------- Session state ----------
for key, default in {
    "events": [],
    "worker": None,
    "stop_event": None,
    "event_queue": queue.Queue(),
    "running": False,
    "balance": "—",
    "currency": "—",
    "account_id": "Not connected",
    "mode": "Demo",
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


def emit(message, kind="info"):
    st.session_state.event_queue.put((time.strftime("%H:%M:%S"), message, kind))


def deriv_request(ws, payload, timeout=15):
    ws.send(json.dumps(payload))
    deadline = time.time() + timeout
    while time.time() < deadline:
        raw = ws.recv()
        data = json.loads(raw)
        if data.get("error"):
            raise RuntimeError(data["error"].get("message", "Deriv API error"))
        if data.get("msg_type") == "ping":
            continue
        if "req_id" in payload and data.get("req_id") != payload["req_id"]:
            continue
        return data
    raise TimeoutError("Deriv did not respond in time")


def fetch_authorized_account(token, app_id):
    ws = websocket.create_connection(WS_URL.format(app_id), timeout=15)
    try:
        result = deriv_request(ws, {"authorize": token, "req_id": 1})
        auth = result.get("authorize", {})
        return auth
    finally:
        ws.close()


def bot_worker(token, app_id, symbol, currency, stake, duration, max_trades,
               max_loss, stop_event, output_queue):
    """EMA crossover signal bot. Uses Deriv demo/live token supplied by the user."""
    ws = None
    trades = 0
    session_pnl = 0.0
    prices = deque(maxlen=80)
    last_signal = None
    try:
        ws = websocket.create_connection(WS_URL.format(app_id), timeout=20)
        auth = deriv_request(ws, {"authorize": token, "req_id": 10}).get("authorize", {})
        output_queue.put(("CONNECTED", f"Connected: {auth.get('loginid','Deriv account')}"))
        output_queue.put(("ACCOUNT", auth))
        deriv_request(ws, {
            "ticks_history": symbol, "count": 60, "end": "latest",
            "style": "ticks", "req_id": 11
        })
        deriv_request(ws, {"ticks": symbol, "subscribe": 1, "req_id": 12})
        output_queue.put(("LOG", f"Watching {symbol}. Waiting for EMA crossover signals."))

        while not stop_event.is_set():
            raw = ws.recv()
            data = json.loads(raw)
            if data.get("error"):
                raise RuntimeError(data["error"].get("message", "Deriv API error"))
            if data.get("msg_type") != "tick":
                continue
            quote = float(data["tick"]["quote"])
            prices.append(quote)
            if len(prices) < 30:
                continue

            vals = list(prices)
            fast = sum(vals[-5:]) / 5
            slow = sum(vals[-20:]) / 20
            signal = "CALL" if fast > slow else "PUT"
            output_queue.put(("TICK", quote, signal, fast, slow))

            # Trade only when direction changes; cap trades and session loss.
            if signal != last_signal:
                last_signal = signal
                if trades >= max_trades:
                    output_queue.put(("LOG", "Maximum trades reached. Bot stopped for safety."))
                    break
                if session_pnl <= -abs(max_loss):
                    output_queue.put(("LOG", "Session loss limit reached. Bot stopped for safety."))
                    break

                proposal = deriv_request(ws, {
                    "proposal": 1,
                    "amount": float(stake),
                    "basis": "stake",
                    "contract_type": signal,
                    "currency": currency,
                    "duration": int(duration),
                    "duration_unit": "t",
                    "symbol": symbol,
                    "req_id": 20 + trades
                }).get("proposal", {})
                proposal_id = proposal.get("id")
                ask_price = float(proposal.get("ask_price", stake))
                if not proposal_id:
                    raise RuntimeError("No trade proposal returned by Deriv.")
                bought = deriv_request(ws, {
                    "buy": proposal_id, "price": ask_price, "req_id": 100 + trades
                }).get("buy", {})
                trades += 1
                output_queue.put(("TRADE", {
                    "time": time.strftime("%H:%M:%S"),
                    "direction": signal,
                    "contract_id": bought.get("contract_id", "—"),
                    "buy_price": bought.get("buy_price", ask_price),
                    "status": "Purchased",
                    "note": "Contract outcome pending"
                }))
                output_queue.put(("LOG", f"{signal} contract purchased. Trade {trades}/{max_trades}."))

                # Wait for contract settlement and collect realized profit/loss.
                contract_id = bought.get("contract_id")
                if contract_id:
                    deriv_request(ws, {
                        "proposal_open_contract": 1,
                        "contract_id": contract_id,
                        "subscribe": 1,
                        "req_id": 200 + trades
                    })
                    settle_deadline = time.time() + max(90, int(duration) * 3)
                    while time.time() < settle_deadline and not stop_event.is_set():
                        update = json.loads(ws.recv())
                        if update.get("msg_type") == "proposal_open_contract":
                            contract = update.get("proposal_open_contract", {})
                            if contract.get("is_sold"):
                                profit = float(contract.get("profit", 0))
                                session_pnl += profit
                                output_queue.put(("SETTLED", contract_id, profit, session_pnl))
                                break
    except Exception as exc:
        output_queue.put(("ERROR", str(exc)))
    finally:
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        output_queue.put(("STOPPED", "Bot worker stopped."))


# ---------- Header ----------
st.markdown("""
<div class="hero">
  <h1>📈 WISE FOREX AI</h1>
  <p>Deriv-connected trading workspace · Signal monitoring · Risk controls</p>
</div>
""", unsafe_allow_html=True)

st.warning(
    "Trading is risky. This is an educational tool, not financial advice. "
    "Start with a Deriv demo account. EMA signals can lose money; profits are not guaranteed."
)

# ---------- Sidebar connection ----------
with st.sidebar:
    st.markdown("## ⚙️ Connection")
    app_id = st.text_input("Deriv App ID", value=DEFAULT_APP_ID,
                           help="For production, register your own app ID in your Deriv account.")
    token = st.text_input("Deriv API token", type="password",
                          help="Create a token with trading permission only if you intend to trade. Never share it.")
    mode = st.radio("Account type", ["Demo", "Real"], horizontal=True)
    real_confirm = False
    if mode == "Real":
        real_confirm = st.checkbox("I understand this can use real money and I authorize live trading.")
    connect_clicked = st.button("Connect account", use_container_width=True)

    st.divider()
    st.markdown("## 🛡️ Risk controls")
    stake = st.number_input("Stake per trade", min_value=0.35, max_value=1000.0,
                            value=1.0, step=0.25)
    max_loss = st.number_input("Maximum session loss", min_value=1.0,
                               max_value=10000.0, value=5.0, step=1.0)
    max_trades = st.slider("Maximum trades per run", 1, 30, 5)
    duration = st.slider("Contract duration (ticks)", 5, 20, 5)

if connect_clicked:
    if not token.strip():
        st.error("Enter your Deriv API token first.")
    elif mode == "Real" and not real_confirm:
        st.error("Confirm the real-money risk before connecting in Real mode.")
    else:
        try:
            account = fetch_authorized_account(token.strip(), app_id.strip())
            st.session_state.account_id = account.get("loginid", "Connected")
            st.session_state.balance = f'{float(account.get("balance", 0)):.2f}'
            st.session_state.currency = account.get("currency", "USD")
            st.session_state.mode = mode
            st.success(f"Connected to {st.session_state.account_id} ({mode}).")
        except Exception as exc:
            st.error(f"Connection failed: {exc}")

# ---------- Main dashboard ----------
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="metric"><small>Account</small><h2>{st.session_state.account_id}</h2></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="metric"><small>Balance at connection</small><h2>{st.session_state.balance} {st.session_state.currency}</h2></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="metric"><small>Mode</small><h2>{st.session_state.mode}</h2></div>', unsafe_allow_html=True)
with c4:
    state_text = "RUNNING" if st.session_state.running else "STOPPED"
    st.markdown(f'<div class="metric"><small>Bot status</small><h2>{state_text}</h2></div>', unsafe_allow_html=True)

st.write("")
left, right = st.columns([1.15, 0.85])
with left:
    st.markdown("### 🤖 Trading bot")
    symbol = st.selectbox("Market", ["R_100", "R_75", "R_50", "R_25", "R_10",
                                     "1HZ100V", "1HZ75V", "1HZ50V"])
    currency = st.selectbox("Trade currency", ["USD", "EUR", "GBP", "KES"])
    st.caption("Strategy: 5-tick vs 20-tick simple moving-average crossover. It is a basic demonstration, not a proven strategy.")
    start_col, stop_col = st.columns(2)
    with start_col:
        start_clicked = st.button("▶ Start bot", type="primary", use_container_width=True,
                                  disabled=st.session_state.running)
    with stop_col:
        stop_clicked = st.button("■ Stop bot", use_container_width=True,
                                  disabled=not st.session_state.running)

    if start_clicked:
        if not token.strip():
            st.error("Connect your Deriv account first.")
        elif st.session_state.account_id == "Not connected":
            st.error("Press Connect account before starting.")
        elif st.session_state.mode == "Real" and not real_confirm:
            st.error("Live trading authorization is not confirmed.")
        else:
            stop_event = threading.Event()
            worker = threading.Thread(
                target=bot_worker,
                args=(token.strip(), app_id.strip(), symbol, currency, stake,
                      duration, max_trades, max_loss, stop_event,
                      st.session_state.event_queue),
                daemon=True
            )
            st.session_state.stop_event = stop_event
            st.session_state.worker = worker
            st.session_state.running = True
            worker.start()
            st.rerun()

    if stop_clicked and st.session_state.stop_event:
        st.session_state.stop_event.set()
        st.session_state.running = False
        st.info("Stop requested. The bot will finish its current API operation and stop.")

with right:
    st.markdown("### 📡 Live signal")
    st.info("The latest price and signal will appear here while the bot is running.")
    st.caption("CALL = upward crossover · PUT = downward crossover")

# ---------- Event processing ----------
while not st.session_state.event_queue.empty():
    event = st.session_state.event_queue.get()
    kind = event[0]
    if kind == "CONNECTED":
        st.session_state.account_id = event[1]
    elif kind == "ACCOUNT":
        account = event[1]
        st.session_state.balance = f'{float(account.get("balance", 0)):.2f}'
        st.session_state.currency = account.get("currency", "USD")
    elif kind == "TICK":
        _, quote, signal, fast, slow = event
        st.session_state.latest_tick = (quote, signal, fast, slow)
    elif kind == "TRADE":
        st.session_state.events.append(event[1])
    elif kind == "SETTLED":
        _, contract_id, profit, pnl = event
        st.session_state.events.append({
            "time": time.strftime("%H:%M:%S"),
            "direction": "SETTLED",
            "contract_id": contract_id,
            "buy_price": "—",
            "status": f"Profit/Loss: {profit:+.2f}",
            "note": f"Session P/L: {pnl:+.2f}"
        })
    elif kind == "LOG":
        st.session_state.events.append({
            "time": time.strftime("%H:%M:%S"), "direction": "BOT",
            "contract_id": "—", "buy_price": "—",
            "status": event[1], "note": ""
        })
    elif kind == "ERROR":
        st.session_state.events.append({
            "time": time.strftime("%H:%M:%S"), "direction": "ERROR",
            "contract_id": "—", "buy_price": "—",
            "status": event[1], "note": ""
        })
        st.session_state.running = False
    elif kind == "STOPPED":
        st.session_state.running = False

if hasattr(st.session_state, "latest_tick"):
    quote, signal, fast, slow = st.session_state.latest_tick
    a, b, c = st.columns(3)
    a.metric("Latest quote", f"{quote:.5f}")
    b.metric("Current signal", signal)
    c.metric("Fast / slow average", f"{fast:.5f} / {slow:.5f}")

st.markdown("### 🧾 Trade activity")
if st.session_state.events:
    st.dataframe(list(reversed(st.session_state.events[-50:])),
                 use_container_width=True, hide_index=True)
else:
    st.caption("No trades yet. Connect a demo account and start the bot to see activity.")

if st.session_state.running:
    time.sleep(1.2)
    st.rerun()

st.markdown("---")
st.caption("WISE FOREX AI · Independent project · Deriv account and API access are provided by Deriv.")
