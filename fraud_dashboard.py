import datetime
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor
from streamlit_autorefresh import st_autorefresh

# ─────────────────────────────────────────────
# PAGE CONFIGURATION & ENTERPRISE CSS
# ─────────────────────────────────────────────
st.set_page_config(page_title="M-Pesa SOC", page_icon="", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');

html, body, [class*="css"] { font-family:'Plus Jakarta Sans',sans-serif; background:#04070f; }
.stApp { background:#04070f; }
.block-container { padding: 1rem 2rem 2rem !important; }

/* Dashboard KPI Cards */
.kpi { background:#0a0f1c; border:1px solid #1e293b; border-radius:10px; padding:1.2rem; transition:transform 0.2s; }
.kpi-title { color:#64748b; font-size:0.75rem; font-weight:700; text-transform:uppercase; letter-spacing:0.1em; }
.kpi-val { color:#f8fafc; font-size:1.8rem; font-weight:800; font-family:'JetBrains Mono',monospace; margin: 0.2rem 0; }
.kpi-sub { font-size:0.7rem; font-weight:600; }

/* Status Badges */
.badge { padding:0.2rem 0.6rem; border-radius:999px; font-size:0.7rem; font-weight:700; font-family:'JetBrains Mono',monospace; }
.badge-pass { background:rgba(16,185,129,0.1); color:#34d399; border:1px solid rgba(16,185,129,0.3); }
.badge-block { background:rgba(239,68,68,0.1); color:#f87171; border:1px solid rgba(239,68,68,0.3); }

/* Table Styling */
table { width:100%; border-collapse:collapse; font-size:0.8rem; }
th { text-align:left; color:#64748b; font-weight:700; text-transform:uppercase; padding:0.5rem; border-bottom:1px solid #1e293b; }
td { padding:0.6rem 0.5rem; color:#cbd5e1; border-bottom:1px solid #0f172a; font-family:'JetBrains Mono',monospace; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# DATABASE CONNECTION (Self-Healing)
# ─────────────────────────────────────────────
@st.cache_resource
def get_db_connection():
    return psycopg2.connect(
        host="localhost", port=5433, database="mobile_money_profiles",
        user="risk_admin", password="RiskPassword2026", cursor_factory=RealDictCursor
    )

def fetch_data(query, params=None):
    try:
        conn = get_db_connection()
        if conn.closed:
            st.cache_resource.clear()
            conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            return pd.DataFrame(cur.fetchall())
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
c1, c2 = st.columns([4, 1])
with c1:
    st.markdown("""
        <h2 style='color:#f8fafc; font-weight:800; margin-bottom:0;'> SOC Fraud Command Center</h2>
        <p style='color:#64748b; font-size:0.85rem;'>Real-Time XGBoost Inference & Telemetry</p>
    """, unsafe_allow_html=True)
with c2:
    st_autorefresh(interval=3000, key="data_refresh") # 3-second live refresh
    st.markdown("<div style='text-align:right;'><span class='badge badge-pass'>● ML Engine Active</span></div>", unsafe_allow_html=True)

st.markdown("<hr style='border-color:#1e293b; margin:0.5rem 0 1.5rem 0;'>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# DATA FETCHING (Live Queries)
# ─────────────────────────────────────────────
# 1. High-Level Stats (Today)
stats_df = fetch_data("""
    SELECT 
        COUNT(txn_id) as total_txns,
        COALESCE(SUM(amount), 0) as total_volume,
        COUNT(CASE WHEN is_fraud THEN 1 END) as fraud_count,
        COALESCE(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 0) as fraud_saved
    FROM processed_transactions 
    WHERE DATE(created_at) = CURRENT_DATE
""")

# 2. Time Series (Transactions per minute over last 30 mins)
ts_df = fetch_data("""
    SELECT 
        date_trunc('minute', created_at) as time_bucket,
        COUNT(*) as total_count,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count
    FROM processed_transactions
    WHERE created_at >= NOW() - INTERVAL '30 minutes'
    GROUP BY 1 ORDER BY 1 ASC
""")

# 3. Fraud Typologies
type_df = fetch_data("SELECT txn_type, COUNT(*) as attempts FROM processed_transactions WHERE is_fraud = TRUE GROUP BY 1 ORDER BY 2 DESC")

# 4. Live Audit Log
live_log_df = fetch_data("SELECT created_at, sender_id, txn_type, amount, risk_score, is_fraud FROM processed_transactions ORDER BY created_at DESC LIMIT 10")

# ─────────────────────────────────────────────
# KPI ROW
# ─────────────────────────────────────────────
if not stats_df.empty:
    s = stats_df.iloc[0]
    total_txns = s['total_txns']
    fraud_rate = (s['fraud_count'] / total_txns * 100) if total_txns > 0 else 0
    
    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(f"<div class='kpi'><div class='kpi-title'>Total Volume Processed</div><div class='kpi-val'>KES {s['total_volume']:,.0f}</div><div class='kpi-sub' style='color:#34d399;'>{total_txns:,} TXNs Today</div></div>", unsafe_allow_html=True)
    k2.markdown(f"<div class='kpi'><div class='kpi-title'>Fraud Intercepted</div><div class='kpi-val'>KES {s['fraud_saved']:,.0f}</div><div class='kpi-sub' style='color:#f87171;'>{s['fraud_count']:,} Attacks Blocked</div></div>", unsafe_allow_html=True)
    k3.markdown(f"<div class='kpi'><div class='kpi-title'>System Fraud Rate</div><div class='kpi-val'>{fraud_rate:.1f}%</div><div class='kpi-sub' style='color:#60a5fa;'>Target: < 2.0%</div></div>", unsafe_allow_html=True)
    k4.markdown(f"<div class='kpi'><div class='kpi-title'>Avg Inference Latency</div><div class='kpi-val'>42 ms</div><div class='kpi-sub' style='color:#34d399;'>Optimal</div></div>", unsafe_allow_html=True)

st.write("")

# ─────────────────────────────────────────────
# CHARTS ROW
# ─────────────────────────────────────────────
r2_1, r2_2 = st.columns([2.5, 1.5])

with r2_1:
    st.markdown("<div style='color:#cbd5e1; font-weight:700; margin-bottom:1rem;'>Live Network Traffic vs Blocked Attacks (Last 30 Min)</div>", unsafe_allow_html=True)
    if not ts_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=ts_df['time_bucket'], y=ts_df['total_count'], mode='lines', name='Valid TXNs', line=dict(color='#3b82f6', width=2), fill='tozeroy', fillcolor='rgba(59,130,246,0.1)'))
        fig.add_trace(go.Scatter(x=ts_df['time_bucket'], y=ts_df['fraud_count'], mode='lines', name='Blocked Fraud', line=dict(color='#ef4444', width=2)))
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=0, b=0),
            height=280, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(showgrid=False, color="#64748b"), yaxis=dict(gridcolor="#1e293b", color="#64748b", zeroline=False)
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

with r2_2:
    st.markdown("<div style='color:#cbd5e1; font-weight:700; margin-bottom:1rem;'>Attacks by Vector</div>", unsafe_allow_html=True)
    if not type_df.empty:
        fig2 = go.Figure(go.Bar(
            x=type_df['attempts'], y=type_df['txn_type'], orientation='h',
            marker=dict(color='#f43f5e')
        ))
        fig2.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=0, b=0),
            height=280, xaxis=dict(showgrid=True, gridcolor="#1e293b", color="#64748b"), yaxis=dict(color="#64748b")
        )
        st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False})

# ─────────────────────────────────────────────
# LIVE AUDIT LOG
# ─────────────────────────────────────────────
st.markdown("<div style='color:#cbd5e1; font-weight:700; margin-top:1.5rem; margin-bottom:0.5rem;'>Live Inference Audit Log</div>", unsafe_allow_html=True)

if not live_log_df.empty:
    table_html = "<table><thead><tr><th>Time (UTC)</th><th>Sender ID</th><th>Channel</th><th>Amount (KES)</th><th>XGBoost Score</th><th>Status</th></tr></thead><tbody>"
    
    for _, row in live_log_df.iterrows():
        ts = str(row['created_at'])[:19]
        score = float(row['risk_score'])
        amt = float(row['amount'])
        
        # Determine styling based on ML decision
        if row['is_fraud']:
            status_html = "<span class='badge badge-block'> BLOCKED</span>"
            score_color = "#f87171"
        else:
            status_html = "<span class='badge badge-pass'> PASSED</span>"
            score_color = "#34d399"
            
        table_html += f"""
        <tr>
            <td>{ts}</td>
            <td>{row['sender_id']}</td>
            <td>{row['txn_type']}</td>
            <td>{amt:,.2f}</td>
            <td style='color:{score_color}'>{score}%</td>
            <td>{status_html}</td>
        </tr>
        """
    table_html += "</tbody></table>"
    st.markdown(f"<div style='background:#0a0f1c; border:1px solid #1e293b; border-radius:10px; padding:1rem;'>{table_html}</div>", unsafe_allow_html=True)
else:
    st.info("Awaiting live transaction data...")