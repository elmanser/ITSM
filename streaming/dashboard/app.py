"""
ITSM Intelligence Platform — Dashboard v4
9 Pages · Glassmorphism · Forecasting · Executive View · Team Analytics
"""
import decimal
import os
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

API_URL = os.getenv("API_URL", "http://api:8000")
PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "itsm_dw"),
    user=os.getenv("POSTGRES_USER", "itsm"),
    password=os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026"),
)

PRIORITY_COLORS = {
    "Very High": "#ff4757",
    "High":      "#ff6b35",
    "Medium":    "#ffa726",
    "Low":       "#26de81",
    "Very Low":  "#45aaf2",
}

st.set_page_config(
    page_title="ITSM Intelligence Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;600&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; -webkit-font-smoothing: antialiased; }

[data-testid="stAppViewContainer"] {
    background: #060b17;
    background-image:
        radial-gradient(ellipse 80% 50% at 20% 0%, rgba(124,58,237,.12) 0%, transparent 50%),
        radial-gradient(ellipse 60% 40% at 80% 100%, rgba(6,182,212,.08) 0%, transparent 50%),
        radial-gradient(ellipse 40% 30% at 60% 40%, rgba(16,185,129,.05) 0%, transparent 40%);
    min-height: 100vh;
}
[data-testid="stMain"] { background: transparent !important; }

[data-testid="stSidebar"] {
    background: rgba(6,10,22,0.95) !important;
    backdrop-filter: blur(32px) saturate(1.4);
    border-right: 1px solid rgba(124,58,237,.18) !important;
}
[data-testid="stSidebar"] > div { padding-top: 0 !important; }

h1,h2,h3 { font-family: 'Space Grotesk', sans-serif !important; letter-spacing: -0.03em !important; }
h1 { font-size: 2.2rem !important; font-weight: 700 !important; }
h2 { font-size: 1.5rem !important; font-weight: 600 !important; }
h3 { font-size: 1.15rem !important; font-weight: 600 !important; }

.grad-text {
    background: linear-gradient(135deg, #a78bfa 0%, #38bdf8 50%, #34d399 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text; font-family: 'Space Grotesk', sans-serif;
    font-weight: 700; letter-spacing: -0.03em;
}

@keyframes shimmer { 0% { transform:translateX(-100%) skewX(-15deg); } 100% { transform:translateX(200%) skewX(-15deg); } }
@keyframes card-in { from { opacity:0; transform:translateY(16px); } to { opacity:1; transform:translateY(0); } }
@keyframes glow-pulse { 0%,100% { box-shadow:0 0 20px -4px var(--glow,rgba(124,58,237,.4)); } 50% { box-shadow:0 0 35px -2px var(--glow,rgba(124,58,237,.65)); } }

.kpi-card {
    background: linear-gradient(135deg, rgba(255,255,255,.04) 0%, rgba(255,255,255,.01) 100%);
    border: 1px solid rgba(255,255,255,.08);
    border-top: 1px solid rgba(255,255,255,.14);
    border-radius: 20px; padding: 24px 22px 20px;
    position: relative; overflow: hidden; backdrop-filter: blur(16px);
    animation: card-in .4s ease both, glow-pulse 4s ease-in-out infinite;
    --glow: rgba(124,58,237,.25);
    transition: transform .3s cubic-bezier(.34,1.56,.64,1), border-color .2s;
    cursor: default;
}
.kpi-card::before {
    content:''; position:absolute; top:0; left:0; right:0; height:2px;
    background: linear-gradient(90deg, transparent, var(--accent,#7c3aed), transparent);
    border-radius: 20px 20px 0 0;
}
.kpi-card::after {
    content:''; position:absolute; top:0; left:-75%; width:50%; height:100%;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,.04), transparent);
    animation: shimmer 3.5s ease infinite;
}
.kpi-card:hover { transform:translateY(-5px) scale(1.015); border-color:rgba(255,255,255,.16); }
.kpi-icon { font-size:1.6rem; margin-bottom:10px; display:flex; align-items:center; justify-content:center; filter:drop-shadow(0 0 8px var(--accent,#7c3aed)); }
.kpi-value { font-family:'Space Grotesk',sans-serif; font-size:2.3rem; font-weight:700; letter-spacing:-0.05em; line-height:1; color:var(--accent,#a78bfa); }
.kpi-label { font-size:.72rem; font-weight:600; text-transform:uppercase; letter-spacing:.1em; color:#475569; margin-top:8px; }
.kpi-delta-up { font-size:.75rem; color:#26de81; font-weight:700; margin-top:4px; }
.kpi-delta-down { font-size:.75rem; color:#ff4757; font-weight:700; margin-top:4px; }
.kpi-delta-neutral { font-size:.75rem; color:#64748b; font-weight:600; margin-top:4px; }

.page-hero {
    background: linear-gradient(135deg, rgba(124,58,237,.08) 0%, rgba(6,182,212,.05) 50%, rgba(16,185,129,.04) 100%);
    border: 1px solid rgba(255,255,255,.06); border-radius:24px;
    padding:32px 36px; margin-bottom:28px; position:relative; overflow:hidden;
}
.page-hero::before {
    content:''; position:absolute; top:-50%; right:-20%;
    width:400px; height:400px;
    background: radial-gradient(circle, rgba(124,58,237,.08) 0%, transparent 70%);
    pointer-events:none;
}
.page-hero-title { font-family:'Space Grotesk',sans-serif; font-size:2rem; font-weight:700; letter-spacing:-0.04em; color:#f8fafc; margin:0; }
.page-hero-sub { font-size:.9rem; color:#64748b; margin-top:6px; font-weight:400; }

.sec-label {
    font-size:.72rem; font-weight:700; text-transform:uppercase; letter-spacing:.12em;
    color:#475569; margin:32px 0 12px; display:flex; align-items:center; gap:8px;
}
.sec-label::before { content:''; width:16px; height:2px; background:linear-gradient(90deg,#7c3aed,#06b6d4); border-radius:2px; }

.alert-critical { background:linear-gradient(135deg,rgba(255,71,87,.1),rgba(255,71,87,.03)); border:1px solid rgba(255,71,87,.3); border-left:3px solid #ff4757; border-radius:14px; padding:14px 18px; margin:10px 0; color:#fca5a5; font-size:.88rem; font-weight:500; }
.alert-warn { background:linear-gradient(135deg,rgba(255,167,38,.08),rgba(255,167,38,.02)); border:1px solid rgba(255,167,38,.25); border-left:3px solid #ffa726; border-radius:14px; padding:14px 18px; margin:10px 0; color:#fcd34d; font-size:.88rem; }
.alert-ok { background:linear-gradient(135deg,rgba(38,222,129,.08),rgba(38,222,129,.02)); border:1px solid rgba(38,222,129,.25); border-left:3px solid #26de81; border-radius:14px; padding:14px 18px; margin:10px 0; color:#6ee7b7; font-size:.88rem; }

@keyframes live-ring { 0% { transform:scale(1); opacity:1; } 100% { transform:scale(2.2); opacity:0; } }
.live-badge { display:inline-flex; align-items:center; gap:8px; background:rgba(38,222,129,.1); border:1px solid rgba(38,222,129,.3); border-radius:999px; padding:6px 14px 6px 10px; font-size:.8rem; font-weight:700; color:#26de81; letter-spacing:.06em; }
.live-ring { position:relative; width:10px; height:10px; }
.live-ring-dot { position:absolute; top:0; left:0; width:10px; height:10px; background:#26de81; border-radius:50%; z-index:1; }
.live-ring-pulse { position:absolute; top:0; left:0; width:10px; height:10px; background:#26de81; border-radius:50%; animation:live-ring 1.4s ease-out infinite; }

.predict-result {
    background: linear-gradient(145deg, rgba(255,255,255,.04) 0%, rgba(255,255,255,.01) 100%);
    border:1px solid rgba(255,255,255,.08); border-radius:24px; padding:40px 32px;
    text-align:center; position:relative; overflow:hidden; backdrop-filter:blur(12px);
    min-height:280px; display:flex; flex-direction:column; justify-content:center;
}
.predict-priority { font-family:'Space Grotesk',sans-serif; font-size:3.2rem; font-weight:700; letter-spacing:-0.04em; line-height:1; margin:12px 0; }
.predict-label { font-size:.72rem; font-weight:700; text-transform:uppercase; letter-spacing:.14em; color:#475569; }
.predict-meta { font-size:.85rem; color:#64748b; margin-top:14px; }
.conf-chip { display:inline-block; padding:4px 14px; border-radius:999px; font-size:.78rem; font-weight:700; letter-spacing:.04em; margin-top:10px; }

.exec-card {
    background: linear-gradient(135deg, rgba(255,255,255,.03) 0%, rgba(255,255,255,.01) 100%);
    border:1px solid rgba(255,255,255,.07); border-radius:18px; padding:20px 24px;
    display:flex; align-items:center; gap:16px; margin-bottom:12px;
}
.exec-card-val { font-family:'Space Grotesk',sans-serif; font-size:2rem; font-weight:700; color:#f8fafc; }
.exec-card-label { font-size:.75rem; color:#64748b; text-transform:uppercase; letter-spacing:.08em; font-weight:600; }
.exec-status-green { color:#26de81; font-weight:700; font-size:.8rem; }
.exec-status-red { color:#ff4757; font-weight:700; font-size:.8rem; }
.exec-status-yellow { color:#ffa726; font-weight:700; font-size:.8rem; }

[data-testid="stSidebar"] [data-testid="stRadio"] > div { display:flex; flex-direction:column; gap:4px; }
[data-testid="stSidebar"] [data-testid="stRadio"] label { background:rgba(255,255,255,.02); border:1px solid transparent; border-radius:12px; padding:10px 14px; cursor:pointer; font-size:.88rem; font-weight:500; color:#64748b !important; transition:all .2s ease; }
[data-testid="stSidebar"] [data-testid="stRadio"] label:hover { background:rgba(124,58,237,.08); border-color:rgba(124,58,237,.2); color:#a78bfa !important; }

.stButton > button { background:linear-gradient(135deg,#7c3aed 0%,#4f46e5 100%) !important; color:#fff !important; border:none !important; border-radius:12px !important; font-family:'Inter',sans-serif !important; font-weight:600 !important; font-size:.9rem !important; padding:.55rem 1.4rem !important; box-shadow:0 4px 20px rgba(124,58,237,.35) !important; transition:all .25s cubic-bezier(.34,1.56,.64,1) !important; }
.stButton > button:hover { transform:translateY(-2px) !important; box-shadow:0 8px 28px rgba(124,58,237,.5) !important; }

[data-testid="stForm"] { background:rgba(255,255,255,.025); border:1px solid rgba(255,255,255,.07); border-radius:20px; padding:28px; backdrop-filter:blur(8px); }
.stSlider > div > div > div { background:#7c3aed !important; }

[data-testid="stDataFrame"] { border:1px solid rgba(255,255,255,.06) !important; border-radius:14px !important; overflow:hidden; }
[data-testid="stDataFrame"] th { background:rgba(124,58,237,.12) !important; color:#a78bfa !important; font-size:.78rem !important; font-weight:700 !important; text-transform:uppercase !important; letter-spacing:.08em !important; }
[data-testid="stDataFrame"] td { font-family:'JetBrains Mono',monospace !important; font-size:.82rem !important; }

.stTabs [data-baseweb="tab-list"] { background:transparent; gap:4px; border-bottom:1px solid rgba(255,255,255,.06); }
.stTabs [data-baseweb="tab"] { color:#475569; font-weight:500; font-size:.88rem; padding:10px 18px; border-radius:8px 8px 0 0; transition:color .2s; }
.stTabs [aria-selected="true"] { color:#a78bfa !important; background:rgba(124,58,237,.08) !important; border-bottom:2px solid #7c3aed !important; }

hr { border:none !important; border-top:1px solid rgba(255,255,255,.06) !important; margin:24px 0 !important; }

[data-testid="stMetric"] { background:rgba(255,255,255,.02); border:1px solid rgba(255,255,255,.06); border-radius:14px; padding:14px !important; }
[data-testid="stMetricValue"] { font-family:'Space Grotesk',sans-serif !important; font-size:1.6rem !important; font-weight:700 !important; color:#a78bfa !important; }
[data-testid="stMetricLabel"] { font-size:.72rem !important; text-transform:uppercase !important; letter-spacing:.08em !important; color:#475569 !important; }

::-webkit-scrollbar { width:6px; height:6px; }
::-webkit-scrollbar-track { background:rgba(255,255,255,.02); }
::-webkit-scrollbar-thumb { background:rgba(124,58,237,.4); border-radius:999px; }

.stCaption, .stMarkdown small { color:#475569 !important; font-size:.78rem !important; }
.stInfo { border-radius:12px !important; background:rgba(6,182,212,.07) !important; border-color:rgba(6,182,212,.25) !important; }
.stSpinner > div { border-top-color:#7c3aed !important; }

.status-pill-ok { display:inline-flex; align-items:center; gap:6px; background:rgba(38,222,129,.1); border:1px solid rgba(38,222,129,.3); border-radius:999px; padding:5px 12px; font-size:.78rem; font-weight:700; color:#26de81; }
.status-pill-err { display:inline-flex; align-items:center; gap:6px; background:rgba(255,71,87,.1); border:1px solid rgba(255,71,87,.3); border-radius:999px; padding:5px 12px; font-size:.78rem; font-weight:700; color:#ff4757; }
</style>
""", unsafe_allow_html=True)


# ── DB helpers ─────────────────────────────────────────────────────────────────
def _fetch(sql: str) -> pd.DataFrame:
    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=cols)
            for col in df.columns:
                if not df.empty and df[col].dtype == object:
                    sample = df[col].dropna()
                    if len(sample) and isinstance(sample.iloc[0], decimal.Decimal):
                        df[col] = df[col].astype(float)
            return df

@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    try: return _fetch(sql)
    except Exception as e: st.error(f"DB error: {e}"); return pd.DataFrame()

@st.cache_data(ttl=12)
def query_live(sql: str) -> pd.DataFrame:
    try: return _fetch(sql)
    except Exception: return pd.DataFrame()

def query_fresh(sql: str) -> pd.DataFrame:
    try: return _fetch(sql)
    except Exception: return pd.DataFrame()


# ── SVG icons ──────────────────────────────────────────────────────────────────
def _svg(paths, size=44, stroke="white", opacity=0.92):
    return (f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
            f'stroke="{stroke}" stroke-width="1.6" stroke-linecap="round" '
            f'stroke-linejoin="round" opacity="{opacity}">{paths}</svg>')

def _kpi_ico(paths):
    return (f'<svg width="22" height="22" viewBox="0 0 24 24" fill="none" '
            f'stroke="var(--accent)" stroke-width="1.8" stroke-linecap="round" '
            f'stroke-linejoin="round">{paths}</svg>')

ICO_HERO = {
    "kpis":     _svg('<rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/><polyline points="6 10 9 7 12 10 16 6"/>'),
    "live":     _svg('<polyline points="2 12 6 12 8 4 10 20 12 10 14 16 16 12 22 12"/>'),
    "trend":    _svg('<polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/>'),
    "sla":      _svg('<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/>'),
    "predict":  _svg('<circle cx="12" cy="12" r="2.5"/><circle cx="4" cy="6" r="1.5" fill="white" stroke="none"/><circle cx="20" cy="6" r="1.5" fill="white" stroke="none"/><circle cx="4" cy="18" r="1.5" fill="white" stroke="none"/><circle cx="20" cy="18" r="1.5" fill="white" stroke="none"/><line x1="5.5" y1="7" x2="10.2" y2="11"/><line x1="18.5" y1="7" x2="13.8" y2="11"/><line x1="5.5" y1="17" x2="10.2" y2="13"/><line x1="18.5" y1="17" x2="13.8" y2="13"/>'),
    "ml":       _svg('<line x1="18" y1="20" x2="18" y2="8"/><line x1="12" y1="20" x2="12" y2="3"/><line x1="6" y1="20" x2="6" y2="13"/><line x1="2" y1="20" x2="22" y2="20"/>'),
    "forecast": _svg('<polyline points="2 17 8 11 13 15 22 6"/><polyline points="17 6 22 6 22 11"/>'),
    "teams":    _svg('<circle cx="9" cy="7" r="4"/><path d="M3 21v-2a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v2"/><circle cx="19" cy="11" r="2"/><path d="M23 21v-1a2 2 0 0 0-2-2h-1"/>'),
    "exec":     _svg('<rect x="2" y="2" width="20" height="20" rx="3"/><line x1="8" y1="9" x2="16" y2="9"/><line x1="8" y1="13" x2="14" y2="13"/><line x1="8" y1="17" x2="12" y2="17"/>'),
}

ICO_KPI = {
    "tickets": _kpi_ico('<path d="M14 2H6a2 2 0 0 0-2 2v16c0 1.1.9 2 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="9" y1="13" x2="15" y2="13"/><line x1="9" y1="17" x2="13" y2="17"/>'),
    "clock":   _kpi_ico('<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>'),
    "check":   _kpi_ico('<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>'),
    "timer":   _kpi_ico('<circle cx="12" cy="13" r="8"/><path d="M12 9v4l2.5 2.5"/><line x1="9" y1="2" x2="15" y2="2"/><line x1="12" y1="2" x2="12" y2="5"/>'),
    "gauge":   _kpi_ico('<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>'),
    "alert":   _kpi_ico('<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>'),
}

_LOGO_SVG = ('<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">'
             '<rect x="3" y="3" width="18" height="18" rx="4"/>'
             '<circle cx="8.5" cy="8.5" r="1.3" fill="white" stroke="none"/>'
             '<circle cx="15.5" cy="8.5" r="1.3" fill="white" stroke="none"/>'
             '<circle cx="8.5" cy="15.5" r="1.3" fill="white" stroke="none"/>'
             '<circle cx="15.5" cy="15.5" r="1.3" fill="white" stroke="none"/>'
             '<circle cx="12" cy="12" r="1.8" fill="white" stroke="none"/>'
             '<line x1="8.5" y1="9.8" x2="10.8" y2="11"/>'
             '<line x1="15.5" y1="9.8" x2="13.2" y2="11"/>'
             '<line x1="8.5" y1="14.2" x2="10.8" y2="13"/>'
             '<line x1="15.5" y1="14.2" x2="13.2" y2="13"/></svg>')


# ── API helpers ────────────────────────────────────────────────────────────────
def api_predict(payload):
    try:
        r = requests.post(f"{API_URL}/predict", json=payload, timeout=6)
        r.raise_for_status(); return r.json()
    except Exception as e: st.error(f"API error: {e}"); return None

def api_health():
    try: return requests.get(f"{API_URL}/health", timeout=4).json()
    except Exception: return None

def api_metadata():
    try: return requests.get(f"{API_URL}/metadata", timeout=4).json()
    except Exception: return None


# ── Chart theme ────────────────────────────────────────────────────────────────
def dark_fig(fig, height=380, margin=None):
    m = margin or dict(l=16, r=16, t=48, b=16)
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#64748b", family="Inter", size=12),
        height=height, margin=m,
        legend=dict(bgcolor="rgba(10,16,30,.7)", bordercolor="rgba(255,255,255,.06)",
                    borderwidth=1, font=dict(color="#94a3b8", size=11)),
        xaxis=dict(gridcolor="rgba(255,255,255,.04)", zerolinecolor="rgba(255,255,255,.06)", tickfont=dict(size=11)),
        yaxis=dict(gridcolor="rgba(255,255,255,.04)", zerolinecolor="rgba(255,255,255,.06)", tickfont=dict(size=11)),
        title_font=dict(size=13, color="#94a3b8", family="Space Grotesk"),
        hoverlabel=dict(bgcolor="rgba(10,16,30,.92)", bordercolor="rgba(255,255,255,.1)", font=dict(color="#f8fafc", size=12)),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"""
    <div style="padding:24px 16px 20px;border-bottom:1px solid rgba(255,255,255,.06);margin-bottom:16px;">
        <div style="display:flex;align-items:center;gap:12px;">
            <div style="width:38px;height:38px;background:linear-gradient(135deg,#7c3aed,#06b6d4);
                border-radius:10px;display:flex;align-items:center;justify-content:center;
                box-shadow:0 0 20px rgba(124,58,237,.4);">{_LOGO_SVG}</div>
            <div>
                <div style="font-family:'Space Grotesk',sans-serif;font-size:1rem;font-weight:700;color:#f8fafc;letter-spacing:-.02em;">AIOps ITSM</div>
                <div style="font-size:.7rem;color:#475569;font-weight:500;margin-top:1px;">Intelligence Platform v4</div>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

    health = api_health()
    if health:
        mv = health.get("model_version", "")[:10] if health.get("model_version") else ""
        st.markdown(f"""
        <div class="status-pill-ok" style="width:100%;justify-content:center;margin-bottom:4px;">
            <div style="width:7px;height:7px;background:#26de81;border-radius:50%;box-shadow:0 0 8px #26de81;"></div>
            API en ligne
        </div>
        <div style="text-align:center;font-size:.7rem;color:#334155;margin-bottom:12px;">Modèle: {mv}</div>""",
        unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-pill-err" style="width:100%;justify-content:center;margin-bottom:12px;">⚠ API hors ligne</div>', unsafe_allow_html=True)

    page = st.radio(
        "nav",
        ["📊 KPIs Stratégiques", "🔴 Live Stream", "📈 Tendances",
         "🎯 Analyse SLA", "🤖 Moteur Prédictif", "🧠 Performance ML",
         "🔮 Prévisions IA", "👥 Équipes & Catégories", "📋 Vue Exécutive"],
        label_visibility="collapsed",
    )

    st.divider()
    st.markdown('<div style="font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#334155;margin-bottom:8px;">Filtres</div>', unsafe_allow_html=True)

    source_selection = st.multiselect(
        "Source", ["Historique (Batch)", "Temps Réel (API)"],
        default=["Historique (Batch)", "Temps Réel (API)"],
        label_visibility="visible",
    )
    source_map = {"Historique (Batch)": "'csv_batch'", "Temps Réel (API)": "'glpi_api'"}
    selected_sources = [source_map[s] for s in source_selection]
    source_filter = f"source IN ({','.join(selected_sources)})" if selected_sources else "1=1"

    st.markdown('<div style="font-size:.72rem;color:#475569;margin:10px 0 4px;font-weight:600;">Période</div>', unsafe_allow_html=True)
    preset = st.selectbox("Preset", ["Tout", "30 derniers jours", "90 derniers jours", "12 derniers mois"],
                          label_visibility="collapsed")
    today = date.today()
    if preset == "30 derniers jours":
        date_from, date_to = today - timedelta(days=30), today
    elif preset == "90 derniers jours":
        date_from, date_to = today - timedelta(days=90), today
    elif preset == "12 derniers mois":
        date_from, date_to = today - timedelta(days=365), today
    else:
        date_from, date_to = None, None

    date_cond = (f"date_creation BETWEEN '{date_from}' AND '{date_to + timedelta(days=1)}'"
                 if date_from else "1=1")

    st.divider()
    last_df = query_live("SELECT MAX(updated_at) AS t, COUNT(*) AS total FROM fact_tickets WHERE source='glpi_api'")
    if not last_df.empty and last_df["t"].iloc[0] is not None:
        n  = int(last_df["total"].iloc[0])
        lt = pd.to_datetime(last_df["t"].iloc[0]).strftime("%H:%M:%S")
        st.markdown(f"""
        <div style="background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.06);border-radius:12px;padding:12px 14px;">
            <div style="font-size:.68rem;text-transform:uppercase;letter-spacing:.1em;color:#334155;font-weight:700;margin-bottom:6px;">Streaming Live</div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:1.5rem;font-weight:700;color:#a78bfa;">{n:,}</div>
            <div style="font-size:.72rem;color:#475569;margin-top:3px;">tickets · {lt}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↻  Actualiser", use_container_width=True):
        st.cache_data.clear(); st.rerun()

    st.markdown('<div style="padding:16px 0 8px;border-top:1px solid rgba(255,255,255,.04);text-align:center;font-size:.68rem;color:#1e293b;margin-top:12px;">ENSA Fès · PFE 2026</div>', unsafe_allow_html=True)


# ── WHERE builder ──────────────────────────────────────────────────────────────
def where(extra: str = "") -> str:
    parts = [c for c in [extra, source_filter, date_cond] if c and c != "1=1"]
    return ("WHERE " + " AND ".join(parts)) if parts else ""

def where_no_date(extra: str = "") -> str:
    parts = [c for c in [extra, source_filter] if c and c != "1=1"]
    return ("WHERE " + " AND ".join(parts)) if parts else ""

def hero(title, subtitle, icon=""):
    st.markdown(f"""
    <div class="page-hero">
        <div style="margin-bottom:10px;display:flex;justify-content:center;">{icon}</div>
        <div class="page-hero-title">{title}</div>
        <div class="page-hero-sub">{subtitle}</div>
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — KPIs STRATÉGIQUES
# ══════════════════════════════════════════════════════════════════════════════
if page == "📊 KPIs Stratégiques":
    hero("Tableau de Bord Exécutif",
         "Vue d'ensemble en temps réel de la performance ITSM", ICO_HERO["kpis"])

    total_df  = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where()}")
    open_df   = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where('status_id IN (SELECT status_id FROM dim_status WHERE code IN (1,2,3,4))')}")
    solved_df = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where('status_id IN (SELECT status_id FROM dim_status WHERE code=5)')}")
    mttr_df   = query(f"SELECT ROUND(AVG(mttr_hours)::numeric,1) AS v FROM fact_tickets {where('mttr_hours IS NOT NULL')}")
    sla_df    = query(f"SELECT ROUND(100.0*SUM(CASE WHEN sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS v FROM fact_tickets {where('sla_respected IS NOT NULL')}")
    crit_df   = query(f"SELECT COUNT(*) AS n FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id {where('dp.code=1 AND ft.status_id IN (SELECT status_id FROM dim_status WHERE code IN (1,2,3,4))')}")
    _cond_w7  = "date_creation >= NOW()-INTERVAL '7 days'"
    _cond_w14 = "date_creation BETWEEN NOW()-INTERVAL '14 days' AND NOW()-INTERVAL '7 days'"
    w7_df     = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where_no_date(_cond_w7)}")
    w14_df    = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where_no_date(_cond_w14)}")

    total  = int(total_df["n"].iloc[0])   if not total_df.empty  else 0
    open_  = int(open_df["n"].iloc[0])    if not open_df.empty   else 0
    solved = int(solved_df["n"].iloc[0])  if not solved_df.empty else 0
    mttr   = float(mttr_df["v"].iloc[0])  if not mttr_df.empty and mttr_df["v"].iloc[0] is not None else 0.0
    sla    = float(sla_df["v"].iloc[0])   if not sla_df.empty  and sla_df["v"].iloc[0] is not None  else 0.0
    crit   = int(crit_df["n"].iloc[0])    if not crit_df.empty else 0
    w7n    = int(w7_df["n"].iloc[0])      if not w7_df.empty   else 0
    w14n   = int(w14_df["n"].iloc[0])     if not w14_df.empty  else 0

    wow    = round((w7n - w14n) / max(w14n, 1) * 100, 1)
    wow_h  = (f'<div class="kpi-delta-up">▲ +{wow}% vs sem. préc.</div>' if wow > 0
              else f'<div class="kpi-delta-down">▼ {wow}% vs sem. préc.</div>' if wow < 0
              else '<div class="kpi-delta-neutral">→ Stable vs sem. préc.</div>')
    res_pct = round(solved / max(total, 1) * 100, 1)

    sla_color  = "#26de81" if sla >= 80 else ("#ffa726" if sla >= 60 else "#ff4757")
    crit_color = "#ff4757" if crit > 0 else "#26de81"

    cards = [
        ("Total Tickets",     f"{total:,}",  "#a78bfa", "#7c3aed", ICO_KPI["tickets"], wow_h),
        ("En cours",          f"{open_:,}",  "#fb923c", "#ea580c", ICO_KPI["clock"],   ""),
        ("Résolus",           f"{solved:,}", "#26de81", "#16a34a", ICO_KPI["check"],   f'<div class="kpi-delta-up">{res_pct}% du total</div>'),
        ("MTTR Moyen",        f"{mttr}h",    "#38bdf8", "#0284c7", ICO_KPI["timer"],   ""),
        ("Taux SLA",          f"{sla}%",     sla_color, sla_color, ICO_KPI["gauge"],   ""),
        ("Critiques ouverts", f"{crit}",     crit_color,crit_color,ICO_KPI["alert"],   ""),
    ]
    cols = st.columns(6)
    for i, (col, (label, val, color, glow, icon, delta)) in enumerate(zip(cols, cards)):
        try:
            r, g, b = int(glow[1:3], 16), int(glow[3:5], 16), int(glow[5:7], 16)
            gr = f"rgba({r},{g},{b},.25)"
        except Exception:
            gr = "rgba(124,58,237,.25)"
        col.markdown(f"""
        <div class="kpi-card" style="--accent:{color};--glow:{gr};animation-delay:{i*0.06}s">
            <span class="kpi-icon">{icon}</span>
            <div class="kpi-value" style="color:{color}">{val}</div>
            <div class="kpi-label">{label}</div>
            {delta}
        </div>""", unsafe_allow_html=True)

    if crit > 0:
        st.markdown(f'<div class="alert-critical">🚨 <strong>{crit} ticket(s) Très Haute priorité</strong> toujours ouverts — intervention immédiate requise.</div>', unsafe_allow_html=True)
    elif sla >= 80:
        st.markdown(f'<div class="alert-ok">✅ SLA respecté à <strong>{sla}%</strong> — performance conforme aux objectifs.</div>', unsafe_allow_html=True)
    elif sla > 0:
        st.markdown(f'<div class="alert-warn">⚠️ SLA à <strong>{sla}%</strong> — en dessous de l\'objectif 80%.</div>', unsafe_allow_html=True)

    st.divider()
    col_l, col_r = st.columns(2)

    prio_df = query(f"""
        SELECT dp.label AS priority, dp.code, COUNT(*) AS count
        FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        {where()} GROUP BY dp.label, dp.code ORDER BY dp.code
    """)
    if not prio_df.empty:
        fig = px.pie(prio_df, values="count", names="priority", hole=.6,
                     color="priority", color_discrete_map=PRIORITY_COLORS,
                     title="Répartition par Priorité")
        fig.update_traces(textposition="outside", textinfo="label+percent",
                          marker=dict(line=dict(color="rgba(0,0,0,0)", width=0)))
        col_l.plotly_chart(dark_fig(fig), use_container_width=True)

    stat_df = query(f"""
        SELECT ds.label AS status, dp.label AS priority, COUNT(*) AS count
        FROM fact_tickets ft
        JOIN dim_status ds ON ft.status_id=ds.status_id
        JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        {where()} GROUP BY ds.label, ds.code, dp.label, dp.code ORDER BY ds.code, dp.code
    """)
    if not stat_df.empty:
        fig2 = px.bar(stat_df, x="status", y="count", color="priority",
                      color_discrete_map=PRIORITY_COLORS, barmode="stack",
                      title="Tickets par Statut & Priorité")
        fig2.update_layout(bargap=0.25)
        col_r.plotly_chart(dark_fig(fig2), use_container_width=True)

    st.divider()
    st.markdown('<div class="sec-label">Carte des Catégories (Treemap)</div>', unsafe_allow_html=True)
    cat_tree = query(f"""
        SELECT dc.name AS categorie, COUNT(*) AS tickets,
               ROUND(AVG(ft.mttr_hours)::numeric,1) AS mttr_moy
        FROM fact_tickets ft JOIN dim_category dc ON ft.category_id=dc.category_id
        {where()} GROUP BY dc.name ORDER BY tickets DESC LIMIT 15
    """)
    if not cat_tree.empty:
        fig_t = px.treemap(cat_tree, path=["categorie"], values="tickets",
                           color="mttr_moy",
                           color_continuous_scale=["#26de81","#ffa726","#ff4757"],
                           title="Volume par catégorie · couleur = MTTR moyen (h)")
        fig_t.update_traces(textinfo="label+value", textfont_size=13)
        fig_t.update_layout(coloraxis_colorbar=dict(title="MTTR (h)", tickfont=dict(color="#64748b")))
        st.plotly_chart(dark_fig(fig_t, height=380), use_container_width=True)

    st.divider()
    st.markdown('<div class="sec-label">Flux ITSM — Sankey</div>', unsafe_allow_html=True)
    sankey_df = query(f"""
        SELECT ft.source, dp.label AS priority, ds.label AS status, COUNT(*) AS count
        FROM fact_tickets ft
        JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        JOIN dim_status   ds ON ft.status_id=ds.status_id
        {where()} GROUP BY ft.source, dp.label, ds.label
    """)
    if not sankey_df.empty:
        nodes = (list(sankey_df["source"].unique()) +
                 list(sankey_df["priority"].unique()) +
                 list(sankey_df["status"].unique()))
        idx = {n: i for i, n in enumerate(nodes)}
        src, tgt, val = [], [], []
        for _, r in sankey_df.groupby(["source","priority"])["count"].sum().reset_index().iterrows():
            src.append(idx[r["source"]]); tgt.append(idx[r["priority"]]); val.append(r["count"])
        for _, r in sankey_df.groupby(["priority","status"])["count"].sum().reset_index().iterrows():
            src.append(idx[r["priority"]]); tgt.append(idx[r["status"]]); val.append(r["count"])
        node_colors = (["#7c3aed"] * len(sankey_df["source"].unique()) +
                       [PRIORITY_COLORS.get(p, "#7c3aed") for p in sankey_df["priority"].unique()] +
                       ["#334155"] * len(sankey_df["status"].unique()))
        fig_s = go.Figure(go.Sankey(
            node=dict(pad=18, thickness=24, label=nodes, color=node_colors,
                      line=dict(color="rgba(0,0,0,0)", width=0)),
            link=dict(source=src, target=tgt, value=val, color="rgba(124,58,237,.18)"),
        ))
        st.plotly_chart(dark_fig(fig_s, height=400), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — LIVE STREAM
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔴 Live Stream":
    count = st_autorefresh(interval=15_000, key="live_autorefresh")

    col_title, col_badge = st.columns([5, 1])
    with col_title:
        hero("Flux Temps Réel", f"Ingestion Kafka → PostgreSQL · Auto-refresh #{count}", ICO_HERO["live"])
    with col_badge:
        st.markdown("""<br><div class="live-badge" style="margin-top:8px;">
            <div class="live-ring"><div class="live-ring-dot"></div><div class="live-ring-pulse"></div></div>
            LIVE</div>""", unsafe_allow_html=True)

    tp1h      = query_live("SELECT COUNT(*) AS n FROM fact_tickets WHERE updated_at > NOW()-INTERVAL '1 hour' AND source='glpi_api'")
    tp5m      = query_live("SELECT COUNT(*) AS n FROM fact_tickets WHERE updated_at > NOW()-INTERVAL '5 minutes' AND source='glpi_api'")
    tp_total  = query_live("SELECT COUNT(*) AS n FROM fact_tickets WHERE source='glpi_api'")
    crit_open = query_live("""SELECT COUNT(*) AS n FROM fact_tickets ft
        JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        JOIN dim_status   ds ON ft.status_id=ds.status_id
        WHERE dp.code=1 AND ds.code IN (1,2,3,4) AND ft.source='glpi_api'""")

    ntotal = int(tp_total["n"].iloc[0])  if not tp_total.empty  else 0
    n1h    = int(tp1h["n"].iloc[0])      if not tp1h.empty      else 0
    n5m    = int(tp5m["n"].iloc[0])      if not tp5m.empty      else 0
    ncrit  = int(crit_open["n"].iloc[0]) if not crit_open.empty else 0

    c1,c2,c3,c4 = st.columns(4)
    for col, val, label, color, icon in [
        (c1, f"{ntotal:,}", "Total streaming",   "#a78bfa", "📡"),
        (c2, f"{n1h:,}",   "Dernière heure",    "#38bdf8", "🕐"),
        (c3, f"{n5m:,}",   "5 dernières min",   "#26de81", "⚡"),
        (c4, f"{ncrit:,}", "Critiques ouverts", "#ff4757", "🔥"),
    ]:
        col.markdown(f"""
        <div class="kpi-card" style="--accent:{color};--glow:rgba(0,0,0,.2)">
            <span class="kpi-icon">{icon}</span>
            <div class="kpi-value" style="color:{color}">{val}</div>
            <div class="kpi-label">{label}</div>
        </div>""", unsafe_allow_html=True)

    if ncrit > 0:
        st.markdown(f'<div class="alert-critical">🔥 <strong>{ncrit} ticket(s) Très Haute priorité</strong> en attente de traitement urgent !</div>', unsafe_allow_html=True)

    st.divider()
    col_roll, col_prio = st.columns(2)

    roll_df = query_live("""
        SELECT DATE_TRUNC('minute', updated_at) AS minute, COUNT(*) AS tickets
        FROM fact_tickets WHERE updated_at > NOW()-INTERVAL '30 minutes' AND source='glpi_api'
        GROUP BY 1 ORDER BY 1
    """)
    with col_roll:
        if not roll_df.empty:
            fig_r = px.area(roll_df, x="minute", y="tickets",
                            title="Ingestion — 30 dernières minutes",
                            color_discrete_sequence=["#7c3aed"], line_shape="spline")
            fig_r.update_traces(fill="tozeroy", fillcolor="rgba(124,58,237,.12)", line=dict(width=2.5))
            st.plotly_chart(dark_fig(fig_r, height=260), use_container_width=True)
        else:
            st.info("Aucune donnée streaming dans les 30 dernières minutes.")

    live_prio = query_live("""
        SELECT dp.label AS priority, COUNT(*) AS count
        FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        WHERE ft.updated_at > NOW()-INTERVAL '1 hour' AND ft.source='glpi_api'
        GROUP BY dp.label, dp.code ORDER BY dp.code
    """)
    with col_prio:
        if not live_prio.empty:
            fig_lp = px.bar(live_prio, x="priority", y="count", color="priority",
                            color_discrete_map=PRIORITY_COLORS,
                            title="Priorités — dernière heure")
            fig_lp.update_layout(showlegend=False, bargap=0.3)
            st.plotly_chart(dark_fig(fig_lp, height=260), use_container_width=True)

    st.divider()
    st.markdown('<div class="sec-label">Derniers tickets ingérés</div>', unsafe_allow_html=True)
    live_df = query_live("""
        SELECT ft.glpi_ticket_id AS id, LEFT(ft.title,50) AS titre,
               COALESCE(dp.label,'—') AS priority, COALESCE(ds.label,'—') AS statut,
               CASE WHEN ft.mttr_hours IS NULL THEN '—'
                    ELSE ROUND(ft.mttr_hours::numeric,1)::text || 'h'
               END AS mttr_h,
               CASE WHEN ft.sla_respected THEN 'OK' WHEN ft.sla_respected=false THEN 'KO' ELSE '—' END AS sla,
               TO_CHAR(ft.updated_at,'HH24:MI:SS') AS ingestion
        FROM fact_tickets ft
        LEFT JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        LEFT JOIN dim_status   ds ON ft.status_id=ds.status_id
        WHERE ft.source='glpi_api' ORDER BY ft.updated_at DESC LIMIT 30
    """)
    if live_df.empty:
        st.warning("Aucun ticket streaming. Le producer Kafka alimente la table toutes les 30 s.")
    else:
        def _cp(v):
            c = {"Very High":"background-color:rgba(255,71,87,.15);color:#ff8fa3","High":"background-color:rgba(255,107,53,.12);color:#ffad8a","Medium":"background-color:rgba(255,167,38,.1);color:#ffd166","Low":"background-color:rgba(38,222,129,.1);color:#6ee7b7","Very Low":"background-color:rgba(69,170,242,.1);color:#93c5fd"}
            return c.get(v, "")
        def _cs(v): return "color:#26de81;font-weight:700" if v=="OK" else ("color:#ff4757;font-weight:700" if v=="KO" else "")
        st.dataframe(live_df.style.map(_cp, subset=["priority"]).map(_cs, subset=["sla"]),
                     use_container_width=True, height=480)

    st.caption(f"⟳ Actualisation toutes les 15 s — cycle #{count}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — TENDANCES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Tendances":
    hero("Tendances Temporelles",
         "Analyse de l'évolution des incidents dans le temps", ICO_HERO["trend"])

    granularity = st.selectbox("Granularité", ["Jour", "Semaine", "Heure"], index=0)
    trunc = {"Jour": "day", "Semaine": "week", "Heure": "hour"}[granularity]

    trend_df = query(f"""
        SELECT DATE_TRUNC('{trunc}', date_creation) AS periode, source,
               COUNT(*) AS tickets,
               ROUND(AVG(mttr_hours)::numeric,2) AS avg_mttr,
               ROUND(100.*SUM(CASE WHEN sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS sla_pct
        FROM fact_tickets {where("date_creation IS NOT NULL")}
        GROUP BY 1,2 ORDER BY 1
    """)

    if trend_df.empty:
        st.info("Aucune donnée temporelle disponible.")
    else:
        col_vol, col_mttr = st.columns(2)
        with col_vol:
            fig = px.area(trend_df, x="periode", y="tickets", color="source",
                          title=f"Volume par {granularity.lower()}",
                          line_shape="spline", color_discrete_sequence=["#7c3aed","#26de81"])
            fig.update_traces(fill="tozeroy")
            st.plotly_chart(dark_fig(fig, height=320), use_container_width=True)

        with col_mttr:
            mg = trend_df.groupby("periode")["avg_mttr"].mean().reset_index()
            mg["rolling_7"] = mg["avg_mttr"].rolling(7, min_periods=1).mean()
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=mg["periode"], y=mg["avg_mttr"],
                                      mode="lines+markers", name="MTTR",
                                      line=dict(color="#ffa726", width=2.5), marker=dict(size=5)))
            fig2.add_trace(go.Scatter(x=mg["periode"], y=mg["rolling_7"],
                                      mode="lines", name="Moy. mobile",
                                      line=dict(color="#a78bfa", width=2, dash="dot")))
            fig2.update_layout(title="MTTR + Moyenne mobile")
            st.plotly_chart(dark_fig(fig2, height=320), use_container_width=True)

        st.divider()
        col_sla, col_cum = st.columns(2)
        with col_sla:
            sg = trend_df.groupby("periode")["sla_pct"].mean().reset_index()
            fig3 = px.line(sg, x="periode", y="sla_pct", title="Évolution du taux SLA (%)",
                           color_discrete_sequence=["#26de81"], markers=True)
            fig3.add_hline(y=80, line_dash="dash", line_color="#ffa726",
                           annotation_text="Objectif 80%", annotation_position="bottom right",
                           annotation_font_color="#ffa726")
            fig3.update_layout(yaxis=dict(range=[0,105]))
            st.plotly_chart(dark_fig(fig3, height=320), use_container_width=True)

        with col_cum:
            cd = trend_df.groupby("periode")["tickets"].sum().reset_index()
            cd["cumul"] = cd["tickets"].cumsum()
            fig4 = px.area(cd, x="periode", y="cumul", title="Volume cumulé",
                           color_discrete_sequence=["#06b6d4"], line_shape="spline")
            fig4.update_traces(fill="tozeroy", fillcolor="rgba(6,182,212,.1)")
            st.plotly_chart(dark_fig(fig4, height=320), use_container_width=True)

    st.divider()
    col_heat, col_pevo = st.columns(2)
    heat_df = query(f"""
        SELECT EXTRACT(DOW FROM date_creation)::int AS dow,
               EXTRACT(HOUR FROM date_creation)::int AS hour,
               COUNT(*) AS count
        FROM fact_tickets {where("date_creation IS NOT NULL")} GROUP BY 1,2
    """)
    with col_heat:
        st.markdown('<div class="sec-label">Heatmap Tickets (Jour × Heure)</div>', unsafe_allow_html=True)
        if not heat_df.empty:
            days  = ["Dim","Lun","Mar","Mer","Jeu","Ven","Sam"]
            pivot = heat_df.pivot_table(index="dow", columns="hour", values="count", fill_value=0)
            pivot = pivot.reindex(index=range(7), columns=range(24), fill_value=0)
            fig_h = px.imshow(pivot, labels=dict(x="Heure", y="Jour", color="Tickets"),
                              x=list(range(24)), y=[days[i] for i in pivot.index],
                              color_continuous_scale=["#0d1021","#3b0764","#7c3aed","#a78bfa"],
                              title="Concentration temporelle des incidents")
            st.plotly_chart(dark_fig(fig_h, height=320), use_container_width=True)

    prio_ev = query(f"""
        SELECT DATE_TRUNC('week', date_creation) AS semaine,
               dp.label AS priority, COUNT(*) AS count
        FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        {where("date_creation IS NOT NULL")} GROUP BY 1,2,dp.code ORDER BY 1,dp.code
    """)
    with col_pevo:
        st.markdown('<div class="sec-label">Évolution des priorités</div>', unsafe_allow_html=True)
        if not prio_ev.empty:
            fig_pe = px.bar(prio_ev, x="semaine", y="count", color="priority",
                            barmode="stack", title="Mix de priorités par semaine",
                            color_discrete_map=PRIORITY_COLORS)
            st.plotly_chart(dark_fig(fig_pe, height=320), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — SLA
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🎯 Analyse SLA":
    hero("Analyse des SLA",
         "Conformité aux engagements de service et distribution MTTR", ICO_HERO["sla"])

    sla_prio = query(f"""
        SELECT dp.label AS priority, dp.code, COUNT(*) AS total,
               ROUND(AVG(ft.mttr_hours)::numeric,2) AS avg_mttr,
               SUM(CASE WHEN ft.sla_respected THEN 1 ELSE 0 END) AS respected,
               ROUND(100.*SUM(CASE WHEN ft.sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pct
        FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id
        {where("ft.sla_respected IS NOT NULL")}
        GROUP BY dp.label, dp.code ORDER BY dp.code
    """)

    if sla_prio.empty:
        st.info("Données SLA non disponibles.")
    else:
        global_sla = float(sla_prio["respected"].sum() / sla_prio["total"].sum() * 100) if sla_prio["total"].sum() > 0 else 0
        gc = "#26de81" if global_sla >= 80 else ("#ffa726" if global_sla >= 60 else "#ff4757")

        col_g, col_b = st.columns([1,2])
        with col_g:
            fig_g = go.Figure(go.Indicator(
                mode="gauge+number", value=global_sla,
                number=dict(suffix="%", font=dict(color=gc, size=48, family="Space Grotesk")),
                gauge=dict(axis=dict(range=[0,100], tickfont=dict(color="#475569")),
                           bar=dict(color=gc, thickness=.55), bgcolor="rgba(0,0,0,0)", borderwidth=0,
                           steps=[dict(range=[0,60],color="rgba(255,71,87,.08)"),
                                  dict(range=[60,80],color="rgba(255,167,38,.08)"),
                                  dict(range=[80,100],color="rgba(38,222,129,.08)")],
                           threshold=dict(line=dict(color="#a78bfa",width=3), thickness=.75, value=80)),
                title=dict(text="Taux SLA Global", font=dict(color="#64748b",size=13,family="Space Grotesk")),
            ))
            st.plotly_chart(dark_fig(fig_g, height=280, margin=dict(l=24,r=24,t=48,b=10)), use_container_width=True)

        with col_b:
            fig_b = px.bar(sla_prio, x="priority", y="pct",
                           color="pct", color_continuous_scale=["#ff4757","#ffa726","#26de81"],
                           range_color=[0,100], title="% SLA Respecté par Priorité", text="pct")
            fig_b.update_traces(texttemplate="%{text}%", textposition="outside", marker_line_width=0)
            fig_b.update_layout(coloraxis_showscale=False, yaxis=dict(range=[0,115]), bargap=0.3)
            st.plotly_chart(dark_fig(fig_b, height=280), use_container_width=True)

        st.divider()
        col_l, col_r = st.columns(2)
        with col_l:
            fig_bub = px.scatter(sla_prio, x="avg_mttr", y="pct", size="total",
                                 color="priority", hover_name="priority", size_max=65,
                                 color_discrete_map=PRIORITY_COLORS, title="MTTR vs SLA% vs Volume")
            fig_bub.add_hline(y=80, line_dash="dash", line_color="#a78bfa",
                              annotation_text="80% objectif", annotation_font_color="#a78bfa")
            fig_bub.update_layout(yaxis=dict(range=[0,110]))
            st.plotly_chart(dark_fig(fig_bub, height=320), use_container_width=True)

        with col_r:
            grp_df = query(f"""
                SELECT dg.name AS groupe,
                       ROUND(100.*SUM(CASE WHEN ft.sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS sla_pct,
                       COUNT(*) AS total
                FROM fact_tickets ft JOIN dim_group dg ON ft.group_id=dg.group_id
                {where("ft.sla_respected IS NOT NULL")}
                GROUP BY dg.name ORDER BY sla_pct DESC LIMIT 10
            """)
            if not grp_df.empty:
                fig_grp = px.bar(grp_df, x="sla_pct", y="groupe", orientation="h",
                                 color="sla_pct", color_continuous_scale=["#ff4757","#ffa726","#26de81"],
                                 range_color=[0,100], title="SLA% par Groupe", text="sla_pct")
                fig_grp.update_traces(texttemplate="%{text}%", marker_line_width=0)
                fig_grp.update_layout(coloraxis_showscale=False)
                st.plotly_chart(dark_fig(fig_grp, height=320), use_container_width=True)

        st.divider()
        col_box, col_viol = st.columns(2)
        mttr_box = query(f"""
            SELECT dp.label AS priority, ft.mttr_hours
            FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id
            {where("ft.mttr_hours IS NOT NULL AND ft.mttr_hours < 500")}
        """)
        with col_box:
            if not mttr_box.empty:
                fig_box = px.box(mttr_box, x="priority", y="mttr_hours",
                                 color="priority", color_discrete_map=PRIORITY_COLORS,
                                 title="MTTR (h) — Quartiles par Priorité", points="outliers")
                fig_box.update_layout(showlegend=False)
                st.plotly_chart(dark_fig(fig_box, height=340), use_container_width=True)

        late_df = query(f"""
            SELECT ft.glpi_ticket_id AS ID, dp.label AS Priorité,
                   CASE WHEN fts.delay_hours IS NULL THEN '—'
                        ELSE ROUND(fts.delay_hours::numeric,1)::text || 'h'
                   END AS "Retard (h)",
                   LEFT(ft.title,42) AS Titre, ft.date_creation::date AS Date
            FROM fact_tickets ft
            JOIN fact_ticket_sla fts ON ft.ticket_id=fts.ticket_id
            JOIN dim_priority dp ON ft.priority_id=dp.priority_id
            {where("fts.sla_respected=false")}
            ORDER BY fts.delay_hours DESC LIMIT 20
        """)
        with col_viol:
            st.markdown('<div class="sec-label">Dépassements SLA</div>', unsafe_allow_html=True)
            if late_df.empty:
                st.markdown('<div class="alert-ok" style="margin-top:40px;">✅ Aucun dépassement SLA détecté.</div>', unsafe_allow_html=True)
            else:
                st.dataframe(late_df, use_container_width=True, height=340)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — PRÉDICTION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🤖 Moteur Prédictif":
    hero("Moteur Prédictif IA",
         "Classification de priorité et estimation MTTR par Machine Learning", ICO_HERO["predict"])

    if "pred_history" not in st.session_state:
        st.session_state.pred_history = []

    col_form, col_hist = st.columns([3,2])
    with col_form:
        with st.form("predict_form"):
            st.markdown('<div style="font-size:.8rem;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.1em;margin-bottom:16px;">Paramètres du ticket</div>', unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            urgency     = c1.slider("Urgence", 1, 5, 3)
            impact      = c2.slider("Impact",  1, 5, 3)
            hour_of_day = c1.number_input("Heure de création", 0, 23, 9)
            day_of_week = c2.selectbox("Jour", range(7), format_func=lambda x: ["Lun","Mar","Mer","Jeu","Ven","Sam","Dim"][x])
            month       = c1.selectbox("Mois", range(1,13))
            category    = c2.selectbox("Catégorie", ["network","hardware","software","security","access","database","email"])
            submitted   = st.form_submit_button("⚡ Prédire la priorité", use_container_width=True)

    with col_hist:
        st.markdown('<div class="sec-label" style="margin-top:0;">Historique de session</div>', unsafe_allow_html=True)
        if st.session_state.pred_history:
            st.dataframe(pd.DataFrame(st.session_state.pred_history), use_container_width=True, height=380)
        else:
            st.markdown('<div style="background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.06);border-radius:16px;padding:48px 24px;text-align:center;color:#334155;font-size:.9rem;">Lancez une prédiction<br>pour voir l\'historique ici.</div>', unsafe_allow_html=True)

    result = None
    if submitted:
        with st.spinner("Interrogation du modèle ML…"):
            result = api_predict(dict(urgency=urgency, impact=impact,
                                      hour_of_day=hour_of_day, day_of_week=day_of_week,
                                      month=month, category_type=category))
        if result:
            label      = result["predicted_label"]
            color      = PRIORITY_COLORS.get(label, "#a78bfa")
            probs      = result.get("probabilities", {})
            confidence = result.get("confidence", max(probs.values()) if probs else 0)
            mttr_est   = result.get("predicted_mttr_hours")
            conf_color = "#26de81" if confidence>=0.7 else ("#ffa726" if confidence>=0.5 else "#ff4757")

            st.divider()
            col_res, col_prob = st.columns(2)
            with col_res:
                mttr_html = (f'<div style="margin-top:16px;padding:12px 20px;background:rgba(255,167,38,.08);border:1px solid rgba(255,167,38,.2);border-radius:12px;display:inline-block;"><span style="font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.1em;font-weight:700;">MTTR Estimé</span><br><span style="font-family:Space Grotesk;font-size:1.8rem;font-weight:700;color:#ffa726;">{mttr_est}h</span></div>'
                             if mttr_est else "")
                st.markdown(f"""
                <div class="predict-result" style="border-top:3px solid {color};">
                    <div class="predict-label">Priorité Prédite</div>
                    <div class="predict-priority" style="color:{color};text-shadow:0 0 40px {color}55;">{label}</div>
                    <div><span class="conf-chip" style="background:{conf_color}18;border:1px solid {conf_color}44;color:{conf_color};">{confidence:.0%} confidence</span></div>
                    <div class="predict-meta">{category} · Urgence={urgency} · Impact={impact}</div>
                    {mttr_html}
                </div>""", unsafe_allow_html=True)
            with col_prob:
                if probs:
                    prob_df = pd.DataFrame(list(probs.items()), columns=["Priorité","Probabilité"]).sort_values("Probabilité")
                    fig_p = px.bar(prob_df, y="Priorité", x="Probabilité", orientation="h",
                                   color="Priorité", color_discrete_map=PRIORITY_COLORS,
                                   range_x=[0,1], text="Probabilité", title="Distribution des probabilités")
                    fig_p.update_traces(texttemplate="%{text:.0%}", textposition="outside", marker_line_width=0)
                    fig_p.update_layout(showlegend=False, xaxis=dict(range=[0,1.15]))
                    st.plotly_chart(dark_fig(fig_p, height=320), use_container_width=True)

            st.session_state.pred_history.insert(0, {
                "Priorité": label, "Conf.": f"{confidence:.0%}",
                "Urgence": urgency, "Impact": impact, "Catégorie": category,
                "MTTR est.": f"{mttr_est}h" if mttr_est else "—",
                "Heure": datetime.now().strftime("%H:%M:%S"),
            })
            st.session_state.pred_history = st.session_state.pred_history[:10]

    if result:
        st.divider()
        st.markdown('<div class="sec-label">Tickets historiques similaires</div>', unsafe_allow_html=True)
        sim_df = query(f"""
            SELECT ft.glpi_ticket_id AS ID, dp.label AS Priorité,
                   ft.urgency AS Urgence, ft.impact AS Impact,
                   ROUND(ft.mttr_hours::numeric,1)::text || 'h' AS "MTTR (h)",
                   CASE WHEN ft.sla_respected THEN '✅' ELSE '❌' END AS SLA
            FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id
            {where(f'ft.urgency BETWEEN {urgency-1} AND {urgency+1} AND ft.impact BETWEEN {impact-1} AND {impact+1} AND ft.mttr_hours IS NOT NULL')}
            ORDER BY ABS(ft.urgency-{urgency})+ABS(ft.impact-{impact}) LIMIT 8
        """)
        if not sim_df.empty:
            st.dataframe(sim_df, use_container_width=True, height=260)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — PERFORMANCE ML
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🧠 Performance ML":
    hero("Performance du Modèle ML",
         "Métriques, features et historique d'entraînement", ICO_HERO["ml"])

    meta = api_metadata()
    if not meta:
        st.error("Impossible d'accéder aux métadonnées. Vérifiez que l'API est opérationnelle.")
        st.stop()

    metrics  = meta.get("metrics", {})
    f1       = float(metrics.get("f1", 0))
    acc      = float(metrics.get("accuracy", 0))
    cv_f1    = float(metrics.get("cv_f1_mean", 0))
    cv_std   = float(metrics.get("cv_f1_std", 0))
    bal_acc  = float(metrics.get("balanced_accuracy", 0))
    mae_mttr = float(metrics.get("mae_mttr", 0))
    algo     = metrics.get("algorithm", "—")

    if f1 < 0.75:
        st.markdown(f'<div class="alert-warn">⚠️ F1-score ({f1:.1%}) sous l\'objectif 75%. Retraining recommandé.</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="alert-ok">✅ Modèle performant — F1 = {f1:.1%} · CV-F1 = {cv_f1:.1%} ± {cv_std:.1%}</div>', unsafe_allow_html=True)

    cards_ml = [
        ("Algorithme",     algo,            "#a78bfa","#7c3aed","🤖"),
        ("F1-Score",       f"{f1:.1%}",     "#26de81","#16a34a","📊"),
        ("CV-F1 (5-fold)", f"{cv_f1:.1%}",  "#38bdf8","#0284c7","🔁"),
        ("Accuracy",       f"{acc:.1%}",    "#ffa726","#ea580c","🎯"),
        ("Balanced Acc.",  f"{bal_acc:.1%}","#f472b6","#be185d","⚖️"),
        ("MAE MTTR",       f"{mae_mttr:.1f}h","#fb923c","#c2410c","⏱"),
    ]
    cols = st.columns(6)
    for i, (col, (label, val, color, glow, icon)) in enumerate(zip(cols, cards_ml)):
        col.markdown(f"""
        <div class="kpi-card" style="--accent:{color};animation-delay:{i*0.06}s">
            <span class="kpi-icon">{icon}</span>
            <div class="kpi-value" style="color:{color}">{val}</div>
            <div class="kpi-label">{label}</div>
        </div>""", unsafe_allow_html=True)

    st.divider()
    col_l, col_r = st.columns(2)
    with col_l:
        f1c = "#26de81" if f1>=0.75 else ("#ffa726" if f1>=0.60 else "#ff4757")
        fig_f1 = go.Figure(go.Indicator(
            mode="gauge+number+delta", value=f1*100,
            delta=dict(reference=75, suffix="% vs 75%",
                       increasing=dict(color="#26de81"), decreasing=dict(color="#ff4757")),
            number=dict(suffix="%", font=dict(color=f1c, size=48, family="Space Grotesk")),
            gauge=dict(axis=dict(range=[0,100], tickfont=dict(color="#475569")),
                       bar=dict(color=f1c, thickness=.6), bgcolor="rgba(0,0,0,0)", borderwidth=0,
                       steps=[dict(range=[0,60],color="rgba(255,71,87,.08)"),
                              dict(range=[60,75],color="rgba(255,167,38,.08)"),
                              dict(range=[75,100],color="rgba(38,222,129,.08)")],
                       threshold=dict(line=dict(color="#a78bfa",width=3), thickness=.8, value=75)),
            title=dict(text="F1-Score Pondéré", font=dict(color="#64748b",size=13,family="Space Grotesk")),
        ))
        st.plotly_chart(dark_fig(fig_f1, height=300, margin=dict(l=24,r=24,t=50,b=10)), use_container_width=True)

    with col_r:
        ac = "#26de81" if acc>=0.75 else "#ffa726"
        fig_acc = go.Figure(go.Indicator(
            mode="gauge+number", value=acc*100,
            number=dict(suffix="%", font=dict(color=ac, size=48, family="Space Grotesk")),
            gauge=dict(axis=dict(range=[0,100], tickfont=dict(color="#475569")),
                       bar=dict(color=ac, thickness=.6), bgcolor="rgba(0,0,0,0)", borderwidth=0,
                       steps=[dict(range=[0,60],color="rgba(255,71,87,.08)"),
                              dict(range=[60,100],color="rgba(38,222,129,.08)")]),
            title=dict(text="Accuracy", font=dict(color="#64748b",size=13,family="Space Grotesk")),
        ))
        st.plotly_chart(dark_fig(fig_acc, height=300, margin=dict(l=24,r=24,t=50,b=10)), use_container_width=True)

    st.divider()
    features = meta.get("features", [])
    if features:
        ft_types = {"urgency":"Numérique base","impact":"Numérique base","urgency_x_impact":"Engineered","urgency_sq":"Engineered","impact_sq":"Engineered","severity_score":"Engineered","hour_of_day":"Temporel","day_of_week":"Temporel","month":"Temporel","is_business_hours":"Engineered","is_weekend":"Engineered","quarter":"Engineered"}
        feat_df = pd.DataFrame([{"Feature":f,"Type":"Catégoriel (one-hot)" if f.startswith("category_type_") else ft_types.get(f,"Autre")} for f in features])
        col_f1, col_f2 = st.columns(2)
        col_f1.dataframe(feat_df, use_container_width=True, height=300)
        tc = feat_df["Type"].value_counts().reset_index(); tc.columns = ["Type","Nombre"]
        fig_feat = px.pie(tc, values="Nombre", names="Type", hole=.55,
                          color_discrete_sequence=["#7c3aed","#26de81","#ffa726","#38bdf8"],
                          title="Répartition des types de features")
        col_f2.plotly_chart(dark_fig(fig_feat, height=300), use_container_width=True)

    st.divider()
    hist_model = query("""
        SELECT algorithm AS Algorithme, ROUND(f1_score::numeric,4) AS F1,
               ROUND(accuracy::numeric,4) AS Accuracy,
               TO_CHAR(trained_at,'YYYY-MM-DD HH24:MI') AS "Entraîné le"
        FROM ml_model_registry ORDER BY trained_at DESC LIMIT 10
    """)
    if hist_model.empty:
        st.caption("Aucun historique disponible.")
    else:
        st.dataframe(hist_model, use_container_width=True, height=240)

    st.divider()
    col_btn, col_info = st.columns([1,3])
    if col_btn.button("🚀 Lancer le retraining", use_container_width=True):
        try:
            requests.post(f"{API_URL}/retrain", timeout=10).raise_for_status()
            col_info.markdown('<div class="alert-ok" style="margin-top:8px;">✅ Ré-entraînement lancé. Rechargez dans ~2 min.</div>', unsafe_allow_html=True)
        except Exception as e:
            col_info.markdown(f'<div class="alert-critical" style="margin-top:8px;">❌ Erreur: {e}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — PRÉVISIONS IA
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔮 Prévisions IA":
    hero("Prévisions IA", "Projection du volume de tickets et de la charge future", ICO_HERO["forecast"])

    horizon = st.slider("Horizon de prévision (jours)", 7, 60, 14, step=7)

    hist_df = query(f"""
        SELECT DATE_TRUNC('day', date_creation)::date AS jour, COUNT(*) AS tickets
        FROM fact_tickets {where("date_creation IS NOT NULL")}
        GROUP BY 1 ORDER BY 1
    """)

    if hist_df.empty or len(hist_df) < 7:
        st.info("Pas assez de données historiques (minimum 7 jours requis).")
    else:
        hist_df["jour"] = pd.to_datetime(hist_df["jour"])
        hist_df = hist_df.sort_values("jour").reset_index(drop=True)
        hist_df["x"] = np.arange(len(hist_df))

        x = hist_df["x"].values
        y = hist_df["tickets"].values.astype(float)
        coeffs = np.polyfit(x, y, 1)
        poly   = np.poly1d(coeffs)

        x_future     = np.arange(len(hist_df), len(hist_df) + horizon)
        last_date    = hist_df["jour"].iloc[-1]
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=horizon)
        y_forecast   = np.clip(poly(x_future), 0, None)

        rolling_std  = pd.Series(y).rolling(7, min_periods=3).std().bfill().values
        avg_std      = float(np.mean(rolling_std[-14:]))
        upper = y_forecast + 1.5 * avg_std
        lower = np.clip(y_forecast - 1.5 * avg_std, 0, None)

        fig_fc = go.Figure()
        fig_fc.add_trace(go.Scatter(x=hist_df["jour"], y=hist_df["tickets"],
                                    mode="lines+markers", name="Historique",
                                    line=dict(color="#7c3aed", width=2), marker=dict(size=4)))
        fig_fc.add_trace(go.Scatter(x=future_dates, y=y_forecast,
                                    mode="lines+markers", name="Prévision",
                                    line=dict(color="#06b6d4", width=2.5, dash="dot"),
                                    marker=dict(size=5, color="#06b6d4")))
        fig_fc.add_trace(go.Scatter(
            x=list(future_dates) + list(future_dates[::-1]),
            y=list(upper) + list(lower[::-1]),
            fill="toself", fillcolor="rgba(6,182,212,.08)",
            line=dict(color="rgba(0,0,0,0)"), name="Intervalle ±1.5σ",
        ))
        fig_fc.add_vline(x=int(last_date.timestamp() * 1000), line_dash="dash",
                         line_color="rgba(255,255,255,.2)",
                         annotation_text="Aujourd'hui", annotation_font_color="#475569")
        fig_fc.update_layout(title=f"Prévision du volume de tickets — {horizon} prochains jours")
        st.plotly_chart(dark_fig(fig_fc, height=420), use_container_width=True)

        st.divider()
        avg_fc     = float(np.mean(y_forecast))
        total_fc   = int(np.sum(y_forecast))
        trend_dir  = "hausse" if coeffs[0] > 0.5 else ("baisse" if coeffs[0] < -0.5 else "stable")
        trend_col  = "#ff4757" if trend_dir=="hausse" else ("#26de81" if trend_dir=="baisse" else "#ffa726")

        c1,c2,c3 = st.columns(3)
        c1.markdown(f'<div class="kpi-card" style="--accent:#06b6d4"><span class="kpi-icon">📅</span><div class="kpi-value" style="color:#06b6d4">{total_fc:,}</div><div class="kpi-label">Tickets prévus ({horizon}j)</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="kpi-card" style="--accent:#a78bfa"><span class="kpi-icon">📊</span><div class="kpi-value" style="color:#a78bfa">{avg_fc:.0f}/j</div><div class="kpi-label">Moyenne journalière prévue</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="kpi-card" style="--accent:{trend_col}"><span class="kpi-icon">📈</span><div class="kpi-value" style="color:{trend_col};font-size:1.6rem">{trend_dir.upper()}</div><div class="kpi-label">Tendance ({coeffs[0]:+.1f} tickets/j)</div></div>', unsafe_allow_html=True)

        st.divider()
        st.markdown('<div class="sec-label">Répartition attendue par Priorité</div>', unsafe_allow_html=True)
        prio_share = query(f"""
            SELECT dp.label AS priority, dp.code,
                   ROUND(100.0*COUNT(*)/NULLIF(SUM(COUNT(*)) OVER(),0),1) AS share
            FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id
            {where_no_date("date_creation >= NOW()-INTERVAL '30 days'")}
            GROUP BY dp.label, dp.code ORDER BY dp.code
        """)
        if not prio_share.empty:
            prio_share["tickets_prevus"] = (prio_share["share"] / 100 * total_fc).round(0).astype(int)
            col_p1, col_p2 = st.columns(2)
            with col_p1:
                fig_ps = px.bar(prio_share, x="priority", y="tickets_prevus",
                                color="priority", color_discrete_map=PRIORITY_COLORS,
                                title=f"Tickets prévus par priorité ({horizon}j)", text="tickets_prevus")
                fig_ps.update_traces(textposition="outside", marker_line_width=0)
                fig_ps.update_layout(showlegend=False)
                st.plotly_chart(dark_fig(fig_ps, height=300), use_container_width=True)
            with col_p2:
                tbl = prio_share[["priority","share","tickets_prevus"]].copy()
                tbl.columns = ["Priorité","Part (%)","Tickets prévus"]
                st.dataframe(tbl, use_container_width=True, height=220)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 8 — ÉQUIPES & CATÉGORIES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "👥 Équipes & Catégories":
    hero("Équipes & Catégories",
         "Performance des groupes de support et analyse des catégories", ICO_HERO["teams"])

    tab1, tab2 = st.tabs(["👥 Groupes de Support", "📂 Catégories"])

    with tab1:
        grp_perf = query(f"""
            SELECT dg.name AS groupe, COUNT(*) AS tickets,
                   ROUND(AVG(ft.mttr_hours)::numeric,1) AS mttr_moy,
                   ROUND(100.*SUM(CASE WHEN ft.sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS sla_pct
            FROM fact_tickets ft JOIN dim_group dg ON ft.group_id=dg.group_id
            {where()} GROUP BY dg.name ORDER BY tickets DESC LIMIT 12
        """)

        if grp_perf.empty:
            st.info("Aucune donnée de groupe disponible.")
        else:
            st.markdown('<div class="sec-label">Leaderboard des équipes</div>', unsafe_allow_html=True)
            max_t = int(grp_perf["tickets"].max())
            for _, row in grp_perf.iterrows():
                sc = "#26de81" if row["sla_pct"]>=80 else ("#ffa726" if row["sla_pct"]>=60 else "#ff4757")
                fw = int(row["tickets"] / max_t * 100)
                st.markdown(f"""
                <div style="background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.06);
                    border-radius:12px;padding:12px 16px;margin-bottom:8px;
                    display:flex;align-items:center;gap:16px;">
                    <div style="width:130px;font-size:.82rem;color:#94a3b8;font-weight:500;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{row['groupe']}</div>
                    <div style="flex:1;height:8px;background:rgba(255,255,255,.05);border-radius:999px;overflow:hidden;">
                        <div style="width:{fw}%;height:100%;background:linear-gradient(90deg,#7c3aed,#06b6d4);border-radius:999px;"></div>
                    </div>
                    <div style="font-family:'Space Grotesk',sans-serif;font-size:.95rem;font-weight:700;color:#f8fafc;width:50px;text-align:right;">{int(row['tickets']):,}</div>
                    <div style="font-size:.8rem;color:{sc};font-weight:700;width:60px;text-align:center;">SLA {row['sla_pct']}%</div>
                    <div style="font-size:.78rem;color:#64748b;width:70px;text-align:right;">MTTR {row['mttr_moy']}h</div>
                </div>""", unsafe_allow_html=True)

            st.divider()
            col_g1, col_g2 = st.columns(2)
            with col_g1:
                fig_gv = px.bar(grp_perf, x="groupe", y="tickets", color="sla_pct",
                                color_continuous_scale=["#ff4757","#ffa726","#26de81"],
                                range_color=[0,100], title="Volume & SLA par groupe")
                fig_gv.update_layout(xaxis_tickangle=-35, coloraxis_showscale=False)
                st.plotly_chart(dark_fig(fig_gv, height=320), use_container_width=True)

            with col_g2:
                fig_gm = px.scatter(grp_perf, x="mttr_moy", y="sla_pct", size="tickets",
                                    color="groupe", hover_name="groupe", size_max=55,
                                    title="MTTR vs SLA% par groupe")
                fig_gm.add_hline(y=80, line_dash="dash", line_color="#a78bfa",
                                 annotation_text="Objectif SLA 80%", annotation_font_color="#a78bfa")
                st.plotly_chart(dark_fig(fig_gm, height=320), use_container_width=True)

    with tab2:
        cat_df = query(f"""
            SELECT dc.name AS categorie, COUNT(*) AS tickets,
                   ROUND(AVG(ft.mttr_hours)::numeric,1) AS mttr_moy,
                   ROUND(100.*SUM(CASE WHEN ft.sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS sla_pct,
                   ROUND(100.*SUM(CASE WHEN dp.code IN (1,2) THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pct_critique
            FROM fact_tickets ft
            JOIN dim_category dc ON ft.category_id=dc.category_id
            JOIN dim_priority dp ON ft.priority_id=dp.priority_id
            {where()} GROUP BY dc.name ORDER BY tickets DESC LIMIT 15
        """)

        if cat_df.empty:
            st.info("Aucune donnée de catégorie.")
        else:
            col_c1, col_c2 = st.columns(2)
            with col_c1:
                fig_ct = px.treemap(cat_df, path=["categorie"], values="tickets",
                                    color="sla_pct",
                                    color_continuous_scale=["#ff4757","#ffa726","#26de81"],
                                    range_color=[0,100],
                                    title="Treemap : Volume (couleur = SLA%)")
                fig_ct.update_traces(textinfo="label+value")
                st.plotly_chart(dark_fig(fig_ct, height=380), use_container_width=True)

            with col_c2:
                fig_cm = px.bar(cat_df.head(10), x="mttr_moy", y="categorie",
                                orientation="h", color="mttr_moy",
                                color_continuous_scale=["#26de81","#ffa726","#ff4757"],
                                title="MTTR moyen par catégorie (top 10)", text="mttr_moy")
                fig_cm.update_traces(texttemplate="%{text}h", marker_line_width=0)
                fig_cm.update_layout(coloraxis_showscale=False)
                st.plotly_chart(dark_fig(fig_cm, height=380), use_container_width=True)

            st.divider()
            col_c3, col_c4 = st.columns(2)
            with col_c3:
                fig_cc = px.bar(cat_df.head(10), x="categorie", y="pct_critique",
                                color="pct_critique",
                                color_continuous_scale=["#26de81","#ffa726","#ff4757"],
                                title="% tickets critiques (P1+P2) par catégorie", text="pct_critique")
                fig_cc.update_traces(texttemplate="%{text}%", textposition="outside", marker_line_width=0)
                fig_cc.update_layout(xaxis_tickangle=-30, coloraxis_showscale=False, yaxis=dict(range=[0,100]))
                st.plotly_chart(dark_fig(fig_cc, height=320), use_container_width=True)

            with col_c4:
                st.markdown('<div class="sec-label" style="margin-top:0">Tableau récapitulatif</div>', unsafe_allow_html=True)
                st.dataframe(
                    cat_df[["categorie","tickets","mttr_moy","sla_pct","pct_critique"]].rename(
                        columns={"categorie":"Catégorie","tickets":"Tickets","mttr_moy":"MTTR (h)","sla_pct":"SLA %","pct_critique":"Critiques %"}),
                    use_container_width=True, height=320)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 9 — VUE EXÉCUTIVE
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📋 Vue Exécutive":
    hero("Vue Exécutive",
         "Synthèse globale de la plateforme ITSM pour le management", ICO_HERO["exec"])

    st.markdown(f"""
    <div style="background:rgba(124,58,237,.06);border:1px solid rgba(124,58,237,.2);
        border-radius:16px;padding:16px 20px;margin-bottom:24px;
        display:flex;justify-content:space-between;align-items:center;">
        <div style="font-family:'Space Grotesk',sans-serif;font-size:1.1rem;font-weight:600;color:#f8fafc;">
            Rapport de Performance ITSM</div>
        <div style="font-size:.8rem;color:#64748b;">Généré le {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>
    </div>""", unsafe_allow_html=True)

    ex_total  = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where()}")
    ex_open   = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where('status_id IN (SELECT status_id FROM dim_status WHERE code IN (1,2,3,4))')}")
    ex_solved = query(f"SELECT COUNT(*) AS n FROM fact_tickets {where('status_id IN (SELECT status_id FROM dim_status WHERE code=5)')}")
    ex_mttr   = query(f"SELECT ROUND(AVG(mttr_hours)::numeric,1) AS v FROM fact_tickets {where('mttr_hours IS NOT NULL')}")
    ex_sla    = query(f"SELECT ROUND(100.*SUM(CASE WHEN sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS v FROM fact_tickets {where('sla_respected IS NOT NULL')}")
    ex_crit   = query(f"SELECT COUNT(*) AS n FROM fact_tickets ft JOIN dim_priority dp ON ft.priority_id=dp.priority_id {where('dp.code=1 AND ft.status_id IN (SELECT status_id FROM dim_status WHERE code IN (1,2,3,4))')}")

    etotal = int(ex_total["n"].iloc[0])   if not ex_total.empty  else 0
    eopen  = int(ex_open["n"].iloc[0])    if not ex_open.empty   else 0
    esolv  = int(ex_solved["n"].iloc[0])  if not ex_solved.empty else 0
    emttr  = float(ex_mttr["v"].iloc[0])  if not ex_mttr.empty and ex_mttr["v"].iloc[0] is not None else 0.0
    esla   = float(ex_sla["v"].iloc[0])   if not ex_sla.empty  and ex_sla["v"].iloc[0] is not None  else 0.0
    ecrit  = int(ex_crit["n"].iloc[0])    if not ex_crit.empty else 0

    sla_st  = "🟢 CONFORME"     if esla  >= 80 else ("🟡 À SURVEILLER" if esla  >= 60 else "🔴 CRITIQUE")
    mttr_st = "🟢 BON"          if emttr <= 24 else ("🟡 MOYEN"        if emttr <= 48 else "🔴 ÉLEVÉ")
    crit_st = "🔴 ACTION REQUISE" if ecrit > 0 else "🟢 OK"
    sla_cls  = "exec-status-green" if esla >=80 else ("exec-status-yellow" if esla >=60 else "exec-status-red")
    mttr_cls = "exec-status-green" if emttr<=24 else ("exec-status-yellow" if emttr<=48 else "exec-status-red")
    crit_cls = "exec-status-red"   if ecrit > 0 else "exec-status-green"

    c1,c2,c3 = st.columns(3)
    c1.markdown(f'<div class="exec-card"><div style="font-size:2rem;">🎫</div><div><div class="exec-card-val">{etotal:,}</div><div class="exec-card-label">Total tickets</div><div class="exec-status-green">{esolv:,} résolus ({round(esolv/max(etotal,1)*100,1)}%)</div></div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="exec-card"><div style="font-size:2rem;">🎯</div><div><div class="exec-card-val">{esla}%</div><div class="exec-card-label">Taux SLA global</div><div class="{sla_cls}">{sla_st}</div></div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="exec-card"><div style="font-size:2rem;">⏱</div><div><div class="exec-card-val">{emttr}h</div><div class="exec-card-label">MTTR moyen</div><div class="{mttr_cls}">{mttr_st}</div></div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    c4,c5,c6 = st.columns(3)
    c4.markdown(f'<div class="exec-card"><div style="font-size:2rem;">🔓</div><div><div class="exec-card-val">{eopen:,}</div><div class="exec-card-label">Tickets en cours</div></div></div>', unsafe_allow_html=True)
    c5.markdown(f'<div class="exec-card"><div style="font-size:2rem;">🚨</div><div><div class="exec-card-val">{ecrit}</div><div class="exec-card-label">Critiques ouverts</div><div class="{crit_cls}">{crit_st}</div></div></div>', unsafe_allow_html=True)

    meta_ex = api_metadata()
    if meta_ex:
        f1_ex  = float(meta_ex.get("metrics",{}).get("f1",0))
        ml_cls = "exec-status-green" if f1_ex>=0.75 else "exec-status-yellow"
        ml_st  = "🟢 OPTIMAL" if f1_ex>=0.75 else "🟡 À AMÉLIORER"
        c6.markdown(f'<div class="exec-card"><div style="font-size:2rem;">🤖</div><div><div class="exec-card-val">{f1_ex:.1%}</div><div class="exec-card-label">F1-Score modèle ML</div><div class="{ml_cls}">{ml_st}</div></div></div>', unsafe_allow_html=True)

    st.divider()
    col_rad, col_top = st.columns(2)

    with col_rad:
        st.markdown('<div class="sec-label">Radar de Performance</div>', unsafe_allow_html=True)
        f1_ex   = float(meta_ex.get("metrics",{}).get("f1",0)) if meta_ex else 0
        vals    = [min(esla,100), round(esolv/max(etotal,1)*100,1),
                   max(0,100-ecrit*10), max(0,100-(emttr/72*100)), f1_ex*100]
        cats    = ["SLA (%)","Résolution (%)","Anti-Critique","MTTR (inv.)","ML F1 (%)"]
        vals_c  = vals + [vals[0]]
        cats_c  = cats + [cats[0]]

        fig_rad = go.Figure()
        fig_rad.add_trace(go.Scatterpolar(r=vals_c, theta=cats_c, fill="toself",
                                          fillcolor="rgba(124,58,237,.15)",
                                          line=dict(color="#7c3aed", width=2),
                                          name="Performance actuelle"))
        fig_rad.add_trace(go.Scatterpolar(r=[80]*6, theta=cats_c, mode="lines",
                                          name="Objectif (80)",
                                          line=dict(color="#ffa726", width=1.5, dash="dot")))
        fig_rad.update_layout(
            polar=dict(bgcolor="rgba(0,0,0,0)",
                       radialaxis=dict(visible=True, range=[0,100],
                                       gridcolor="rgba(255,255,255,.06)",
                                       tickfont=dict(color="#475569", size=10)),
                       angularaxis=dict(gridcolor="rgba(255,255,255,.06)",
                                        tickfont=dict(color="#94a3b8", size=11))),
            showlegend=True,
            title=dict(text="Vue multidimensionnelle",
                       font=dict(color="#94a3b8", size=13, family="Space Grotesk")),
        )
        st.plotly_chart(dark_fig(fig_rad, height=360), use_container_width=True)

    with col_top:
        st.markdown('<div class="sec-label">Top 5 Catégories — Impact SLA</div>', unsafe_allow_html=True)
        top_cat = query(f"""
            SELECT dc.name AS categorie, COUNT(*) AS tickets,
                   ROUND(100.*SUM(CASE WHEN NOT ft.sla_respected THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS breach_pct
            FROM fact_tickets ft JOIN dim_category dc ON ft.category_id=dc.category_id
            {where("ft.sla_respected IS NOT NULL")}
            GROUP BY dc.name ORDER BY tickets DESC LIMIT 5
        """)
        if not top_cat.empty:
            fig_top = px.bar(top_cat, x="tickets", y="categorie", orientation="h",
                             color="breach_pct",
                             color_continuous_scale=["#26de81","#ffa726","#ff4757"],
                             range_color=[0,50],
                             title="Volume (couleur = % dépassements SLA)", text="tickets")
            fig_top.update_traces(textposition="outside", marker_line_width=0)
            fig_top.update_layout(coloraxis_colorbar=dict(title="Breach %", tickfont=dict(color="#64748b")))
            st.plotly_chart(dark_fig(fig_top, height=360), use_container_width=True)

    st.divider()
    st.markdown('<div class="sec-label">Export du rapport</div>', unsafe_allow_html=True)
    report_df = pd.DataFrame({
        "Métrique": ["Total tickets","En cours","Résolus","MTTR moyen (h)","Taux SLA (%)","Critiques ouverts"],
        "Valeur":   [etotal, eopen, esolv, emttr, esla, ecrit],
        "Statut":   ["—","—", f"{round(esolv/max(etotal,1)*100,1)}%", mttr_st, sla_st, crit_st],
    })
    st.download_button(
        label="⬇️  Télécharger le rapport CSV",
        data=report_df.to_csv(index=False).encode("utf-8"),
        file_name=f"itsm_rapport_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )

