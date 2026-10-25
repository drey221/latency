import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from netmiko import ConnectHandler
import threading
import time
import re
import sqlite3
import concurrent.futures
from datetime import datetime, timedelta
import logging
import os

# Sembunyikan HTTP request log dari Flask / Werkzeug
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# ==========================================
# 1. KONFIGURASI ROUTER & KREDENSIAL
# ==========================================
USERNAME = "andrey.firmando"
PASSWORD = "Fstar123!"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "latency_monitor.db")

ROUTERS = [
    {"id": "MDN-BTM", "normal": "12", "name": "SU-MDN-08-PE1-NIXMD <> KR-BTM-02-PE1-TJBBN", "desc": "ETH-TRUNK2_1X100G_via-MORA-CID: 1288741800174851", "mgmt_ip": "10.109.3.244", "peer_ip": "10.133.29.74"},
    {"id": "PBR-DMAI", "normal": "4", "name": "RI-PBR-04-PE1-SNPLN <> RI-DUM-01-PE1-DMAI", "desc": "20G_MORATEL_CID:1288740400068289", "mgmt_ip": "10.109.14.6", "peer_ip": "10.132.34.142"},
    {"id": "PLG-JKT", "normal": "8", "name": "SS-PLG-08-PE1-NIXPG <> JK-CKG-04-PE2-NDCJN", "desc": "ETH3_1X100G_via_MORATEL-CID:1288741800177138", "mgmt_ip": "10.109.3.24", "peer_ip": "10.132.34.73"},
    {"id": "PBR-MDN", "normal": "10", "name": "RI-PBR-04-PE1-SNPLN <> SU-MDN-08-PE1-NIXMD", "desc": "Eth7_1x20G_via_MORATEL-CID1288741800176256", "mgmt_ip": "10.109.14.6", "peer_ip": "10.132.34.54"},
    {"id": "BDL-JKT1", "normal": "4", "name": "LA-BDL-05-PE1-USULA <> JK-KYB-134-P1-CYB1", "desc": "100G_via-MORA-CID:1288741800102824", "mgmt_ip": "10.109.14.5", "peer_ip": "10.132.34.86"},
    {"id": "BDL-JKT2", "normal": "3", "name": "LA-BDL-05-PE1-USULA <> JK-GGP-07-PE1-DCATR", "desc": "2x100G_Via_DWDM_JAL", "mgmt_ip": "10.109.14.5", "peer_ip": "10.132.37.181"},
    {"id": "SBY-MKS", "normal": "11", "name": "JI-SBY-19-PE2-DCNCX <> SN-MKS-05-PE1-NCXMS", "desc": "100G_NNI-BLIP_101225", "mgmt_ip": "10.109.4.220", "peer_ip": "10.132.30.14"},
    {"id": "SBY-BJM", "normal": "8", "name": "JI-SBY-19-PE2-DCNCX <> KS-BJM-04-PE2-NCXBM", "desc": "Eth-Trunk2_3x10G_130326 VIA TELKOM", "mgmt_ip": "10.109.4.220", "peer_ip": "10.132.33.33"},
    {"id": "KDUS-IDC-F", "normal": "8", "name": "JT-KDS-01-P1-KDUS <> JK-KYB-140-SC2-IDCD3", "desc": "Eth-Trunk0_1X400G", "mgmt_ip": "10.109.6.13", "peer_ip": "10.133.35.65"},
    {"id": "JMBR-KTTG", "normal": "3", "name": "JI-JMR-01-P2-JMBR <> JI-SBY-02-P3-KTTG", "desc": "2x100G_UAJB", "mgmt_ip": "10.109.6.19", "peer_ip": "10.132.22.117"},
    {"id": "IDC-CRB-F", "normal": "3", "name": "JK-KYB-140-SC2-IDCD3 <> JB-CBN-02-P2-CRBKT", "desc": "1X400G", "mgmt_ip": "10.109.6.2", "peer_ip": "10.133.35.50"},
    {"id": "IDC-OMDT-F", "normal": "11", "name": "JK-KYB-140-SC2-IDCD3 <> JI-SBY-01-P2-OMDT", "desc": "1X400G", "mgmt_ip": "10.109.6.2", "peer_ip": "10.133.35.173"},
    {"id": "IDC-KTTG-F", "normal": "11", "name": "JK-KYB-140-SC2-IDCD3 <> JI-SBY-02-P3-KTTG", "desc": "2x400G_UAJB-F", "mgmt_ip": "10.109.6.2", "peer_ip": "10.133.35.74"},
    {"id": "KTTG-CYB2", "normal": "12", "name": "JI-SBY-02-P3-KTTG <> JK-JKT-02-P1-CYB2", "desc": "10x10G_UAJB", "mgmt_ip": "10.109.6.24", "peer_ip": "10.132.23.49"},
    {"id": "KTTG-KNYN", "normal": "3", "name": "JI-SBY-02-P3-KTTG <> JI-TLG-01-PE2-KNYN", "desc": "2x100G_UAJB", "mgmt_ip": "10.109.6.24", "peer_ip": "10.133.29.33"},
    {"id": "KTTG-CYB2-SC1", "normal": "11", "name": "JI-SBY-02-P3-KTTG <> JK-KYB-132-SC1-CYB2", "desc": "4x100G_UAJB&UJB", "mgmt_ip": "10.109.6.24", "peer_ip": "10.132.17.189"},
    {"id": "KTTG-IDC-SC2-F", "normal": "11", "name": "JI-SBY-02-P3-KTTG <> JK-KYB-140-SC2-IDCD3", "desc": "2x400G_UAJB-F", "mgmt_ip": "10.109.6.24", "peer_ip": "10.133.35.73"},

    {"id": "CRB-KLGR", "normal": "4", "name": "JB-CBN-02-P2-CRBKT <> JT-SMG-02-P2-KLGRG", "desc": "2x100G_UAJB", "mgmt_ip": "10.109.6.9", "peer_ip": "10.132.17.2"},
    {"id": "CRB-BKR", "normal": "4", "name": "JB-CBN-02-P2-CRBKT <> JB-BDG-07-P1-BKR", "desc": "2x100G_UAJB", "mgmt_ip": "10.109.6.9", "peer_ip": "10.132.23.189"},
    {"id": "CRB-PWT", "normal": "3", "name": "JB-CBN-02-P2-CRBKT <> JJT-PWT-01-P2-KGKM", "desc": "2x100G_UAJB", "mgmt_ip": "10.109.6.9", "peer_ip": "10.131.18.109"},
    {"id": "CRB-CYB2", "normal": "4", "name": "JB-CBN-02-P2-CRBKT <> JK-KYB-132-SC1-CYB2", "desc": "2x100G_UJB+UAJB", "mgmt_ip": "10.109.6.9", "peer_ip": "10.132.26.105"},

    {"id": "MLGJW-CYB1-F", "normal": "9", "name": "JT-KRG-01-P2-MLGJW <> JK-KYB-134-SC2-CYB1", "desc": "1X400G_UAJB", "mgmt_ip": "10.109.6.14", "peer_ip": "10.133.35.69"},
    {"id": "MLGJW-KLGRG", "normal": "3", "name": "JT-KRG-01-P2-MLGJW <> JT-SMG-02-P2-KLGRG", "desc": "4x100G_BE16_UAJB", "mgmt_ip": "10.109.6.14", "peer_ip": "10.131.18.126"},
    {"id": "MLGJW-CYB1", "normal": "7", "name": "JT-KRG-01-P2-MLGJW <> JK-KYB-134-SC3-CYB1", "desc": "2x100G_UAJB", "mgmt_ip": "10.109.6.14", "peer_ip": "10.132.26.133"},
    {"id": "MLG-CYB1", "normal": "14", "name": "JI-MLG-01-P2-MLNG <> JK-KYB-134-SC3-CYB1", "desc": "4x100_DWDM", "mgmt_ip": "10.109.6.22", "peer_ip": "10.133.35.101"},

    {"id": "JMBG-CYB1-F", "normal": "12", "name": "JI-JBG-01-P2-JMBG <> JK-KYB-134-SC1-CYB1", "desc": "3X400G_UAJB-OVERLAY", "mgmt_ip": "10.109.6.20", "peer_ip": "10.133.35.93"},
    {"id": "PDKN-CYB1", "normal": "9", "name": "YO-YYK-01-P1-PDKN <> JK-KYB-134-SC3-CYB1", "desc": "4x100G_UAJB", "mgmt_ip": "10.109.4.131", "peer_ip": "10.133.33.197"},
    {"id": "PDKN-CYB1-F", "normal": "8", "name": "YO-YYK-01-P1-PDKN <> JK-KYB-134-SC1-CYB1", "desc": "1x400G_OVERLAY", "mgmt_ip": "10.109.4.131", "peer_ip": "10.133.35.169"},
    
    {"id": "MDUN-CYB1-F", "normal": "10", "name": "JI-MAD-01-P2-MDUN <> JK-KYB-134-SC2-CYB1", "desc": "2X400G_UAJB-F", "mgmt_ip": "10.109.6.18", "peer_ip": "10.133.35.85"},

    {"id": "CMHI-CYB1-F", "normal": "3", "name": "JB-CMH-01-P2-CMHI <> JK-KYB-134-SC1-CYB1", "desc": "2x400G_UAJB-OVERLAY", "mgmt_ip": "10.109.6.10", "peer_ip": "10.133.35.53"},

    {"id": "PWT-CYB1-F", "normal": "6", "name": "JT-PWT-01-P2-KGKM <> JK-KYB-134-SC2-CYB1", "desc": "1x400G", "mgmt_ip": "10.109.6.12", "peer_ip": "10.133.35.61"},
    {"id": "PWT-CYB1", "normal": "6", "name": "JT-PWT-01-P2-KGKM <> JK-KYB-134-SC3-CYB1", "desc": "4x100G_UAJB", "mgmt_ip": "10.109.6.12", "peer_ip": "10.132.26.126"},
    {"id": "PWT-MLGJW", "normal": "4", "name": "JT-PWT-01-P2-KGKM <> JT-KRG-01-P2-MLGJW", "desc": "2x100G_DWDM-HW_UAJB", "mgmt_ip": "10.109.6.12", "peer_ip": "10.132.26.174"},
    {"id": "PWT-GRUT", "normal": "3", "name": "JT-PWT-01-P2-KGKM <> JB-GRT-01-PE2-GRUT", "desc": "2x100G_UJB", "mgmt_ip": "10.109.6.12", "peer_ip": "10.133.31.165"},

    {"id": "KLGRG-CYB2", "normal": "8", "name": "JT-SMG-02-P2-KLGRG <> JK-JKT-02-SC2-CYB2", "desc": "7x100G_JAYABAYA", "mgmt_ip": "10.109.6.11", "peer_ip": "10.132.23.73"},
    {"id": "DRCKG-IDC-F", "normal": "7", "name": "JT-SMG-10-P1-DRCKG <> JK-KYB-140-SC1-IDCD3", "desc": "2X400G", "mgmt_ip": "10.109.6.28", "peer_ip": "10.133.35.57"},
    
    {"id": "MDN-BTM2", "normal": "10", "name": "SU-MDN-01-PE2-NCXMD <> KR-BTM-01-PE1-IDCBT", "desc": "via_TELKOM-SID:8882606240945239_BoD70G", "mgmt_ip": "10.109.4.242", "peer_ip": "10.132.34.161"},
    {"id": "MDN-PKB", "normal": "8", "name": "SU-MDN-01-PE2-NCXMD <> RI-PBR-09-PE1-NCPKB", "desc": "2x10G_Via-Telkom_SID:8882606240948284", "mgmt_ip": "10.109.4.242", "peer_ip": "10.132.34.70"}
    
     
]

# Mengelompokkan ROUTERS berdasarkan mgmt_ip secara otomatis untuk SSH Worker
ROUTER_GROUPS_DICT = {}
for r in ROUTERS:
    ip = r['mgmt_ip']
    if ip not in ROUTER_GROUPS_DICT:
        ROUTER_GROUPS_DICT[ip] = {
            'mgmt_ip': ip,
            'peers': []
        }
    ROUTER_GROUPS_DICT[ip]['peers'].append(r)

ROUTER_GROUPS_LIST = list(ROUTER_GROUPS_DICT.values())

LINE_COLORS = ['#00f0ff', '#ff007f', '#00ff87', '#ffaa00', '#ab47bc', '#42a5f5']

# ==========================================
# 2. DATABASE MANAGEMENT (SQLite)
# ==========================================
def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ping_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                router_id TEXT,
                timestamp DATETIME,
                latency REAL,
                uptime TEXT,
                peer_up_time TEXT,
                status TEXT
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_router_time ON ping_metrics(router_id, timestamp)')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Gagal inisialisasi Database: {e}")

def save_metric_to_db(router_id, latency, uptime, peer_up_time, status):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        cursor = conn.cursor()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
            INSERT INTO ping_metrics (router_id, timestamp, latency, uptime, peer_up_time, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (router_id, now_str, latency, uptime, peer_up_time, status))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving to DB: {e}")

def get_metrics_from_db(router_id, minutes_back):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    time_threshold = datetime.now() - timedelta(minutes=minutes_back)
    time_threshold_str = time_threshold.strftime('%Y-%m-%d %H:%M:%S')
    
    cursor.execute('''
        SELECT timestamp, latency, uptime, peer_up_time, status 
        FROM ping_metrics 
        WHERE router_id = ? AND timestamp >= ? 
        ORDER BY timestamp ASC
    ''', (router_id, time_threshold_str))
    
    rows = cursor.fetchall()
    conn.close()
    return rows

init_db()

# ==========================================
# 3. BACKGROUND WORKER (Netmiko SSH - Grouped)
# ==========================================
def fetch_router_data(router_group):
    device = {
        'device_type': 'huawei',
        'host': router_group['mgmt_ip'],
        'username': USERNAME,
        'password': PASSWORD,
        'global_delay_factor': 0.5
    }
    
    try:
        # Buka SSH 1x untuk router management IP ini
        with ConnectHandler(**device) as net_connect:
            # Ambil status IS-IS peer sekaligus untuk semua peer pada router ini
            isis_cmd = "display isis peer verbose"
            isis_out = net_connect.send_command(isis_cmd)
            isis_lines = isis_out.splitlines()

            # Process setiap peer yang terhubung ke router ini
            for peer in router_group['peers']:
                peer_ip = peer['peer_ip']
                peer_id = peer['id']
                
                # 1. Ping ke Peer IP
                ping_cmd = f"ping -s 9000 -c 2 {peer_ip}"
                ping_out = net_connect.send_command(ping_cmd)
                
                if "0 response(s) received" in ping_out or "Request time out" in ping_out or "Error" in ping_out or "100% packet loss" in ping_out:
                    ping_cmd = f"ping -s 4000 -c 2 {peer_ip}"
                    ping_out = net_connect.send_command(ping_cmd)

                avg_latency = 0
                match_latency = re.search(r'min/avg/max.*?=\s*\d+/(\d+)/\d+', ping_out)
                if match_latency:
                    avg_latency = float(match_latency.group(1))
                else:
                    times = re.findall(r'time=(\d+\.?\d*)', ping_out)
                    if times:
                        avg_latency = float(times[-1])
                
                # 2. Parse Uptime ISIS khusus Peer IP ini
                uptime = "Down/Not Found"
                peer_up_time = "N/A"
                
                found_ip = False
                for line in isis_lines:
                    if "Peer IP Address(es)" in line and peer_ip in line:
                        found_ip = True
                        continue
                    
                    if found_ip:
                        if "Uptime" in line and "Peer Up Time" not in line:
                            uptime = line.split(":", 1)[1].strip()
                        elif "Peer Up Time" in line:
                            peer_up_time = line.split(":", 1)[1].strip()
                        elif "Peer System Id" in line or "----------------" in line:
                            break

                # 3. Status Check & Save to DB
                normal_val = float(peer.get('normal', 0))
                if avg_latency > 0:
                    if normal_val > 0 and avg_latency >= (normal_val * 1.5):
                        status = 'HL'
                    else:
                        status = 'OK'
                else:
                    status = 'Ping Failed'

                save_metric_to_db(peer_id, avg_latency, uptime, peer_up_time, status)

    except Exception as e:
        # Jika SSH ke mgmt_ip gagal, tandai semua peer di router ini sebagai Error
        for peer in router_group['peers']:
            save_metric_to_db(peer['id'], 0, "Error", "N/A", "Error")

def data_collection_loop():
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            executor.map(fetch_router_data, ROUTER_GROUPS_LIST)
        time.sleep(5)

thread = threading.Thread(target=data_collection_loop, daemon=True)
thread.start()

# ==========================================
# 4. DASHBOARD UI & STYLING
# ==========================================
app = dash.Dash(__name__)
app.title = "NOC Backbone Latency Monitor"

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            html, body {
                margin: 0 !important;
                padding: 0 !important;
                background-color: #0d0d0d !important;
                overflow-x: hidden;
            }

            /* Box luar dropdown */
            .dash-dropdown div[class*="-control"] {
                background-color: #ffffff !important;
                border-color: #cccccc !important;
                min-height: 32px !important;
                height: 32px !important;
                border-radius: 6px !important;
            }

            /* TARGET TEPAT: Teks nilai yang terpilih di dalam box */
            .dash-dropdown-value,
            .dash-dropdown-value-item,
            .dash-dropdown-value-item span,
            #timeframe-selector-value {
                color: #000000 !important;
                font-weight: 600 !important;
            }

            /* Menu pilihan saat dropdown dibuka */
            .dash-dropdown div[class*="-menu"] {
                background-color: #1e1e1e !important;
                border: 1px solid #333333 !important;
                box-shadow: 0 6px 16px rgba(0,0,0,0.8) !important;
                border-radius: 6px !important;
                z-index: 9999 !important;
            }
            .dash-dropdown div[class*="-option"] {
                background-color: #1e1e1e !important;
                color: #e0e0e0 !important;
                font-size: 13px !important;
                padding: 6px 12px !important;
                cursor: pointer;
            }
            .dash-dropdown div[class*="-option"]:hover,
            .dash-dropdown div[class*="-option"][class*="focused"] {
                background-color: #2a2a2a !important;
                color: #00f0ff !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

DARK_BG = '#0d0d0d'
CARD_BG = '#1e1e1e'
BORDER_COLOR = '#2e2e2e'
TEXT_COLOR = '#ffffff'
TEXT_MUTED = '#a0a0a0'

card_style = {
    'backgroundColor': CARD_BG,
    'borderRadius': '10px',
    'padding': '16px',
    'margin': '10px',
    'boxShadow': '0 4px 12px rgba(0,0,0,0.5)',
    'border': f'1px solid {BORDER_COLOR}',
    'flex': '1 1 45%',
    'minWidth': '480px'
}

tab_style = {
    'backgroundColor': '#1e1e1e',
    'color': '#888888',
    'border': '1px solid #333333',
    'borderBottom': '1px solid #333333',
    'padding': '0px 25px',
    'lineHeight': '44px',
    'whiteSpace': 'nowrap',
    'fontWeight': '600',
    'cursor': 'pointer'
}

tab_selected_style = {
    'backgroundColor': '#2a2a2a',
    'color': '#00f0ff',
    'border': '1px solid #333333',
    'borderBottom': '2px solid #00f0ff',
    'padding': '0px 25px',
    'lineHeight': '44px',
    'whiteSpace': 'nowrap',
    'fontWeight': 'bold',
    'cursor': 'default'
}

app.layout = html.Div(style={'backgroundColor': DARK_BG, 'minHeight': '100vh', 'padding': '20px', 'color': TEXT_COLOR, 'fontFamily': 'Segoe UI, sans-serif'}, children=[
    html.H1("IS-IS & Latency Monitor", style={'textAlign': 'center', 'color': TEXT_COLOR, 'margin': '0 0 15px 0', 'fontWeight': '600'}),
    
    html.Div(style={'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center', 'marginBottom': '15px'}, children=[
        html.Span("Time Range: ", style={'marginRight': '12px', 'color': TEXT_MUTED, 'fontWeight': 'bold', 'fontSize': '14px'}),
        html.Div(style={'width': '150px'}, children=[
            dcc.Dropdown(
                id='timeframe-selector',
                options=[
                    {'label': 'Realtime (15m)', 'value': 15},
                    {'label': '1 Hour', 'value': 60},
                    {'label': '6 Hours', 'value': 360},
                    {'label': '12 Hours', 'value': 720},
                    {'label': '1 Day', 'value': 1440},
                    {'label': '3 Days', 'value': 4320},
                    {'label': '1 Week', 'value': 10080}
                ],
                value=15,
                clearable=False,
                searchable=False,
                className='dash-dropdown'
            )
        ])
    ]),
    
    # Kumpulan Tombol Tab Filter Group
    html.Div(style={'display': 'flex', 'justifyContent': 'center', 'marginBottom': '20px'}, children=[
        dcc.Tabs(
            id='group-selector',
            value='3RD-PARTY',
            children=[
                dcc.Tab(label='3RD-PARTY', value='3RD-PARTY', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='UNDERLAY', value='UNDERLAY', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='OVERLAY', value='OVERLAY', style=tab_style, selected_style=tab_selected_style),
            ],
            style={'height': '44px'}
        )
    ]),
    
    html.Div(id='backbone-container', style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'center'}),
    
    dcc.Interval(id='interval-component', interval=5000, n_intervals=0)
])

# ==========================================
# 5. UI CALLBACK
# ==========================================
@app.callback(
    Output('backbone-container', 'children'),
    [Input('interval-component', 'n_intervals'),
     Input('timeframe-selector', 'value'),
     Input('group-selector', 'value')]
)
def update_dashboard(n, timeframe_minutes, selected_group):
    cards = []
    
    now_dt = datetime.now()
    start_dt = now_dt - timedelta(minutes=timeframe_minutes)
    
    # 1. Konfigurasi Sampling, Tampilan Marker, dan Format Sumbu X Berdasarkan Timeframe
    if timeframe_minutes >= 10080:    # 1 Week
        step = 60
        show_markers = False
        date_format = '%d %b %H:%M'
    elif timeframe_minutes >= 4320:   # 3 Days
        step = 32
        show_markers = False
        date_format = '%d %b %H:%M'
    elif timeframe_minutes >= 1440:   # 1 Day
        step = 24
        show_markers = False
        date_format = '%H:%M'
    elif timeframe_minutes >= 720:    # 12 Hours
        step = 16
        show_markers = False
        date_format = '%H:%M'
    elif timeframe_minutes >= 360:    # 6 Hours
        step = 12
        show_markers = False
        date_format = '%H:%M:%S'
    elif timeframe_minutes >= 60:     # 1 Hour
        step = 4
        show_markers = True
        date_format = '%H:%M:%S'
    else:                             # Realtime (15 min)
        step = 2
        show_markers = True
        date_format = '%H:%M:%S'

    # 2. Filter Grouping
    group_mapping = {
        '3RD-PARTY': ['MDN-BTM', 'PBR-DMAI', 'PLG-JKT', 'PBR-MDN', 'BDL-JKT1', 'SBY-MKS', 'SBY-BJM', 'MDN-BTM2', 'MDN-PKB'],
        'UNDERLAY':  ['BDL-JKT2', 'JMBR-KTTG', 'KTTG-CYB2', 'KTTG-KNYN', 'KTTG-CYB2-SC1', 'CRB-KLGR', 'CRB-BKR', 'CRB-PWT', 'CRB-CYB2', 'MLGJW-KLGRG', 'MLGJW-CYB1', 'MLG-CYB1','PDKN-CYB1','PWT-CYB1','PWT-MLGJW','PWT-GRUT'],
        'OVERLAY':   ['KDUS-IDC-F', 'IDC-CRB-F', 'IDC-OMDT-F', 'IDC-KTTG-F', 'KTTG-IDC-SC2-F', 'MLGJW-CYB1-F','JMBG-CYB1-F','PDKN-CYB1-F','MDUN-CYB1-F','CMHI-CYB1-F','PWT-CYB1-F','KLGRG-CYB2','DRCKG-IDC-F']
    }
    
    allowed_ids = group_mapping.get(selected_group, [])
    filtered_routers = [r for r in ROUTERS if r['id'] in allowed_ids]
    
    for idx, r in enumerate(filtered_routers):
        r_id = r['id']
        line_color = LINE_COLORS[idx % len(LINE_COLORS)]
        
        raw_history = get_metrics_from_db(r_id, timeframe_minutes)
        
        if raw_history:
            latest_row = raw_history[-1]
            uptime = latest_row[2]
            peer_up_time = latest_row[3]
            status = latest_row[4]
            latest_lat = f"{latest_row[1]} ms" if latest_row[1] > 0 else "Down"
        else:
            uptime, peer_up_time, status, latest_lat = "N/A", "-", "Connecting...", "N/A"
        
        # 3. Downsampling
        sampled_history = raw_history[::step]
        
        raw_times = []
        raw_latencies = []
        for row in sampled_history:
            t_str = row[0].split('.')[0]
            try:
                dt_obj = datetime.strptime(t_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                dt_obj = t_str
            
            raw_times.append(dt_obj)
            raw_latencies.append(row[1] if row[1] > 0 else None)

        # 4. Gap Detection (Sisipkan None jika ada rentang waktu kosong agar garis putus)
        gap_threshold = max(60, step * 60)
        times = []
        latencies = []

        for i in range(len(raw_times)):
            if i > 0 and isinstance(raw_times[i], datetime) and isinstance(raw_times[i-1], datetime):
                delta = (raw_times[i] - raw_times[i-1]).total_seconds()
                if delta > gap_threshold:
                    times.append(raw_times[i-1] + timedelta(seconds=1))
                    latencies.append(None)
            times.append(raw_times[i])
            latencies.append(raw_latencies[i])
        
        # 5. Konfigurasi Plotly
        fig = go.Figure(data=[
            go.Scatter(
                x=times,
                y=latencies,
                mode='lines+markers',
                line=dict(
                    color=line_color, 
                    width=2.5 if show_markers else 1.7, 
                    shape='hvh'
                ),
                marker=dict(
                    size=4 if show_markers else 2.5, 
                    color=line_color
                ),
                connectgaps=False,
                name=r_id
            )
        ])
        
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=CARD_BG,
            plot_bgcolor='#151515',
            height=250,
            margin=dict(l=35, r=20, t=15, b=60),
            xaxis=dict(
                type='date',
                range=[start_dt, now_dt],
                tickformat=date_format,
                nticks=16,
                showgrid=True, 
                gridcolor='#262626', 
                tickangle=-45,
                fixedrange=False
            ),
            yaxis=dict(
                showgrid=True, 
                gridcolor='#262626', 
                rangemode='tozero', 
                title="ms", 
                title_font=dict(size=12),
                fixedrange=False
            ),
            dragmode='pan'
        )
        
        is_ok = status == "OK"
        status_color = "#00ff87" if is_ok else "#ff4d4d"
        status_bg = '#102015' if is_ok else '#2c1010'
        
        cards.append(html.Div(style=card_style, children=[
            html.Div(style={'borderBottom': f'1px solid {BORDER_COLOR}', 'paddingBottom': '10px', 'marginBottom': '12px'}, children=[
                html.Div(style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center'}, children=[
                    html.H3(r['name'], style={'margin': '0', 'color': TEXT_COLOR, 'fontSize': '18px'}),
                    html.Span(f"Status: {status}", style={
                        'fontSize': '12px', 
                        'color': status_color, 
                        'fontWeight': 'bold', 
                        'backgroundColor': status_bg, 
                        'padding': '2px 8px', 
                        'borderRadius': '4px'
                    })
                ]),
                html.P(r['desc'], style={'margin': '4px 0 0 0', 'color': TEXT_MUTED, 'fontSize': '12px'}),
            ]),
            
            html.Div(style={'display': 'flex', 'justifyContent': 'space-between', 'fontSize': '13px', 'marginBottom': '10px', 'backgroundColor': '#181818', 'padding': '8px 12px', 'borderRadius': '6px'}, children=[
                html.Div([
                    html.Span("IS-IS Uptime: ", style={'color': TEXT_MUTED}),
                    html.Span(uptime, style={'color': TEXT_COLOR, 'fontWeight': 'bold'})
                ]),
                html.Div([
                    html.Span("Peer Up Time: ", style={'color': TEXT_MUTED}),
                    html.Span(peer_up_time, style={'color': TEXT_COLOR, 'fontWeight': 'bold'})
                ]),
                html.Div([
                    html.Span("Latest: ", style={'color': TEXT_MUTED}),
                    html.Span(latest_lat, style={'color': status_color, 'fontWeight': 'bold'})
                ])
            ]),
            
            dcc.Graph(
                figure=fig, 
                config={
                    'displayModeBar': True,
                    'scrollZoom': True,
                    'displaylogo': False,
                    'modeBarButtonsToRemove': ['select2d', 'lasso2d']
                }
            )
        ]))

    return cards

# ==========================================
# 6. RUN APP
# ==========================================
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=8050)