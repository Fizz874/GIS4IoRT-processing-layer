import sys
import threading
import time
import json
import math
import paho.mqtt.client as mqtt
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
from shapely import wkb
from datetime import datetime

if len(sys.argv) < 9:
    print("Usage: python3 visualizer.py <MQTT_IP> <MQTT_PORT> <VIS_PORT> <TOPICS> <QUERIES> <HUM_THRESH> <BUF_RADIUS> <FIELD_FILE>")
    sys.exit(1)

MQTT_IP = sys.argv[1]
MQTT_PORT = int(sys.argv[2])
VISUALIZER_PORT = int(sys.argv[3])
DATA_TOPICS = sys.argv[4].split(';')
QUERY_NAMES = sys.argv[5].split(';')
HUMIDITY_THRESHOLD = float(sys.argv[6])
BUFFER_RADIUS = float(sys.argv[7])
FIELD_FILE_PATH = sys.argv[8]
ALL_TOPICS = DATA_TOPICS + QUERY_NAMES
ROBOT_TOPICS = DATA_TOPICS[0:3]
HUMIDITY_TOPIC = DATA_TOPICS[3]
GEOFENCE_QUERY = QUERY_NAMES[0]
HUMIDITY_QUERY = QUERY_NAMES[1]
COLLISION_QUERY = QUERY_NAMES[2]
ROBOT_COLORS = ['#FFD700', '#00FFFF', '#FF00FF']
ROBOT_COLOR_MAP = {f"robot_{i}": ROBOT_COLORS[i] for i in range(len(ROBOT_COLORS))}

def load_field_polygon(path):
    try:
        with open(path, 'r') as f:
            hex_string = f.read().strip()
        poly = wkb.loads(bytes.fromhex(hex_string))
        return poly
    except Exception as e:
        print(f"Error: {e}", flush=True)
        sys.exit(1)

field_polygon = load_field_polygon(FIELD_FILE_PATH)
x_coords_field, y_coords_field = field_polygon.exterior.xy
centroid = field_polygon.centroid
center_lat = centroid.y
center_lon = centroid.x

data_lock = threading.Lock()
latest_sensor_data = {}
robot_history = {}
geofence_events = []
humidity_events = []
collision_events = []

def format_timestamp(ts_ms):
    dt = datetime.fromtimestamp(ts_ms / 1000.0)
    return dt.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
def window_timestamp(ts_ms):
    dt = datetime.fromtimestamp(ts_ms / 1000.0)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def generate_circle_coords(lat, lon, radius_meters, num_points=32):
    lats = []
    lons = []
    earth_radius = 6378137.0
    for i in range(num_points + 1):
        angle = math.radians(float(i) / num_points * 360.0)
        d_lat = (radius_meters / earth_radius) * (180.0 / math.pi)
        d_lon = (radius_meters / earth_radius) * (180.0 / math.pi) / math.cos(math.radians(lat))
        lats.append(lat + d_lat * math.sin(angle))
        lons.append(lon + d_lon * math.cos(angle))
    return lats, lons

def mqtt_thread_target():
    client = mqtt.Client(
        client_id="visualizer",
        transport="websockets",
        protocol=mqtt.MQTTv5,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    def on_connect(client, userdata, flags, reasonCode, properties):
        for topic in ALL_TOPICS:
            client.subscribe(topic)
    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            if msg.topic == HUMIDITY_TOPIC:
                s_id = payload.get('sensor_id')
                if s_id is not None:
                    with data_lock:
                        latest_sensor_data[s_id] = payload
            elif msg.topic in ROBOT_TOPICS:
                r_name = payload.get('robot_name')
                if r_name is not None:
                    with data_lock:
                        if r_name not in robot_history:
                            robot_history[r_name] = []
                            if r_name not in ROBOT_COLOR_MAP:
                                idx = len(ROBOT_COLOR_MAP) % len(ROBOT_COLORS)
                                ROBOT_COLOR_MAP[r_name] = ROBOT_COLORS[idx]
                        lat = payload.get('position_y')
                        lon = payload.get('position_x')
                        ts = payload.get('timestamp')
                        if lat is not None and lon is not None and ts is not None:
                            robot_history[r_name].append({
                                'lat': float(lat),
                                'lon': float(lon),
                                'ts': int(ts)
                            })
            elif msg.topic == GEOFENCE_QUERY:
                r_name = payload.get('gps_position$robot_name')
                ts = payload.get('gps_position$timestamp')
                exited = payload.get('gps_position$exited')
                if exited and r_name and ts:
                    with data_lock:
                        geofence_events.insert(0, {
                            'robot_name': r_name,
                            'timestamp': int(ts),
                            'event': 'OUT'
                        })
                        if len(geofence_events) > 100:
                            geofence_events.pop()
            elif msg.topic == HUMIDITY_QUERY:
                r_name = payload.get('gps_position$robot_name')
                s_id = payload.get('gps_position$sensor_id')
                ts_end = payload.get('gps_position$end')
                if r_name is not None and s_id is not None and ts_end is not None:
                    with data_lock:
                        humidity_events.insert(0, {
                            'robot_name': r_name,
                            'sensor_id': s_id,
                            'timestamp_end': ts_end,
                            'event': 'ALERT'
                        })
                        if len(humidity_events) > 100:
                            humidity_events.pop()
            elif msg.topic == COLLISION_QUERY:
                r1_name = payload.get('gps_position$robot_nameR1')
                r2_name = payload.get('gps_position$robot_nameR2')
                ts_end = payload.get('gps_position$end')
                if r1_name and r2_name and ts_end:
                    with data_lock:
                        collision_events.insert(0, {
                            'robot1_name': r1_name,
                            'robot2_name': r2_name,
                            'timestamp_end': ts_end,
                            'event': 'COLLISION'
                        })
                        if len(collision_events) > 100:
                            collision_events.pop()
        except Exception as e:
            print(f"Error: {e}", flush=True)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_IP, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f"Error: {e}", flush=True)

mqtt_th = threading.Thread(target=mqtt_thread_target, daemon=True)
mqtt_th.start()

app = dash.Dash(__name__)
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Visualizer</title>
        {%favicon%}
        {%css%}
        <style>
            body { margin: 0; padding: 0; overflow: hidden; background-color: black; }
            ::-webkit-scrollbar { width: 8px; }
            ::-webkit-scrollbar-track { background: #121212; }
            ::-webkit-scrollbar-thumb { background: #333; border-radius: 4px; }
            ::-webkit-scrollbar-thumb:hover { background: #444; }
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
SECTION_STYLE = {
    'flex': '1',
    'display': 'flex',
    'flexDirection': 'column',
    'overflow': 'hidden',
    'borderBottom': '1px solid #333',
    'padding': '10px'
}
LOG_CONTENT_STYLE = {
    'flex': '1',
    'overflowY': 'auto',
    'marginTop': '5px'
}
app.layout = html.Div(
    style={
        'display': 'flex',
        'flexDirection': 'row',
        'height': '100vh',
        'width': '100vw',
        'margin': '0',
        'padding': '0',
        'overflow': 'hidden',
        'backgroundColor': 'black'
    },
    children=[
        html.Div(
            style={'flex': '3', 'height': '100vh', 'position': 'relative'},
            children=[
                dcc.Graph(
                    id='live-map',
                    figure=go.Figure(), 
                    style={'height': '100vh', 'width': '100%'},
                    config={'displayModeBar': False, 'scrollZoom': True}
                )
            ]
        ),
        html.Div(
            style={
                'flex': '1',
                'height': '100vh',
                'backgroundColor': '#121212',
                'borderLeft': '1px solid #333',
                'display': 'flex',
                'flexDirection': 'column',
                'color': '#cccccc',
                'fontFamily': 'Consolas, monospace',
                'fontSize': '12px',
                'boxSizing': 'border-box'
            },
            children=[
                html.Div(style=SECTION_STYLE, children=[
                    html.H3("Geofence Query", style={'color': '#dd4444', 'margin': '0', 'fontSize': '20px'}),
                    html.Div(id='geofence-log-content', style=LOG_CONTENT_STYLE)
                ]),
                html.Div(style=SECTION_STYLE, children=[
                    html.H3("Humidity Query", style={'color': '#44dd44', 'margin': '0', 'fontSize': '20px'}),
                    html.Div(id='humidity-log-content', style=LOG_CONTENT_STYLE)
                ]),
                html.Div(style={**SECTION_STYLE, 'borderBottom': 'none'}, children=[
                    html.H3("Collision Query", style={'color': '#4444dd', 'margin': '0', 'fontSize': '20px'}),
                    html.Div(id='collision-log-content', style=LOG_CONTENT_STYLE)
                ]),
            ]
        ),
        dcc.Interval(id='interval-component', interval=500, n_intervals=0)
    ]
)

@app.callback(
    [Output('live-map', 'figure'),
     Output('geofence-log-content', 'children'),
     Output('humidity-log-content', 'children'),
     Output('collision-log-content', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_map(n):
    fig = go.Figure()
    now_ms = time.time() * 1000
    cutoff_ms = now_ms - 60000
    geofence_log = []
    humidity_log = []
    collision_log = []
    fig.add_trace(go.Scattermap(
        lon=list(x_coords_field),
        lat=list(y_coords_field),
        mode='lines',
        fill="toself",
        fillcolor="rgba(0, 128, 255, 0.25)",
        line=dict(color="blue", width=2),
        hoverinfo='none',
        showlegend=False
    ))
    with data_lock:
        valid_geofence = [p for p in geofence_events if p['timestamp'] > cutoff_ms]
        valid_humidity = [p for p in humidity_events if p['timestamp_end'] > cutoff_ms]
        valid_collision = [p for p in collision_events if p['timestamp_end'] > cutoff_ms]
        for r_name, history in robot_history.items():
            valid_history = [p for p in history if p['ts'] > cutoff_ms]
            robot_history[r_name] = valid_history
            if not valid_history: 
                continue
            
            color = ROBOT_COLOR_MAP.get(r_name, '#FFFFFF')
            
            fig.add_trace(go.Scattermap(
                lon=[p['lon'] for p in valid_history], 
                lat=[p['lat'] for p in valid_history], 
                mode='lines',
                line=dict(color=color, width=3),
                hoverinfo='none',
                showlegend=False
            ))
            last_pos = valid_history[-1]
            fig.add_trace(go.Scattermap(
                lon=[last_pos['lon']],
                lat=[last_pos['lat']],
                mode='markers',
                marker=dict(size=12, color=color),
                hoverinfo='none',
                showlegend=False
            ))
        for sensor in list(latest_sensor_data.values()):
            try:
                s_id = sensor.get('sensor_id')
                lat = float(sensor.get('position_y'))
                lon = float(sensor.get('position_x'))
                hum = float(sensor.get('humidity'))
                s_color = 'red' if hum >= HUMIDITY_THRESHOLD else 'white'
                c_lats, c_lons = generate_circle_coords(lat, lon, BUFFER_RADIUS)
                fig.add_trace(go.Scattermap(
                    lon=c_lons,
                    lat=c_lats,
                    mode='lines',
                    fill='toself',
                    fillcolor="rgba(255,255,255,0.1)",
                    line=dict(color=s_color, width=1),
                    hoverinfo='none',
                    showlegend=False
                ))
                fig.add_trace(go.Scattermap(
                    lon=[lon],
                    lat=[lat],
                    mode='markers+text',
                    marker=dict(size=9, color=s_color),
                    text=[f"ID: {s_id}"],
                    textposition="top center",
                    textfont=dict(size=12, color=s_color),
                    hoverinfo='none',
                    showlegend=False
                ))
            except: 
                continue
        if not valid_geofence:
            geofence_log = [html.Div("No recent alerts", style={'color': '#444'})]
        else:
            for ev in valid_geofence:
                r_color = ROBOT_COLOR_MAP.get(ev['robot_name'], '#ccc')
                log_line = html.Div([
                    html.Span(f"{format_timestamp(ev['timestamp'])} - "),
                    html.Span(f"{ev['robot_name']}", style={'color': r_color, 'fontWeight': 'bold'}),
                    html.Span(f" - {ev['event']}")
                ], style={'marginBottom': '2px', 'whiteSpace': 'nowrap'})
                geofence_log.append(log_line)
        if not valid_humidity:
            humidity_log = [html.Div("No recent alerts", style={'color': '#444'})]
        else:
            for ev in valid_humidity:
                r_color = ROBOT_COLOR_MAP.get(ev['robot_name'], '#ccc')
                log_line = html.Div([
                    html.Span(f"{window_timestamp(ev['timestamp_end'])} - "),
                    html.Span(f"{ev['robot_name']}", style={'color': r_color, 'fontWeight': 'bold'}),
                    html.Span(" - "),
                    html.Span(f"Sensor ID: {ev['sensor_id']}", style={'fontWeight': 'bold'}),
                    html.Span(f" - {ev['event']}")
                ], style={'marginBottom': '2px', 'whiteSpace': 'nowrap'})
                humidity_log.append(log_line)
        if not valid_collision:
            collision_log = [html.Div("No recent alerts", style={'color': '#444'})]
        else:
            for ev in valid_collision:
                r1_color = ROBOT_COLOR_MAP.get(ev['robot1_name'], '#ccc')
                r2_color = ROBOT_COLOR_MAP.get(ev['robot2_name'], '#ccc')
                log_line = html.Div([
                    html.Span(f"{window_timestamp(ev['timestamp_end'])} - "),
                    html.Span(f"{ev['robot1_name']}", style={'color': r1_color, 'fontWeight': 'bold'}),
                    html.Span(" - "),
                    html.Span(f"{ev['robot2_name']}", style={'color': r2_color, 'fontWeight': 'bold'}),
                    html.Span(f" - {ev['event']}")
                ], style={'marginBottom': '2px', 'whiteSpace': 'nowrap'})
                collision_log.append(log_line)
        
    fig.update_layout(
        map_style="carto-darkmatter",
        map=dict(center=dict(lat=center_lat, lon=center_lon), zoom=19),
        margin={"r":0,"t":0,"l":0,"b":0},
        showlegend=False,
        uirevision='constant'
    )

    return fig, geofence_log, humidity_log, collision_log

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=VISUALIZER_PORT, use_reloader=False)
