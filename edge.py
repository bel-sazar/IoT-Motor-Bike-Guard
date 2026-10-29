#!/usr/bin/env python3
import os, json, ssl, time, math, subprocess, argparse, threading
from collections import deque
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

load_dotenv()
ap = argparse.ArgumentParser()
ap.add_argument("--id", required=True)
ap.add_argument("--sim", action="store_true")
ap.add_argument("--lat", type=float, default=-7.0)
ap.add_argument("--lon", type=float, default=110.4)
args = ap.parse_args()
ME, PID = args.id, os.getenv("PROJECT_ID", "ultiot")
radius = float(os.getenv("RADIUS_M", "50"))
PERIOD, STALE_S, RATE_WARN, BREACH_N, G = 2.0, 20, 2.5, 3, 9.81

# ---------- geo ----------
def dist_m(la1, lo1, la2, lo2):
    p1, p2 = math.radians(la1), math.radians(la2)
    dp, dl = p2 - p1, math.radians(lo2 - lo1)
    h = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*6371000*math.asin(math.sqrt(h))

def bearing(la1, lo1, la2, lo2):
    p1, p2, dl = math.radians(la1), math.radians(la2), math.radians(lo2 - lo1)
    y = math.sin(dl)*math.cos(p2)
    x = math.cos(p1)*math.sin(p2) - math.sin(p1)*math.cos(p2)*math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360

COMP = ["N","NE","E","SE","S","SW","W","NW"]
REL = ["ahead","ahead-right","right","behind-right","behind","behind-left","left","ahead-left"]
compass = lambda b: COMP[int((b + 22.5)//45) % 8]
rel = lambda b, h: REL[int(((b - h) % 360 + 22.5)//45) % 8]

# ---------- sensors ----------
gps = {"t": 0}
accel = deque(maxlen=20)

class Stream:  # parses a stream of JSON objects from a command
    def __init__(s, cmd, cb):
        s.p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)
        s.cb = cb
        threading.Thread(target=s.run, daemon=True).start()
    def run(s):
        buf, dec = "", json.JSONDecoder()
        while True:
            d = os.read(s.p.stdout.fileno(), 4096)
            if not d: break
            buf += d.decode(errors="ignore")
            while True:
                buf = buf.lstrip()
                if not buf: break
                try: obj, i = dec.raw_decode(buf)
                except ValueError: break
                buf = buf[i:]
                s.cb(obj)

def on_gps(o):
    if "latitude" in o:
        gps.update(lat=o["latitude"], lon=o["longitude"], acc=o.get("accuracy", 99),
                   speed=o.get("speed") or 0, hdg=o.get("bearing") or 0, t=time.time())

def on_acc(o):
    for v in o.values():
        x, y, z = v["values"][:3]
        accel.append(math.sqrt(x*x + y*y + z*z) - G)

def start_sensors():
    if args.sim:
        def sim():
            lat, lon, t0 = args.lat, args.lon, time.time()
            while True:
                moving = ME == "bike" and time.time() - t0 > 20
                if moving: lat += 8/111320
                gps.update(lat=lat, lon=lon, acc=5, speed=8 if moving else 0, hdg=0, t=time.time())
                accel.append(1.0 if moving else 0.05)
                time.sleep(1)
        threading.Thread(target=sim, daemon=True).start()
        return
    subprocess.Popen(["termux-wake-lock"])
    names = json.loads(subprocess.run(["termux-sensor", "-l"], capture_output=True, text=True).stdout)["sensors"]
    an = next(n for n in names if "accel" in n.lower())
    Stream(["termux-location", "-p", "gps", "-r", "updates"], on_gps)
    Stream(["termux-sensor", "-s", an, "-d", "100"], on_acc)

accel_rms = lambda: math.sqrt(sum(e*e for e in accel)/len(accel)) if accel else 0.0

# ---------- alert ----------
def alert(msg):
    print("ALERT:", msg)
    if not args.sim:
        subprocess.Popen(["termux-vibrate", "-d", "1500"])
        subprocess.Popen(["termux-notification", "--id", "1", "--priority", "max",
                          "--title", "Tracker", "--content", msg])

# ---------- MQTT ----------
peer, hist = {}, deque()
T = lambda s: f"{PID}/{s}"

def on_connect(c, u, f, rc, p):
    print("connected", rc)
    c.subscribe([(T("edge/+/telemetry"), 0), (T("config"), 1)])
    c.publish(T(f"edge/{ME}/status"), "online", qos=1, retain=True)

def on_message(c, u, m):
    global radius
    try: d = json.loads(m.payload)
    except ValueError: return
    if m.topic.endswith("/config"):
        radius = float(d.get("radius_m", radius))
    elif d.get("device_id") != ME:
        peer.update(d, rx=time.time())

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
if os.getenv("MQTT_USER"):
    c.username_pw_set(os.environ["MQTT_USER"], os.environ["MQTT_PASS"])
if os.getenv("MQTT_TLS") == "1":
    c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
c.will_set(T(f"edge/{ME}/status"), "offline", qos=1, retain=True)
c.reconnect_delay_set(1, 30)
c.on_connect, c.on_message = on_connect, on_message
c.connect(os.getenv("MQTT_HOST", "127.0.0.1"), int(os.getenv("MQTT_PORT", "1883")), keepalive=30)
c.loop_start()

# ---------- decision ----------
breach, seq, last_alert, last_state = 0, 0, 0, "NORMAL"

def evaluate():
    global breach
    now = time.time()
    if now - peer.get("rx", 0) > STALE_S: return "LOST", f"no data from other phone >{STALE_S}s", {}
    if now - gps["t"] > STALE_S: return "NOFIX", "own GPS has no fix", {}
    d = dist_m(gps["lat"], gps["lon"], peer["lat"], peer["lon"])
    hist.append((now, d))
    while now - hist[0][0] > 10: hist.popleft()
    span = now - hist[0][0]
    rate = (d - hist[0][1])/span if span >= 4 else 0.0          # m/s, + = separating
    margin = (gps["acc"] + peer["acc_m"])/2
    if d - margin > radius: breach += 1
    elif d < 0.8*radius: breach = 0                              # hysteresis
    b = bearing(gps["lat"], gps["lon"], peer["lat"], peer["lon"])
    where = compass(b) + (f", {rel(b, gps['hdg'])}" if gps["speed"] > 1.5 else "")
    info = {"dist_m": round(d), "rate_ms": round(rate, 1), "bearing": round(b),
            "peer_kmh": round(peer["speed_ms"]*3.6), "peer_accel_rms": peer["accel_rms"]}
    txt = f"{peer['device_id']} {d:.0f} m {where}, {info['peer_kmh']} km/h, {rate:+.1f} m/s"
    if breach >= BREACH_N: return "ALERT", "OUT OF PERIMETER: " + txt, info
    if rate >= RATE_WARN and d > 0.4*radius:
        eta = (radius - d)/rate
        return "WARN", f"SEPARATING FAST: {txt}, ~{max(eta,0):.0f}s to limit", info
    return "NORMAL", txt, info

start_sensors()
while True:
    time.sleep(PERIOD)
    if gps["t"]:
        seq += 1
        c.publish(T(f"edge/{ME}/telemetry"), json.dumps({
            "device_id": ME, "sequence": seq, "ts_ms": int(time.time()*1000),
            "lat": gps["lat"], "lon": gps["lon"], "acc_m": gps["acc"],
            "speed_ms": gps["speed"], "bearing_deg": gps["hdg"],
            "accel_rms": round(accel_rms(), 3)}), qos=0)
    state, msg, info = evaluate()
    print(state, msg)
    c.publish(T(f"state/{ME}"), json.dumps({"state": state, "msg": msg, **info}), qos=1, retain=True)
    if state in ("ALERT", "WARN", "LOST") and (state != last_state or time.time() - last_alert > 10):
        alert(msg); last_alert = time.time()
    last_state = state

