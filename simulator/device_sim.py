import argparse
import collections
import datetime as dt
import json
import math
import random
import time

import cbor2
from jsonschema import Draft202012Validator

SCHEMA_VERSION = "1.0"
BUF_CAP = 500

SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "DeviceMessage",
    "type": "object",
    "required": ["device_id", "ts", "seq", "metrics", "schema_version"],
    "properties": {
        "device_id": {"type": "string", "pattern": "^[a-z0-9-]{3,32}$"},
        "ts": {"type": "integer", "minimum": 1577836800000,
               "description": "device_ts, epoch ms по часам устройства"},
        "seq": {"type": "integer", "minimum": 0},
        "schema_version": {"const": SCHEMA_VERSION},
        "metrics": {
            "type": "object", "minProperties": 1, "additionalProperties": False,
            "properties": {
                "temperature": {"type": "number", "minimum": -40, "maximum": 85},
                "humidity": {"type": "number", "minimum": 0, "maximum": 100},
                "pressure": {"type": "number", "minimum": 300, "maximum": 1100},
                "motion": {"type": "integer", "enum": [0, 1]},
            },
        },
        "battery": {"type": "number", "minimum": 0, "maximum": 100},
        "rssi": {"type": "integer", "minimum": -120, "maximum": 0},
        "buf_dropped": {"type": "integer", "minimum": 0,
                        "description": "сколько записей потеряно из-за переполнения буфера"},
    },
    "additionalProperties": False,
}
VALIDATOR = Draft202012Validator(SCHEMA)

class Sensor:
    """значение = base + суточный цикл + дрейф + шум + редкий выброс"""

    def __init__(self, base, amp, noise, drift_per_h, outlier_p, outlier_mag, peak_h, t0):
        self.__dict__.update(locals())

    def read(self, t):
        h = (t % 86400) / 3600
        v = self.base + self.amp * math.sin(2 * math.pi * (h - self.peak_h + 6) / 24)
        v += self.drift_per_h * (t - self.t0) / 3600
        v += random.gauss(0, self.noise)
        if random.random() < self.outlier_p:
            v += random.choice([-1, 1]) * self.outlier_mag
        return v


class PIR:
    def read(self, t):
        h = (t % 86400) / 3600
        return 1 if random.random() < (0.30 if 8 <= h < 22 else 0.02) else 0


def make_sensors(t0):
    return {
        "temperature": Sensor(22, 4, 0.15, 0.01, 0.01, 30, 15, t0),
        "humidity": Sensor(45, 10, 0.5, -0.02, 0.01, 40, 5, t0),
        "pressure": Sensor(1013, 2, 0.2, 0.0, 0.005, 150, 10, t0),
        "motion": PIR(),
    }

RANGES = {"temperature": (-40, 85), "humidity": (0, 100), "pressure": (300, 1100), "motion": (0, 1)}


class ValueChecker:
    def __init__(self, stuck_n=20):
        self.hist = {k: collections.deque(maxlen=stuck_n) for k in RANGES}

    def check(self, name, v):
        """None если значение годное, иначе причина отбраковки"""
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return "nan"
        lo, hi = RANGES[name]
        if not lo <= v <= hi:
            return "range"
        if name != "motion":  
            h = self.hist[name]
            h.append(round(v, 2))
            if len(h) == h.maxlen and len(set(h)) == 1:
                return "stuck"
        return None

def enc_json(m):
    return json.dumps(m, separators=(",", ":")).encode()


def enc_cbor(m):
    return cbor2.dumps(m)


TOP = {"device_id": 1, "ts": 2, "seq": 3, "metrics": 4, "schema_version": 5,
       "battery": 6, "rssi": 7, "buf_dropped": 8}
MET = {"temperature": 1, "humidity": 2, "pressure": 3, "motion": 4}


def enc_cbor_short(m):  
    out = {TOP[k]: ({MET[a]: b for a, b in v.items()} if k == "metrics" else v) for k, v in m.items()}
    return cbor2.dumps(out)


ENC = {"json": enc_json, "cbor": enc_cbor}
DEC = {"json": json.loads, "cbor": cbor2.loads}

class SimBroker:
    """Вместо реального MQTT: можно 'выключить' и проверить приём."""

    def __init__(self, fmt):
        self.fmt, self.online, self.records = fmt, True, []

    def publish(self, payload, now):
        if not self.online:
            return False
        msg = DEC[self.fmt](payload)
        errs = list(VALIDATOR.iter_errors(msg))
        assert not errs, errs[0].message
        msg["ingest_ts"] = int(now * 1000) 
        self.records.append(msg)
        return True


class MqttBroker:
    def __init__(self, host, port=1883, topic="lab1/esp32-001/telemetry"):
        import paho.mqtt.client as mqtt
        self.topic = topic
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.c.connect_async(host, port)
        self.c.loop_start()

    def publish(self, payload, now):
        if not self.c.is_connected():
            return False
        return self.c.publish(self.topic, payload, qos=1).rc == 0

class Device:
    def __init__(self, device_id, broker, fmt, t0):
        self.id, self.broker, self.fmt = device_id, broker, fmt
        self.sensors, self.checker = make_sensors(t0), ValueChecker()
        self.buf = collections.deque()   
        self.seq = 0
        self.dropped = 0
        self.rejected = collections.Counter()
        self.battery = 100.0

    def tick(self, t):
        metrics = {}
        for name, s in self.sensors.items():
            v = s.read(t)
            if name == "motion":
                v = int(v)
            reason = self.checker.check(name, v)
            if reason:
                self.rejected[(name, reason)] += 1
            else:
                metrics[name] = v if name == "motion" else round(v, 2)
        if metrics:
            self.battery -= 0.001
            msg = {"device_id": self.id, "ts": int(t * 1000), "seq": self.seq,
                   "metrics": metrics, "schema_version": SCHEMA_VERSION,
                   "battery": round(self.battery, 1), "rssi": -60 + random.randint(-5, 5)}
            if self.dropped:
                msg["buf_dropped"] = self.dropped
            assert not list(VALIDATOR.iter_errors(msg))
            if len(self.buf) >= BUF_CAP:      
                self.buf.popleft()
                self.dropped += 1
            self.buf.append((self.seq, ENC[self.fmt](msg)))
            self.seq += 1
        self.flush(t)

    def flush(self, now):
        while self.buf:
            if not self.broker.publish(self.buf[0][1], now):
                return                         
            self.buf.popleft()               

T0 = int(dt.datetime(2026, 10, 5, 12, 0, tzinfo=dt.timezone.utc).timestamp())


def scenario(ticks, out_start, out_len, fmt="cbor", seed=1):
    random.seed(seed)
    br = SimBroker(fmt)
    dev = Device("esp32-001", br, fmt, T0)
    max_buf = 0
    for i in range(ticks):
        br.online = not (out_start <= i < out_start + out_len)
        dev.tick(T0 + i)
        max_buf = max(max_buf, len(dev.buf))
    seqs = [r["seq"] for r in br.records]
    missing = sorted(set(range(dev.seq)) - set(seqs))
    lat = max(r["ingest_ts"] - r["ts"] for r in br.records) / 1000
    return dict(dev=dev, br=br, seqs=seqs, missing=missing, max_buf=max_buf, max_latency_s=lat)


def test_validator():
    c = ValueChecker(stuck_n=5)
    assert c.check("temperature", float("nan")) == "nan"
    assert c.check("temperature", 120.0) == "range"
    assert c.check("humidity", -5.0) == "range"
    res = [c.check("pressure", 1013.25) for _ in range(5)]
    assert res[:4] == [None] * 4 and res[4] == "stuck"


def sizes():
    random.seed(1)
    d = Device("esp32-001", SimBroker("json"), "json", T0)
    d.broker.online = False
    d.tick(T0)
    msg = json.loads(d.buf[0][1])
    j, c, cs = len(enc_json(msg)), len(enc_cbor(msg)), len(enc_cbor_short(msg))
    return msg, j, c, cs


def report():
    test_validator()
    a = scenario(180, 60, 60)
    assert a["missing"] == [] and a["seqs"] == sorted(a["seqs"]) == list(range(a["dev"].seq))
    assert len(a["dev"].buf) == 0 and a["dev"].dropped == 0 and a["max_buf"] <= 61
    b = scenario(900, 100, 700)
    d = b["dev"]
    assert d.dropped == len(b["missing"]) == 700 - BUF_CAP + 1 or d.dropped == len(b["missing"])
    assert b["max_buf"] == BUF_CAP
    assert b["missing"] == list(range(b["missing"][0], b["missing"][-1] + 1))  # один непрерывный провал
    assert b["br"].records[-1]["buf_dropped"] == d.dropped
    msg, j, c, cs = sizes()

    L = ["# Lab 1 - отчёт", "", "## Пример сообщения", "```json", json.dumps(msg, indent=1), "```", "",
         "## Формат -> байт/сообщение -> трафик/сутки (интервал 10 с = 8640 сообщений/сутки)", "",
         "| Формат | байт/сообщ. | экономия | КБ/сут (1 устр.) | МБ/сут (1000) | ГБ/сут (100 000) |",
         "|---|---|---|---|---|---|"]
    for n, s in [("JSON", j), ("CBOR", c), ("CBOR + int-ключи", cs)]:
        day = s * 8640
        L.append(f"| {n} | {s} | {100 * (j - s) / j:.1f}% | {day / 1024:.0f} | {day * 1000 / 1e6:.0f} | {day * 1e5 / 1e9:.1f} |")
    L += ["", "Только payload приложения, без заголовков MQTT/TCP/TLS.", "",
          "## Обрыв брокера на 60 c (сценарий A)",
          f"- отправлено устройством: {a['dev'].seq}, принято: {len(a['seqs'])}, пропусков seq: {len(a['missing'])}",
          f"- максимум в буфере: {a['max_buf']} из {BUF_CAP}, после восстановления буфер: {len(a['dev'].buf)}",
          f"- макс. задержка доставки (ingest_ts - ts): {a['max_latency_s']:.0f} c",
          "", f"## Переполнение: обрыв 700 c (сценарий B)",
          f"- максимум в буфере: {b['max_buf']}, потеряно (drop-oldest): {d.dropped}",
          f"- пропущены seq {b['missing'][0]}..{b['missing'][-1]} - один непрерывный провал, приёмник видит его по seq",
          f"- последнее сообщение несёт buf_dropped={b['br'].records[-1]['buf_dropped']}",
          "", f"## Отбраковка (сценарий A): {dict(a['dev'].rejected)}"]
    open("report.md", "w", encoding="utf-8").write("\n".join(L))
    open("message.schema.json", "w", encoding="utf-8").write(json.dumps(SCHEMA, indent=2, ensure_ascii=False))
    print("\n".join(L))
    print("\nВСЕ ТЕСТЫ ПРОЙДЕНЫ")


def live(host, fmt, interval):
    dev = Device("esp32-001", MqttBroker(host), fmt, time.time())
    while True:
        dev.tick(time.time())
        print(f"seq={dev.seq - 1} buf={len(dev.buf)}/{BUF_CAP} dropped={dev.dropped} "
              f"broker_online={dev.broker.c.is_connected()}", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live")
    ap.add_argument("--fmt", default="cbor", choices=["json", "cbor"])
    ap.add_argument("--interval", type=float, default=5)
    a = ap.parse_args()
    live(a.live, a.fmt, a.interval) if a.live else report()