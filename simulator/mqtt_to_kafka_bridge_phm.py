import argparse
import datetime as dt
import json
import signal
import sys
import time
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

PHM_FEATURES = [
    "rms_x", "rms_y", "kurt_x", "kurt_y", 
    "p2p_x", "p2p_y", "crest_x", "crest_y", 
    "skew_x", "skew_y",
    "spec_entropy_x", "spec_entropy_y",
    "band_energy_x", "band_energy_y",
    "peak_freq_x", "peak_freq_y",
    "fft_x_1", "fft_x_2", "fft_x_3", "fft_x_4", "fft_x_5",
    "fft_y_1", "fft_y_2", "fft_y_3", "fft_y_4", "fft_y_5"
]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mqtt-broker", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--mqtt-topic", default="factory/pdm/phm/raw")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--raw-topic", default="pdm.phm.raw")
    parser.add_argument("--dlq-topic", default="pdm.phm.raw.dlq")
    return parser.parse_args()

def is_number(val):
    if isinstance(val, bool): return False
    return isinstance(val, (int, float))

def validate_phm_payload(obj):
    if not isinstance(obj, dict): return False, "Must be JSON object"
    if "unit_nr" not in obj: return False, "unit_nr missing"
    if "run_id" in obj and not isinstance(obj["run_id"], str):
        return False, "run_id must be string"
    if "time_cycles" not in obj: return False, "time_cycles missing"
    if "event_time" not in obj: return False, "event_time missing"
    for f in PHM_FEATURES:
        if f not in obj:
            return False, f"Missing feature {f}"
        if not is_number(obj[f]):
            return False, f"{f} must be numeric"
    return True, ""

def build_dlq(error_type, error_msg, raw):
    return {
        "error_type": error_type,
        "error_message": error_msg,
        "received_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "raw_payload": raw
    }

def main():
    args = parse_args()
    producer = KafkaProducer(bootstrap_servers=args.kafka_bootstrap, acks="all", retries=5)
    running = {"value": True}

    def stop_handler(_1, _2):
        running["value"] = False
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    client_id = f"bridge-phm-{dt.datetime.now().strftime('%Y%m%d%H%M%S')}"
    mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv311)

    def on_connect(client, userdata, flags, rc, props=None):
        if rc == 0 or (hasattr(rc, 'is_failure') and not rc.is_failure):
            print(f"Connected to MQTT, subscribing to {args.mqtt_topic}")
            mqtt_client.subscribe(args.mqtt_topic, qos=1)
        else:
            print(f"MQTT connect failed: {rc}")

    def on_message(client, userdata, msg):
        raw = msg.payload.decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as ex:
            dlq = build_dlq("json_parse_error", str(ex), raw)
            producer.send(args.dlq_topic, json.dumps(dlq).encode("utf-8"))
            return
            
        ok, err = validate_phm_payload(parsed)
        if not ok:
            dlq = build_dlq("validation_error", err, raw)
            producer.send(args.dlq_topic, json.dumps(dlq).encode("utf-8"))
            return
            
        key = str(parsed["unit_nr"]).encode("utf-8")
        producer.send(args.raw_topic, key=key, value=raw.encode("utf-8"))

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    print(f"Bridge PHM connecting. MQTT {args.mqtt_broker} -> Kafka {args.kafka_bootstrap}")
    mqtt_client.connect(args.mqtt_broker, args.mqtt_port, keepalive=60)
    mqtt_client.loop_start()

    try:
        while running["value"]:
            time.sleep(0.5)
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
