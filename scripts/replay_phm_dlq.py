import argparse
import json
from typing import Any, Dict

from kafka import KafkaConsumer, KafkaProducer

PHM_FEATURES = [
    "rms_x", "rms_y", "kurt_x", "kurt_y",
    "p2p_x", "p2p_y", "crest_x", "crest_y",
    "skew_x", "skew_y",
    "spec_entropy_x", "spec_entropy_y",
    "band_energy_x", "band_energy_y",
    "peak_freq_x", "peak_freq_y",
    "fft_x_1", "fft_x_2", "fft_x_3", "fft_x_4", "fft_x_5",
    "fft_y_1", "fft_y_2", "fft_y_3", "fft_y_4", "fft_y_5",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay valid PHM DLQ payloads back to raw topic")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--dlq-topic", default="pdm.phm.raw.dlq", help="DLQ topic to consume")
    parser.add_argument("--raw-topic", default="pdm.phm.raw", help="Raw topic to republish")
    parser.add_argument("--group-id", default="phm-dlq-replay", help="Kafka consumer group id")
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest offsets")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 = no limit)")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, do not republish")
    return parser.parse_args()


def is_number(val: Any) -> bool:
    if isinstance(val, bool):
        return False
    return isinstance(val, (int, float))


def validate_phm_payload(obj: Dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(obj, dict):
        return False, "Payload must be a JSON object"
    if "unit_nr" not in obj:
        return False, "unit_nr missing"
    if "time_cycles" not in obj:
        return False, "time_cycles missing"
    if "event_time" not in obj:
        return False, "event_time missing"
    for f in PHM_FEATURES:
        if f not in obj:
            return False, f"Missing feature {f}"
        if not is_number(obj[f]):
            return False, f"{f} must be numeric"
    return True, ""


def main() -> None:
    args = parse_args()
    consumer = KafkaConsumer(
        args.dlq_topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
    )
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", retries=5)

    print(
        f"Starting PHM DLQ replay. dlq={args.dlq_topic} raw={args.raw_topic} "
        f"from_beginning={args.from_beginning} dry_run={args.dry_run}"
    )

    processed = 0
    try:
        for msg in consumer:
            raw = msg.value.decode("utf-8", "replace")
            try:
                envelope = json.loads(raw)
            except json.JSONDecodeError:
                print("Skip: DLQ message not JSON")
                consumer.commit()
                continue

            payload_raw = envelope.get("raw_payload")
            if not payload_raw:
                print("Skip: DLQ message missing raw_payload")
                consumer.commit()
                continue

            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                print("Skip: raw_payload not JSON")
                consumer.commit()
                continue

            ok, err = validate_phm_payload(payload)
            if not ok:
                print(f"Skip: invalid payload ({err})")
                consumer.commit()
                continue

            if not args.dry_run:
                key = str(payload["unit_nr"]).encode("utf-8")
                producer.send(args.raw_topic, key=key, value=payload_raw.encode("utf-8"))
                producer.flush()

            consumer.commit()
            processed += 1
            if args.max_messages > 0 and processed >= args.max_messages:
                break
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        producer.flush()
        producer.close()
        consumer.close()

    print(f"Replay done. processed={processed}")


if __name__ == "__main__":
    main()
