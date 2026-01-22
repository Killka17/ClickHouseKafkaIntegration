import argparse
import sys
import time
from typing import Iterable

from kafka import KafkaProducer


def iter_values(args: argparse.Namespace) -> Iterable[bytes]:
    if args.values:
        for v in args.values:
            yield f"{v}\n".encode("utf-8")
        return

    for line in sys.stdin:
        if not line:
            continue
        yield line.encode("utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description="Produce integer messages (as text) to Kafka topic.")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--topic", default="numbers", help="Kafka topic")
    p.add_argument("--values", nargs="*", help="Values to send (otherwise read from stdin)")
    p.add_argument("--delay-ms", type=int, default=0, help="Delay between messages")
    args = p.parse_args()

    producer = KafkaProducer(bootstrap_servers=[args.bootstrap])

    sent = 0
    for payload in iter_values(args):
        producer.send(args.topic, payload)
        sent += 1
        if args.delay_ms:
            time.sleep(args.delay_ms / 1000.0)

    producer.flush(10)
    producer.close(10)
    print(f"Sent {sent} message(s) to {args.topic} via {args.bootstrap}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


