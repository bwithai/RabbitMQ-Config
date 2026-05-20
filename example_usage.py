"""
Python examples: publish/consume over AMQPS for user-app <-> erp-app.

Install dependency (project root):
  pip install pika

Credentials: set RABBITMQ_DEFAULT_USER / RABBITMQ_DEFAULT_PASS in .env (same as Docker Compose), or override with RABBITMQ_USER / RABBITMQ_PASSWORD.

Runs also read .env next to this file for RABBITMQ_* and RABBITMQ_CERTS_DIR.
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
from pathlib import Path

import pika


# Topology (topic exchange, two queues for bidirectional sync)
EXCHANGE = "app.integration"
EXCHANGE_TYPE = "topic"

QUEUE_ERP_FROM_USER = "erp.sync.from_user"
QUEUE_USER_FROM_ERP = "user.sync.from_erp"

ROUTING_USER_PROFILE_UPDATED = "user.profile.updated"
ROUTING_ERP_PROFILE_UPDATED = "erp.profile.updated"


def _load_dotenv_simple(env_path: Path) -> None:
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if key and key not in os.environ:
            os.environ[key] = val


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _connection_parameters() -> pika.ConnectionParameters:
    _load_dotenv_simple(_project_root() / ".env")

    host = os.getenv("RABBITMQ_BIND_IP", "127.0.0.1")
    port = int(os.getenv("RABBITMQ_AMQP_HOST_PORT", "5671"))
    certs_dir = Path(os.getenv("RABBITMQ_CERTS_DIR", "./certs"))
    if not certs_dir.is_absolute():
        certs_dir = _project_root() / certs_dir
    cafile = certs_dir / "ca.pem"
    if not cafile.is_file():
        print(f"Missing CA file: {cafile}", file=sys.stderr)
        sys.exit(1)

    user = os.getenv("RABBITMQ_USER") or os.getenv("RABBITMQ_DEFAULT_USER", "guest")
    password = os.getenv("RABBITMQ_PASSWORD") or os.getenv("RABBITMQ_DEFAULT_PASS", "guest")
    vhost = os.getenv("RABBITMQ_VHOST") or os.getenv("RABBITMQ_DEFAULT_VHOST", "/")

    ssl_context = ssl.create_default_context(cafile=str(cafile))
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

    # SNI / hostname verification: use connect host (cert should include IP SAN if using IP)
    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=pika.PlainCredentials(user, password),
        ssl_options=pika.SSLOptions(ssl_context, host),
        connection_attempts=3,
        retry_delay=2,
    )


def _declare_topology(ch: pika.channel.Channel) -> None:
    ch.exchange_declare(
        exchange=EXCHANGE,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )


def publish_user_profile_update(payload: dict) -> None:
    """user-app: send profile update toward erp-app (via queue bound on erp side)."""
    params = _connection_parameters()
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    with pika.BlockingConnection(params) as conn:
        ch = conn.channel()
        _declare_topology(ch)
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_USER_PROFILE_UPDATED,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
    print(f"Published to {EXCHANGE} rk={ROUTING_USER_PROFILE_UPDATED} bytes={len(body)}")


def consume_erp() -> None:
    """erp-app: receive profile (and other user) events from user-app."""
    params = _connection_parameters()

    def on_message(
        ch: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            data = json.loads(body.decode("utf-8"))
            print(f"[erp-app] rk={method.routing_key} payload={data}")
        except json.JSONDecodeError:
            print(f"[erp-app] rk={method.routing_key} raw={body!r}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    with pika.BlockingConnection(params) as conn:
        ch = conn.channel()
        _declare_topology(ch)
        ch.queue_declare(queue=QUEUE_ERP_FROM_USER, durable=True)
        ch.queue_bind(
            exchange=EXCHANGE,
            queue=QUEUE_ERP_FROM_USER,
            routing_key=ROUTING_USER_PROFILE_UPDATED,
        )
        ch.basic_qos(prefetch_count=10)
        ch.basic_consume(queue=QUEUE_ERP_FROM_USER, on_message_callback=on_message)
        print(f"erp-app consuming queue={QUEUE_ERP_FROM_USER}; Ctrl+C to stop")
        ch.start_consuming()


def publish_erp_update(payload: dict) -> None:
    """erp-app: send update toward user-app."""
    params = _connection_parameters()
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    with pika.BlockingConnection(params) as conn:
        ch = conn.channel()
        _declare_topology(ch)
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_ERP_PROFILE_UPDATED,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
    print(f"Published to {EXCHANGE} rk={ROUTING_ERP_PROFILE_UPDATED} bytes={len(body)}")


def consume_user() -> None:
    """user-app: receive events originating from erp-app."""
    params = _connection_parameters()

    def on_message(
        ch: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            data = json.loads(body.decode("utf-8"))
            print(f"[user-app] rk={method.routing_key} payload={data}")
        except json.JSONDecodeError:
            print(f"[user-app] rk={method.routing_key} raw={body!r}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    with pika.BlockingConnection(params) as conn:
        ch = conn.channel()
        _declare_topology(ch)
        ch.queue_declare(queue=QUEUE_USER_FROM_ERP, durable=True)
        ch.queue_bind(
            exchange=EXCHANGE,
            queue=QUEUE_USER_FROM_ERP,
            routing_key=ROUTING_ERP_PROFILE_UPDATED,
        )
        ch.basic_qos(prefetch_count=10)
        ch.basic_consume(queue=QUEUE_USER_FROM_ERP, on_message_callback=on_message)
        print(f"user-app consuming queue={QUEUE_USER_FROM_ERP}; Ctrl+C to stop")
        ch.start_consuming()


def main() -> None:
    parser = argparse.ArgumentParser(description="RabbitMQ AMQPS examples (user-app <-> erp-app)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_pub_u = sub.add_parser("publish-user", help="user-app: publish a sample profile update")
    p_pub_u.add_argument("--user-id", default="u-123", help="Sample user id in JSON payload")

    sub.add_parser("consume-erp", help="erp-app: consume user -> erp queue")

    p_pub_e = sub.add_parser("publish-erp", help="erp-app: publish a sample update to user-app")
    p_pub_e.add_argument("--note", default="synced from ERP", help="Sample text in JSON payload")

    sub.add_parser("consume-user", help="user-app: consume erp -> user queue")

    args = parser.parse_args()

    if args.cmd == "publish-user":
        publish_user_profile_update(
            {"event": "profile.updated", "user_id": args.user_id, "email": "user@example.com"},
        )
    elif args.cmd == "consume-erp":
        consume_erp()
    elif args.cmd == "publish-erp":
        publish_erp_update({"event": "erp.profile.updated", "note": args.note})
    elif args.cmd == "consume-user":
        consume_user()


if __name__ == "__main__":
    main()
