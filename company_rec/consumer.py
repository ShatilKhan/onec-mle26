"""RabbitMQ consumer: parse -> dedupe -> embed -> persist -> ack, with app-level DLQ."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections import Counter

import aio_pika
import asyncpg
from aio_pika.abc import AbstractIncomingMessage

from company_rec.config import Settings
from company_rec.db import claim, init_schema, upsert
from company_rec.embed import Embedder
from company_rec.events import SENTINEL, Event, parse, surrogate_text

log = logging.getLogger("company_rec.consumer")


async def publish_poison(
    channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
    message: AbstractIncomingMessage,
    mid: str,
    reason: str,
) -> None:
    exchange = await channel.declare_exchange(settings.dlx, aio_pika.ExchangeType.DIRECT, durable=True)
    queue = await channel.declare_queue(settings.dlq, durable=True)
    await queue.bind(exchange, routing_key=settings.dlq)
    body = json.dumps(
        {"message_id": mid, "reason": reason, "body": message.body.decode("utf-8", errors="replace")}
    ).encode()
    await exchange.publish(
        aio_pika.Message(
            body,
            message_id=mid,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={"reason": reason, "message_id": mid},
        ),
        routing_key=settings.dlq,
    )


async def handle(
    message: AbstractIncomingMessage,
    pg: asyncpg.Pool,
    channel: aio_pika.abc.AbstractChannel,
    embedder: Embedder,
    settings: Settings,
    counts: Counter[str],
) -> None:
    mid = message.message_id or ""
    parsed = parse(message.body, mid or None)
    mid = parsed.message_id
    if parsed.reason:
        counts[parsed.reason] += 1
        async with pg.acquire() as conn:
            claimed = await claim(conn, mid)
        if claimed:
            if parsed.shell and parsed.reg:
                shell = Event(
                    reg=parsed.reg,
                    kind="company-upserted",
                    updated_at=SENTINEL,
                    message_id=mid,
                    company_name="Unknown",
                    nature="",
                    state="",
                    industry="",
                    valid_data=False,
                )
                async with pg.acquire() as conn:
                    await upsert(conn, shell, None, None, False)
            await publish_poison(channel, settings, message, mid, parsed.reason)
        log.warning("poison message_id=%s reason=%s", mid, parsed.reason)
        await message.ack()
        return

    event = parsed.event
    assert event is not None
    async with pg.acquire() as conn:
        async with conn.transaction():
            if not await claim(conn, mid):
                duplicate = True
            else:
                duplicate = False
                if event.kind == "company-struck-off":
                    await upsert(conn, event, None, None, False)
                    counts["struck-off"] += 1
                elif event.nature:
                    vector = embedder.embed(event.nature)
                    await upsert(conn, event, vector, settings.model, False)
                    counts["embedded"] += 1
                else:
                    text = surrogate_text(event)
                    vector = embedder.embed(text) if text else None
                    await upsert(conn, event, vector, settings.model if vector else None, bool(vector))
                    counts["surrogate" if vector else "shell"] += 1
    # Commit happened before ack: a crash before ack requeues, a crash after ack loses nothing.
    await message.ack()
    if duplicate:
        counts["duplicate"] += 1
    else:
        counts["handled"] += 1


async def consume(settings: Settings) -> None:
    pg = await asyncpg.create_pool(settings.dsn)
    async with pg:
        async with pg.acquire() as conn:
            await init_schema(conn)
        connection = await aio_pika.connect_robust(settings.amqp_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=10)
            queue = await channel.declare_queue(settings.queue, durable=True)
            embedder = Embedder(settings.model)
            counts: Counter[str] = Counter()
            async with queue.iterator() as stream:
                async for message in stream:
                    try:
                        await handle(message, pg, channel, embedder, settings, counts)
                    except Exception:
                        log.exception("consumer failed message; requeueing")
                        await message.nack(requeue=True)
            log.info("consumer stopped: %s", dict(counts))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    try:
        asyncio.run(consume(settings))
    except KeyboardInterrupt:
        log.info("consumer interrupted")


if __name__ == "__main__":
    main()
