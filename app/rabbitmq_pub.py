import aio_pika
import os
import json

# Environment variables from docker-compose
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "mbw")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "mbw123")

EXCHANGE_NAME = "hrbgjobs_exchange"   # already used by your services

async def publish_message(routing_key: str, payload: dict):
    """
    Publish a persistent message to the RabbitMQ topic exchange.
    routing_key = queue name (e.g., 'inactive_queue')
    """
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        login=RABBITMQ_USER,
        password=RABBITMQ_PASS,
    )

    async with connection:
        channel = await connection.channel()

        # Declare durable topic exchange (idempotent – won't break anything)
        exchange = await channel.declare_exchange(
            EXCHANGE_NAME,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        # Persistent message (survives RabbitMQ restart)
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await exchange.publish(message, routing_key=routing_key)
        print(f"Published to '{routing_key}': {payload}")