import aio_pika
import os
import json
from datetime import datetime

# Environment variables from docker-compose
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "mbw")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD") or os.getenv("RABBITMQ_PASS", "mbw123")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

async def publish_message(routing_key: str, payload: dict):
    """
    Publish a persistent message to the RabbitMQ topic exchange.
    routing_key = queue name (e.g., 'inactive_queue')
    """
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        virtualhost=RABBITMQ_VHOST,
    )

    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        await channel.declare_queue(routing_key, durable=True)

        # Persistent message (survives RabbitMQ restart)
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await channel.default_exchange.publish(message, routing_key=routing_key, mandatory=True)
        print(f"Published to '{routing_key}': {payload} - executed at {datetime.now()}")
