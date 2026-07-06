import json
import aio_pika
from aiokafka import AIOKafkaProducer
from src.domain.ports import MessageBrokerPort

class RabbitMQAdapter(MessageBrokerPort):
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection = None
        self.channel = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.connection_url)
        self.channel = await self.connection.channel()

    async def publish_to_rmq(self, queue: str, message: dict) -> None:
        if not self.channel:
            await self.connect()
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key=queue
        )

    async def publish_to_kafka(self, topic: str, message: dict) -> None:
        pass

class KafkaAdapter(MessageBrokerPort):
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def connect(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self.producer.start()

    async def publish_to_kafka(self, topic: str, message: dict) -> None:
        if not self.producer:
            await self.connect()
        await self.producer.send_and_wait(topic, json.dumps(message).encode())

    async def publish_to_rmq(self, queue: str, message: dict) -> None:
        pass
