import asyncio
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.infrastructure.web.api import router
from src.infrastructure.scraping.nalog_scraper import NalogScraperAdapter
from src.infrastructure.proxy.stub_proxy import StubProxyProvider
from src.infrastructure.messaging.brokers import RabbitMQAdapter, KafkaAdapter
from src.application.use_cases import SearchDirectorUseCase

PROXY_URL = os.getenv("PROXY_URL")

rmq_adapter = RabbitMQAdapter("amqp://guest:guest@rabbitmq:5672/")
kafka_adapter = KafkaAdapter("kafka:9092")


async def connect_with_retry(connect_func, service_name: str, max_retries: int = 10, delay: float = 3.0):
    """Пытается подключиться к сервису с повторными попытками."""
    for attempt in range(1, max_retries + 1):
        try:
            await connect_func()
            print(f"✅ Connected to {service_name}")
            return
        except Exception as e:
            print(f"⚠️  Attempt {attempt}/{max_retries} to connect to {service_name} failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(delay)
    raise RuntimeError(f"❌ Could not connect to {service_name} after {max_retries} attempts")


@asynccontextmanager
async def lifespan(app: FastAPI):
    proxy_provider = StubProxyProvider(proxy_url=PROXY_URL)
    scraper = NalogScraperAdapter(proxy=await proxy_provider.get_proxy())
    
    print(f"🌐 Proxy configured: {PROXY_URL or 'None (direct connection)'}")

    await connect_with_retry(rmq_adapter.connect, "RabbitMQ")
    await connect_with_retry(kafka_adapter.connect, "Kafka")

    from src.infrastructure.web import api
    api.use_case = SearchDirectorUseCase(scraper, proxy_provider, rmq_adapter)
    api.kafka_use_case = SearchDirectorUseCase(scraper, proxy_provider, kafka_adapter)

    consumer_task = asyncio.create_task(start_rmq_consumer())

    yield

    consumer_task.cancel()
    try:
        if rmq_adapter.connection and not rmq_adapter.connection.is_closed:
            await rmq_adapter.connection.close()
    except Exception:
        pass
    try:
        if kafka_adapter.producer:
            await kafka_adapter.producer.stop()
    except Exception:
        pass
    await scraper.close()


async def start_rmq_consumer():
    from src.infrastructure.web import api
    try:
        async with rmq_adapter.connection:
            channel = await rmq_adapter.connection.channel()
            queue = await channel.declare_queue("pb.nalog.search.request", durable=True)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        payload = json.loads(message.body)
                        search_string = payload.get("search_string")
                        if search_string:
                            await api.kafka_use_case.execute(search_string, is_async_mode=True)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"❌ RMQ Consumer error: {e}")


app = FastAPI(lifespan=lifespan)
app.include_router(router)
