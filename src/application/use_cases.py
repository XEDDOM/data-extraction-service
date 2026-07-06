import asyncio
import time
from datetime import datetime
from typing import Optional
from src.domain.entities import Person, PersonOrgPair, SearchResult
from src.domain.ports import ScraperPort, ProxyPort, MessageBrokerPort
from src.infrastructure.scraping.nalog_scraper import CaptchaRequiredError, NalogApiError

class SearchDirectorUseCase:
    def __init__(self, scraper: ScraperPort, proxy: ProxyPort, broker: MessageBrokerPort):
        self.scraper = scraper
        self.proxy = proxy
        self.broker = broker
        self.semaphore = asyncio.Semaphore(50)

    async def execute(self, search_string: str, is_async_mode: bool = False) -> SearchResult:
        start_time = time.time()
        error_msg = None
        pairs = []

        try:
            proxy_url = await self.proxy.get_proxy()
            
            directors = await self.scraper.search_directors(search_string, limit=100)
            
            tasks = [self._fetch_orgs_for_person(director) for director in directors]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    print(f"⚠️ Error fetching orgs: {result}")
                    continue
                person, orgs = result
                for org in orgs:
                    pairs.append(PersonOrgPair(person=person, organization=org))
                    
        except CaptchaRequiredError as e:
            error_msg = f"Требуется капча: {str(e)}"
        except NalogApiError as e:
            error_msg = f"Ошибка API: {str(e)}"
        except Exception as e:
            error_msg = str(e)

        duration = time.time() - start_time
        collect_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        
        result = SearchResult(
            success=error_msg is None,
            error=error_msg,
            duration=round(duration, 2),
            collect_time=collect_time,
            total=len(pairs),
            entities=pairs
        )

        if is_async_mode:
            await self._publish_async_results(search_string, result)

        return result

    async def _fetch_orgs_for_person(self, person: Person) -> tuple[Person, list]:
        async with self.semaphore:
            try:
                orgs = await self.scraper.get_person_organizations(person, limit=100)
                return person, orgs
            except Exception as e:
                print(f"⚠️ Failed to get orgs for {person.name}: {e}")
                return person, []

    async def _publish_async_results(self, search_string: str, result: SearchResult):
        rmq_status = {
            "request": {"search_string": search_string},
            "response": {
                "success": result.success,
                "error": result.error,
                "duration": result.duration,
                "collect_time": result.collect_time,
                "total": result.total
            }
        }
        await self.broker.publish_to_rmq("pb.nalog.search.status", rmq_status)

        for pair in result.entities:
            kafka_msg = {
                "request": {"search_string": search_string},
                "response": {
                    "success": result.success,
                    "error": result.error,
                    "duration": result.duration,
                    "total": result.total,
                    "collect_time": result.collect_time,
                    "person": {"name": pair.person.name, "inn": pair.person.inn},
                    "organization": {"name": pair.organization.name}
                }
            }
            await self.broker.publish_to_kafka("pb.nalog.search.response", kafka_msg)
