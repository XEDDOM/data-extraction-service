import aiohttp
import asyncio
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.domain.entities import Person, Organization
from src.domain.ports import ScraperPort


class NalogApiError(Exception):
    pass


class CaptchaRequiredError(Exception):
    """Исключение для случаев, когда требуется капча."""
    pass


class NalogScraperAdapter(ScraperPort):
    BASE_URL = "https://pb.nalog.ru"
    SEARCH_URL = f"{BASE_URL}/search-proc.json"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:152.0) Gecko/20100101 Firefox/152.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/search.html",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    def __init__(self, proxy: Optional[str] = None, timeout: float = 30.0):
        self.proxy = proxy
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._jar = aiohttp.CookieJar()
        self._request_count = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                cookie_jar=self._jar,
                headers=self.HEADERS,
                timeout=self.timeout,
            )
            async with self._session.get(self.BASE_URL, proxy=self.proxy) as resp:
                await resp.read()
                print(f"🍪 Session initialized")
        return self._session

    async def _check_captcha(self, response_data: dict) -> None:
        """Проверяет, требуется ли капча."""
        if "ERRORS" in response_data:
            errors = response_data.get("ERRORS", {})
            if "pbSearchCaptcha" in errors:
                raise CaptchaRequiredError(
                    f"Требуется капча: {errors['pbSearchCaptcha']}"
                )

    async def _start_search(self, payload: dict) -> str:
        """Запускает поиск и возвращает ID сессии."""
        self._request_count += 1
        if self._request_count > 1:
            delay = 1.5
            print(f"⏸️  Waiting {delay}s before request #{self._request_count}")
            await asyncio.sleep(delay)
        
        print(f"\n🚀 Starting search")
        print(f"📤 Payload (first 5 params): {dict(list(payload.items())[:5])}")
        
        session = await self._get_session()
        
        from aiohttp import FormData
        form_data = FormData(payload)
        
        async with session.post(
            self.SEARCH_URL, 
            data=form_data,
            proxy=self.proxy
        ) as resp:
            print(f"📡 Start search response status: {resp.status}")
            
            response_text = await resp.text()
            
            if resp.status == 400:
                try:
                    data = await resp.json(content_type=None)
                    await self._check_captcha(data)
                    print(f"📦 Response body: {response_text[:300]}")
                    raise NalogApiError(f"HTTP {resp.status}: {response_text[:200]}")
                except ValueError:
                    raise NalogApiError(f"HTTP {resp.status}: {response_text[:200]}")
            
            if resp.status != 200:
                raise NalogApiError(f"HTTP {resp.status}")
            
            data = await resp.json(content_type=None)
            await self._check_captcha(data)
            
            print(f"📦 Start search response: {data}")
            
            search_id = data.get("id")
            if not search_id:
                raise NalogApiError(f"No search id in response: {data}")
            
            print(f"✅ Got search ID: {search_id}")
            return search_id

    async def _poll_result(self, search_id: str) -> dict:
        """Опрашивает результат поиска до готовности."""
        print(f"🔄 Polling result for ID: {search_id}")
        session = await self._get_session()
        
        payload = {
            "id": search_id,
            "method": "get-response",
        }
        
        async with session.post(
            self.SEARCH_URL, data=payload, proxy=self.proxy
        ) as resp:
            print(f"📡 Poll response status: {resp.status}")
            if resp.status != 200:
                response_text = await resp.text()
                raise NalogApiError(f"HTTP {resp.status}: {response_text[:200]}")
            
            data = await resp.json(content_type=None)
            print(f"📦 Poll response: {data}")
            
            if not data:
                raise NalogApiError("Result not ready yet")
            
            await self._check_captcha(data)
            
            print(f"📦 Poll response keys: {data.keys() if data else 'None'}")
            
            return data

    async def _search_with_polling(self, payload: dict) -> dict:
        """Полный цикл: старт поиска + polling до результата."""
        search_id = await self._start_search(payload)
        
        for attempt in range(20):
            try:
                result = await self._poll_result(search_id)
                return result
            except CaptchaRequiredError:
                raise
            except NalogApiError as e:
                if "not ready" in str(e):
                    timeout = min((attempt + 1) * 1.5, 10.0)
                    print(f"⏳ Waiting {timeout}s before next poll (attempt {attempt + 1}/20)")
                    await asyncio.sleep(timeout)
                    continue
                raise
        
        raise NalogApiError("Search timeout after 20 attempts")

    async def search_directors(self, query: str, limit: int = 100) -> list[Person]:
        """Поиск руководителей по ФИО или ИНН."""
        print(f"\n{'='*60}")
        print(f"🔍 search_directors called with query: {query}")
        print(f"{'='*60}")
        
        payload = {
            "mode": "search-upr-uchr",
            "queryAll": "",
            "queryUl": "",
            "okvedUl": "",
            "okvedTypeUl": "",
            "regionUl": "",
            "statusUl": "",
            "isMspUl": "",
            "mspUl1": "1",
            "mspUl2": "1",
            "mspUl3": "1",
            "queryIp": "",
            "okvedIp": "",
            "okvedTypeIp": "",
            "regionIp": "",
            "statusIp": "",
            "isMspIp": "",
            "mspIp1": "1",
            "mspIp2": "1",
            "mspIp3": "1",
            "taxIp": "",
            "queryUpr": query,
            "uprType1": "1",
            "queryRdl": "",
            "dateRdl": "",
            "queryAddr": "",
            "regionAddr": "",
            "queryOgr": "",
            "ogrFl": "1",
            "ogrUl": "1",
            "ogrnUlDoc": "",
            "ogrnIpDoc": "",
            "npTypeDoc": "1",
            "nameUlDoc": "",
            "nameIpDoc": "",
            "formUlDoc": "",
            "formIpDoc": "",
            "ifnsDoc": "",
            "dateFromDoc": "",
            "dateToDoc": "",
            "page": "1",
            "pageSize": str(limit),
            "pbCaptchaToken": "",
            "token": "",
        }

        try:
            result = await self._search_with_polling(payload)
            print(f"✅ Search completed, result keys: {result.keys() if result else 'None'}")
        except CaptchaRequiredError as e:
            print(f"🚫 Captcha required: {e}")
            raise NalogApiError(str(e))
        except Exception as e:
            print(f"❌ Search failed: {e}")
            raise
        
        upr_data = result.get("upr", {})
        items = upr_data.get("data", [])
        print(f"👥 Found {len(items)} directors in response")
        
        persons = []
        for item in items[:limit]:
            persons.append(Person(
                name=item.get("name", ""),
                inn=item.get("inn", ""),
                token=item.get("token", ""),
                ul_cnt=item.get("ul_cnt", 0),
            ))
        
        print(f"✅ Returning {len(persons)} persons")
        return persons

    async def get_person_organizations(self, person: Person, limit: int = 100) -> list[Organization]:
        """Получение списка организаций руководителя."""
        if not person.token:
            print(f"⚠️ No token for person {person.name}, skipping")
            return []
        
        print(f"\n{'='*60}")
        print(f"🏢 get_person_organizations for: {person.name}")
        print(f"{'='*60}")
        
        payload = {
            "mode": "search-ul",
            "queryAll": "",
            "queryUl": person.name,
            "okvedUl": "",
            "okvedTypeUl": "",
            "regionUl": "",
            "statusUl": "",
            "isMspUl": "",
            "mspUl1": "1",
            "mspUl2": "1",
            "mspUl3": "1",
            "queryIp": "",
            "okvedIp": "",
            "okvedTypeIp": "",
            "regionIp": "",
            "statusIp": "",
            "isMspIp": "",
            "mspIp1": "1",
            "mspIp2": "1",
            "mspIp3": "1",
            "taxIp": "",
            "queryUpr": "",
            "uprType1": "",
            "queryRdl": "",
            "dateRdl": "",
            "queryAddr": "",
            "regionAddr": "",
            "queryOgr": "",
            "ogrFl": "1",
            "ogrUl": "1",
            "ogrnUlDoc": "",
            "ogrnIpDoc": "",
            "npTypeDoc": "1",
            "nameUlDoc": "",
            "nameIpDoc": "",
            "formUlDoc": "",
            "formIpDoc": "",
            "ifnsDoc": "",
            "dateFromDoc": "",
            "dateToDoc": "",
            "page": "1",
            "pageSize": str(limit),
            "pbCaptchaToken": "",
            "token": person.token,
        }

        try:
            result = await self._search_with_polling(payload)
            print(f"✅ Org search completed, result keys: {result.keys() if result else 'None'}")
        except CaptchaRequiredError as e:
            print(f"🚫 Captcha required during org search: {e}")
            return []
        except Exception as e:
            print(f"❌ Org search failed: {e}")
            raise
        
        ul_data = result.get("ul", {})
        items = ul_data.get("data", [])
        print(f"🏢 Found {len(items)} organizations in response")
        
        organizations = []
        for item in items[:limit]:
            organizations.append(Organization(
                name=item.get("namec") or item.get("namep") or "",
                inn=item.get("inn", ""),
                ogrn=item.get("ogrn", ""),
                dtogrn=item.get("dtogrn", ""),
                regionname=item.get("regionname", ""),
                okved2main=item.get("okved2main", ""),
                okved2mainname=item.get("okved2mainname", ""),
                sulst_ex=item.get("sulst_ex", ""),
                sulst_name_ex=item.get("sulst_name_ex", ""),
            ))
        
        print(f"✅ Returning {len(organizations)} organizations")
        return organizations

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
