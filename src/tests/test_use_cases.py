import pytest
from unittest.mock import AsyncMock
from src.application.use_cases import SearchDirectorUseCase
from src.domain.entities import Person, Organization

@pytest.mark.asyncio
async def test_search_director_use_case():
    mock_scraper = AsyncMock()
    mock_scraper.search_directors.return_value = [
        Person(name="ИВАНОВ И.И.", inn="123")
    ]
    mock_scraper.get_person_organizations.return_value = [
        Organization(name="ООО ТЕСТ")
    ]
    
    mock_proxy = AsyncMock()
    mock_proxy.get_proxy.return_value = None
    
    mock_broker = AsyncMock()
    
    use_case = SearchDirectorUseCase(mock_scraper, mock_proxy, mock_broker)
    
    result = await use_case.execute("Иванов", is_async_mode=False)
    
    assert result.success is True
    assert result.total == 1
    assert result.entities[0].person.name == "ИВАНОВ И.И."
    assert result.entities[0].organization.name == "ООО ТЕСТ"
    mock_broker.publish_to_kafka.assert_not_called()
