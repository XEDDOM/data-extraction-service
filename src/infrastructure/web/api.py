from fastapi import APIRouter
from src.application.use_cases import SearchDirectorUseCase

router = APIRouter()

use_case: SearchDirectorUseCase = None
kafka_use_case: SearchDirectorUseCase = None

@router.get("/api/v1/search")
async def search_directors(search_string: str):
    if not use_case:
        return {"error": "Service not initialized"}
        
    result = await use_case.execute(search_string, is_async_mode=False)
    
    return {
        "request": {"search_string": search_string},
        "response": {
            "success": result.success,
            "error": result.error,
            "duration": result.duration,
            "collect_time": result.collect_time,
            "total": result.total,
            "entities": [
                {
                    "person": {"name": p.person.name, "inn": p.person.inn},
                    "organization": {"name": p.organization.name}
                } for p in result.entities
            ]
        }
    }
