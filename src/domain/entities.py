from dataclasses import dataclass
from typing import Optional

@dataclass
class Person:
    name: str
    inn: str
    token: str = ""
    ul_cnt: int = 0

@dataclass
class Organization:
    name: str
    inn: str = ""
    ogrn: str = ""
    dtogrn: str = ""
    regionname: str = ""
    okved2main: str = ""
    okved2mainname: str = ""
    sulst_ex: str = ""
    sulst_name_ex: str = ""

@dataclass
class PersonOrgPair:
    person: Person
    organization: Organization

@dataclass
class SearchResult:
    success: bool
    error: Optional[str]
    duration: float
    collect_time: str
    total: int
    entities: list[PersonOrgPair]
