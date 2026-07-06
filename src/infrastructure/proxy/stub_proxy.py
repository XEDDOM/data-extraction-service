from typing import Optional
from src.domain.ports import ProxyPort


class StubProxyProvider(ProxyPort):
    def __init__(self, proxy_url: Optional[str] = None):
        self.proxy_url = proxy_url
    
    async def get_proxy(self) -> Optional[str]:
        return self.proxy_url


class RotatingProxyProvider(ProxyPort):
    def __init__(self, proxy_list: list[str]):
        self.proxy_list = proxy_list
        self.current_index = 0
    
    async def get_proxy(self) -> Optional[str]:
        if not self.proxy_list:
            return None
        
        proxy = self.proxy_list[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxy_list)
        
        return proxy
