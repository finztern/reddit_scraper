"""curl_cffi session: Chrome TLS impersonation, IPv4 only."""
from __future__ import annotations

from curl_cffi import CurlOpt
from curl_cffi.requests import AsyncSession

PROFILES = ["chrome", "chrome131", "chrome124", "chrome120"]


class HttpClient:
    def __init__(self, connect_timeout: float = 10.0, read_timeout: float = 15.0) -> None:
        self._timeout = (connect_timeout, read_timeout)
        self._idx = 0
        self._s = AsyncSession(curl_options={CurlOpt.IPRESOLVE: 1})

    @property
    def profile(self) -> str:
        return PROFILES[self._idx]

    def rotate_profile(self) -> str:
        self._idx = (self._idx + 1) % len(PROFILES)
        return self.profile

    async def fetch(self, url: str) -> tuple[int, str]:
        r = await self._s.get(
            url,
            impersonate=self.profile,
            timeout=self._timeout,
            headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        return r.status_code, r.text

    async def close(self) -> None:
        await self._s.close()
