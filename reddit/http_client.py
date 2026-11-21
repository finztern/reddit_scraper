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


class NodeClient:
    """One long-lived session through a SOCKS node (cookies persist). Recreated only by reset()."""

    def __init__(self, port: int | str, profiles: list[str], connect_timeout: float = 10.0, read_timeout: float = 15.0) -> None:
        self.port = port
        self._profiles = list(profiles) or ["chrome"]
        self._idx = 0
        self._timeout = (connect_timeout, read_timeout)
        self.resets = 0
        self.last_headers: dict[str, str] = {}
        self._s = self._new()

    def _new(self) -> AsyncSession:
        url = self.port if isinstance(self.port, str) else f"socks5h://127.0.0.1:{self.port}"  # str = full proxy URL
        return AsyncSession(proxies={"http": url, "https": url}, curl_options={CurlOpt.IPRESOLVE: 1})

    @property
    def profile(self) -> str:
        return self._profiles[self._idx]

    def set_profile(self, name: str) -> None:
        if name in self._profiles:
            self._idx = self._profiles.index(name)

    def rotate_profile(self) -> str:
        self._idx = (self._idx + 1) % len(self._profiles)
        return self.profile

    async def fetch(self, url: str) -> tuple[int, str, int]:
        r = await self._s.get(url, impersonate=self.profile, timeout=self._timeout,
                              headers={"Accept-Language": "en-US,en;q=0.9"})
        self.last_headers = {k.lower(): v for k, v in r.headers.items()}
        return r.status_code, r.text, len(r.content)

    async def reset(self) -> None:
        """Drop the session (cookies too). Call only after repeated network errors / quarantine."""
        old, self._s = self._s, self._new()
        self.resets += 1
        try:
            await old.close()
        except Exception:  # noqa: BLE001
            pass

    async def close(self) -> None:
        try:
            await self._s.close()
        except Exception:  # noqa: BLE001
            pass
