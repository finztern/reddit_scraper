FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /hub
RUN pip install --no-cache-dir "httpx[socks]>=0.27"
ARG SINGBOX_VERSION=1.11.15
RUN python - <<'PY'
import io, os, platform, tarfile, urllib.request
v = os.environ.get("SINGBOX_VERSION", "1.11.15")
arch = {"x86_64": "amd64", "aarch64": "arm64"}[platform.machine()]
url = f"https://github.com/SagerNet/sing-box/releases/download/v{v}/sing-box-{v}-linux-{arch}.tar.gz"
t = tarfile.open(fileobj=io.BytesIO(urllib.request.urlopen(url, timeout=180).read()))
m = next(x for x in t.getmembers() if x.name.endswith("/sing-box"))
open("/usr/local/bin/sing-box", "wb").write(t.extractfile(m).read())
PY
RUN chmod 755 /usr/local/bin/sing-box && sing-box version | head -1
RUN useradd -u 1000 -m app && mkdir -p /hub/data /hub/reddit && chown -R app:app /hub
COPY --chown=app:app reddit/__init__.py reddit/vpn.py /hub/reddit/
COPY --chown=app:app hub/hub.py /hub/hub.py
USER app
CMD ["python", "hub.py"]
