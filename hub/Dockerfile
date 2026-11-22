FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /hub
RUN pip install --no-cache-dir "httpx[socks]>=0.27"
RUN python - <<'PY'
import io, platform, urllib.request, zipfile
arch = {"x86_64": "64", "aarch64": "arm64-v8a"}[platform.machine()]
url = f"https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-{arch}.zip"
z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(url, timeout=120).read()))
open("/usr/local/bin/xray", "wb").write(z.read("xray"))
PY
RUN chmod 755 /usr/local/bin/xray && xray version | head -1
RUN useradd -u 1000 -m app && mkdir -p /hub/data /hub/reddit && chown -R app:app /hub
COPY --chown=app:app reddit/__init__.py reddit/vpn.py /hub/reddit/
COPY --chown=app:app hub/hub.py /hub/hub.py
USER app
CMD ["python", "hub.py"]
