# ТЗ: Reddit-парсер для Exorde (комментарии, без аккаунтов)

Язык кода — любой (рекомендую Python 3.12 + asyncio + curl_cffi, т.к. уже проверено). Всё в Docker. Результат — репозиторий, пушится целиком; в чат файлы не вставлять.

## 1. Цель
Собирать **свежие комментарии Reddit** и слать батчами в локальный collector — туда же и в том же формате, что парсер Threads.
Цель по скорости: **≥10 NEW/сек** (хорошо 15–25) уникальных, ещё не известных Exorde комментариев.

## 2. Что уже проверено (не перепроверять, использовать)
- Работает ТОЛЬКО с домашнего IP (резидентный, **только IPv4**). VPS и бесплатные vless-узлы Reddit блокирует (403/429/timeout).
- HTTP-клиент: `curl_cffi` с `impersonate="chrome"` (обычный requests/httpx/aiohttp блокируется по TLS). Принудительно IPv4 (`CurlOpt.IPRESOLVE=1`), иначе таймауты (у дома нет IPv6).
- JSON и RSS — мертвы. `old.reddit.com` пустой. `/r/all/comments/` → 403.
- Рабочий источник: `https://www.reddit.com/r/popular/comments/?limit=100` — HTML с разметкой old-style, до 100 комментариев на страницу, ≈100 комментов/с в потоке, ≈45/с уникальных при ~0.8 req/с, ~8% повторов. Без `limit=100` — 25 на страницу и часто пусто.
- Дополнительно: отдельные «горячие» сабреддиты `https://www.reddit.com/r/<sub>/comments/?limit=100` (список в конфиге). Мультиреддиты `a+b+c` — залежавшийся кэш, не использовать.
- Бывают пустые «мягко затроттленные» страницы (~36 КБ, без комментов), 29–50% — нужна пауза/повтор.
- Разметка комментария: `<div class=" thing id-t1_… comment " data-fullname="t1_…" data-type="comment" data-subreddit=… data-author=… data-permalink=…>`, время `<time datetime="ISO">`, текст `<div class="md">`.
- Свежие комментарии на 100% новые для Exorde (проверено через url-check).

## 3. Формат выходного item (как у Threads, НЕ как в гайде Grok)
Collector принимает `POST {"items":[ {...}, ... ]}` на `STORE_ENDPOINT` (по умолчанию `http://127.0.0.1:9000/store_items`; можно несколько через запятую — round-robin). Ответ 2xx = батч принят (collector может вернуть счётчики `{"count":N,"results":{"to_gpu":..,"dropped":..}}` — логировать). Анализ (classification/embedding и т.п.) делает pipeline, парсер его НЕ шлёт и не подделывает. Напрямую на upload.exorde.network — запрещено.

Поля item (ключ `content`, не `raw_content`):

| Поле | Значение |
|---|---|
| `content` | Текст комментария, plain text без HTML, strip по краям. Внешние ссылки, которых нет в тексте, дописывать ` --<url>`. Медиа-ссылки (i.redd.it и т.п.) — в конец блоком `‹‹media›› url ‹‹/media››`. |
| `external_id` | `t1_<base36>` из `data-fullname` |
| `created_at` | реальное время из `<time datetime>`, UTC, формат `YYYY-MM-DDTHH:MM:SSZ` |
| `collected_at` | момент сбора, UTC, тот же формат с `Z` |
| `url` | `https://www.reddit.com` + `data-permalink` (реальный, уникальный) |
| `title` | `""` (у комментария своего заголовка нет; чужой не подставлять) |
| `author` | `sha1(username).hexdigest()` (40 hex); если автор `[deleted]` — комментарий пропустить |
| `username` | ник в открытом виде |
| `external_parent_id` | см. п.4 |
| `domain` | всегда `reddit.com` |
| `summary` | **JSON-СТРОКА** (`json.dumps`), не объект: `{"platform":"reddit","communityName":"<sub>","data_type":"comment","score":<int>}`. `score` — реальный: в блоке три `span.score` (dislikes/unvoted/likes), берётся `title` у `span.score.unvoted`. НЕ выдумывать `upvote_ratio`/`num_comments` для комментариев. |
| `userprofile_url` | `https://www.reddit.com/user/<username>` (опционально) |

Запрещены: фейковые id/время/url/parent. Self-ref parent на Reddit запрещён.

## 4. Parent (решение ждём от Exorde — делаем переключаемым)
Правило Exorde: reply → `external_parent_id` = id родителя (`t1_…` комментарий / `t3_…` пост); top-level комментарий → `t3_<id поста>`; пост → null.
**Проверено на реальном HTML (329 комментариев):** лента `/comments/` НЕ отдаёт parent-комментарий (нет `data-parent*`, нет кнопки parent; `context=3` у всех; блок `p.parent` ведёт на пост). Страницы отдельных комментариев с дома не открываются (8 КБ заглушка). `t3_<post_id>` всегда берётся из permalink. Отличить reply от top-level по ленте нельзя.
Поэтому в `mapper.py` режим `PARENT_MODE` (env, по умолчанию `null`):
- `null` — `external_parent_id = null` для всех комментариев (честно, ничего не выдумываем);
- `post` — `t3_<post_id>` для всех (включать ТОЛЬКО если Exorde подтвердит, что это допустимо);
- `resolve` — задел на будущее (добор родителя), в v1 не реализовывать.
Режим логируется при старте. Менять режим = смена env + рестарт, без правок кода.

## 4a. Язык
Парсер язык НЕ трогает: ни `language`, ни `translated_content`, ни фильтр по языку. Язык и перевод определяет pipeline владельца. Опцию `ENGLISH_ONLY` не делать.

## 5. Фильтры
- Минимальная длина текста 7 символов (без URL и эмодзи); только-эмодзи — отбрасывать.
- Пропускать `[deleted]`/`[removed]`, комментарии ботов-модераторов (AutoModerator), пустые тела.
- Дедуп: `SeenCache` (LRU/TTL по `external_id`), claim до отправки, при неудачной отправке claim освобождается.

## 6. url-check (дедуп с Exorde)
`POST https://url-check.exorde.network/v1/check` body `{"urls":[...]}` → список уже известных URL. Партиями (≤100–200) одним sender-таском. С дома напрямую недоступен → ходит через SOCKS (`URLCHECK_PROXY`, напр. `socks5h://127.0.0.1:10809`). Известные URL выбрасываются. Политика при недоступности сервиса: настраиваемая `URLCHECK_POLICY=open|closed` (по умолчанию `closed`, как в Threads: не слать непроверенное, ретрай).

## 7. Планировщик запросов
- **Один запрос за раз** (без параллелизма на одну цель), целевой интервал ~1 req/с (`REQ_INTERVAL`), jitter ±20%.
- Цели: `popular` (основная) + список `SUBS` в round-robin с меньшим весом.
- Пустая/мягко-затроттленная страница → пауза 2–5 с и повтор; 403/429 → backoff 60 с (экспоненциально до 5 мин), счётчик в метриках; смена Chrome-профиля impersonate при серии пустых.
- Таймауты соединения 10 с, чтения 15 с. Всё через IPv4.
- Опциональный адаптивный режим: автоподстройка интервала по доле пустых/429 (hill-climb), границы в конфиге.

## 8. Отправка
- Батч 30–100 (`BATCH_MIN`, `BATCH_MAX`, `FLUSH_SECONDS` — слать неполный батч, если >N с без роста).
- POST `{"items":[...]}` через httpx/aiohttp, retry с backoff, round-robin по нескольким `STORE_ENDPOINT`, `STORE_CONCURRENCY` по умолчанию 4.
- Окончательный отказ → claims в SeenCache освобождаются, элементы возвращаются в очередь (с ограничением размера) либо пишутся в `data/spool/` для повторной отправки (circuit breaker, если collector лежит).

## 9. Метрики и логи (INFO раз в 30–60 с, одной строкой)
страниц/с, комментов в страницах/с, уникальных/с, пустых страниц %, 403/429 за интервал, отброшено фильтрами (по причинам), известных url-check %, отправлено/с, ошибки collector, размер очереди. Предупреждения троттлить (одно в N секунд на шаблон).

## 10. Конфигурация
`.env.example` + `config.yaml` с плейсхолдерами; никаких секретов в git (SOCKS-адрес — только env). Ключи: `STORE_ENDPOINT`, `URLCHECK_PROXY`, `URLCHECK_POLICY`, `SUBS`, `REQ_INTERVAL`, `BATCH_MIN/MAX`, `FLUSH_SECONDS`, `MIN_TEXT_LEN`, `PARENT_MODE`, `LOG_LEVEL`, `SEEN_MAX`.

## 11. Docker
- `Dockerfile` (slim, непривилегированный пользователь) + `docker-compose.yml`: `network_mode: host` (нужен доступ к `127.0.0.1:9000` collector и к SOCKS на хосте, IPv4 дома), `restart: unless-stopped`, volume `./data:/app/data`, `env_file: .env`, healthcheck (свежий heartbeat-файл).
- Код запекается в образ → в README: `docker compose up -d --build`.
- Конфиг монтировать **директорией**, не одиночным файлом (одиночный bind-mount залипает при смене inode).
- Запуск только на домашней машине (резидентный IPv4).

## 12. Структура: один файл — один модуль (Python-вариант)
```
reddit_parser/
  README.md                 # запуск, этап 0, чеклист проверки
  Dockerfile
  docker-compose.yml
  .env.example
  .gitignore                # .env, data/, vless*
  config.yaml
  requirements.txt
  reddit/
    __init__.py
    config.py               # загрузка env+yaml, dataclass Settings
    http_client.py          # curl_cffi сессия, impersonate, IPv4, fetch(url)->(status,text)
    parser.py               # HTML -> list[RawComment] (регулярки по div.thing, time, div.md)
    models.py               # RawComment dataclass
    mapper.py               # RawComment -> item dict (формат из п.3), SHA1, summary-строка, ссылки/медиа
    filters.py              # min length, emoji-only, deleted/bots
    seen_cache.py           # SeenCache (claim/confirm/release, TTL, LRU)
    url_check.py            # батчи в url-check через SOCKS, политика open/closed
    sender.py               # батчинг 30–100, POST {"items":..}, retry, spool, round-robin
    scheduler.py            # цели, интервал, backoff 403/429, пустые страницы, адаптация
    metrics.py              # счётчики + периодическая строка лога
    main.py                 # сборка пайплайна, asyncio tasks, graceful shutdown
  scripts/
    dump_samples.py         # сохранить сырой HTML в data/samples
    check_item.py           # валидатор item по правилам п.3 (regex дат, 40 hex, summary-строка...)
  tests/
    test_parser.py          # на сохранённых samples
    test_mapper.py
    test_filters.py
    test_seen_cache.py
```
(Если выбран другой язык — тот же принцип: один модуль = одна ответственность.)

## 13. Порядок работ
1. Каркас репо + Docker + `dump_samples.py` (образцы HTML уже есть у владельца: scripts/_samples из репо threads_parser, при необходимости пришлёт).
2. parser + mapper (PARENT_MODE) + filters + тесты на samples + `check_item.py`.
3. seen_cache + url_check + sender + scheduler + metrics + main.
4. Прогон дома: цель ≥10 NEW/с через url-check, доля 403/429 и пустых страниц в логе.

## 14. Критерии приёмки
- Все item проходят `check_item.py` (domain `reddit.com`, даты `...Z`, author 40 hex, `external_id` `t1_*`, summary — строка, нет фейков).
- Нет секретов в git (`git grep` по UUID/паролям пуст).
- ≥10 NEW/с на 10-минутном прогоне; collector отвечает 2xx; нет роста очереди.
- Падение collector/SOCKS не роняет процесс; после восстановления отправка продолжается.
- 403/429 → пауза без шторма запросов.

## 15. Входные данные от владельца
Домашняя машина с Docker; collector на `127.0.0.1:9000`; SOCKS для url-check на `127.0.0.1:10809` (адрес vless в git не класть). Ответ Exorde по parent (п.4) — до его получения работает `PARENT_MODE=null`.
