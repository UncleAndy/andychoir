# План доработки `andychoir` (v0.1.0)

> Источник: аудит корректности методов/алгоритмов и безопасности.
> Охват: два раздела — «Алгоритмические/логические дефекты» и «Безопасность (по приоритету)».
> Статус базы: `cargo test` — 112 passed, 0 failed ( host-логика и сетевые алгоритмы покрыты юнит-тестами).
> Правило: `git add`/`git commit` — только по явной команде. Этот файл не коммитится автоматически.

## Легенда приоритетов

| Приоритет | Значение | SLA-ориентир |
|-----------|----------|--------------|
| 🔴 HIGH   | Эксплуатация границы доверия (RCE/эксфильтрация/впрыск в mesh) | до релиза в публичной сети |
| 🟠 MEDIUM | Требует доверенного окружения; утечка/обход защиты при недоверенных плагинах или локальных клиентах | ближайшая итерация |
| 🟡 LOW    | Качество/robustness/документация; не влияет на эксплуатацию при текущей модели доверия | бэклог |

## Сводная таблица задач

| ID  | Область | Заголовок | Приоритет | Затронутые файлы |
|-----|--------|-----------|-----------|------------------|
| A1  | Алгоритм | `node_url` для входящих соединений = `"incoming"` ломает FIB-маршрутизацию к входящему соседу | 🟡 LOW* | `src/host/net/server.rs`, `src/host/net/forward.rs`, `src/host/net/net.rs` |
| A2  | Алгоритм | Утечка `PENDING_RESPONSES` / `RESPONSE_PAYLOADS` по `request_id` | 🟡 LOW | `src/plugin/engine.rs` |
| A3  | Алгоритм | `history_append` держит write-lock на весь push (узкое место) | 🟡 LOW | `src/plugin/engine.rs` |
| A4  | Алгоритм | Док-баг в WIT: `request-id`/`session-id` перепутаны в комментариях | 🟡 LOW | `wit/plugin.wit` |
| B1  | Безопасность | Сетевая аутентификация отсутствует: `token` не проверяется | 🔴 HIGH | `src/host/net/server.rs`, `src/host/net/outbound.rs`, `src/config/config.rs` |
| B2  | Безопасность | `PluginAccess::Network` не проверяется в `post_json` | 🔴 HIGH | `src/plugin/engine.rs`, `src/plugin/config.rs`, `wit/plugin.wit` |
| B3  | Безопасность | MCP-транспорт = произвольный spawn процесса хоста | 🟠 MEDIUM | `src/host/mcp_transport.rs` |
| B4  | Безопасность | HTTP/WS-фронты без auth + обход `session_local` | 🟠 MEDIUM | `src/host/http_server.rs`, `src/host/ws_server.rs`, `src/plugin/engine.rs` |
| B5  | Безопасность | Утечка секретов в лог (`config` плагина) | 🟠 MEDIUM | `src/plugin/engine.rs` |
| B6  | Безопасность | `PluginAccess::Filesystem` объявлено, но не реализовано (мёртвое право) | 🟡 LOW | `src/plugin/config.rs`, `src/plugin/engine.rs` |
| B7  | Безопасность | Metrics-экспортер без auth на `0.0.0.0` | 🟡 LOW | `src/metrics.rs` |
| B8  | Безопасность | `build_http_response`: `unwrap` на невалидном `status` из payload | 🟡 LOW | `src/host/http_server.rs` |
| B9  | Безопасность | `std::process::exit(1)` внутри tokio-задачи при стартовом таймауте | 🟡 LOW | `src/main.rs` |

> \*A1 технически не security, но ломает корректность mesh для асимметричных связей — выделен отдельно по просьбе.

---

# Часть A. Алгоритмические / логические дефекты

## A1. `node_url` для входящих соединений = `"incoming"` ломает FIB к входящему соседу

**Проблема.**
В `src/host/net/server.rs:73` при регистрации узла, подключившегося *входящим* WS-соединением, пишется:
```rust
inner.node_url.write().await.insert(origin.clone(), "incoming".to_string());
```
В `src/host/net/forward.rs` (ветка P5.1) маршрутизация по FIB ищет:
```rust
let node_url_map = inner.node_url.read().await;
if let Some(url) = node_url_map.get(&next_hop) {       // next_hop = "incoming"
    let outbound = inner.outbound.read().await;
    if let Some(tx) = outbound.get(url) { ... }          // outbound ключ — реальный ws://…, не "incoming"
}
```
Следствие: для узла, видимого **только** как входящее соединение (асимметричная связь: A dial-out → B, но C dial-out → A, т.е. A знает C только входящим), маршрут FIB к C строится (`rebuild_fib` корректен), но отправка по нему невозможна — `outbound.get("incoming")` пуст. Событие уйдёт только если сработает ветка `request_origin` (backward-compat), т.е. только как ответ на ранее полученный запрос. Произвольный форвард к такому соседу (через mesh) не работает.

**Где маскируется тестами.** Тесты `p5_forward_routes_via_fib`, `p7_end_to_end_mesh_routing` и др. сами прописывают `node_url = реальный url`, поэтому баг не ловится.

**Решение (аддитивно, без поломки текущей логики).**
1. Завести отдельную карту обратных путей для *входящих* соседей, либо хранить в `node_url` реальный return-path.
2. Минимальный вариант: в `server.rs` при `handle_incoming` регистрировать `node_url[origin] = "<incoming>:<peer-addr>"` (или добавить поле `incoming_return_path: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>`), и в `forward.rs` P5.1 проверять сначала `incoming_senders.get(&next_hop)` (как уже сделано в P5.2), затем `outbound`.
3. Либо (чистый вариант): `forward_inner` после `route_next_hop` пробует в порядке `incoming_senders → outbound`, единообразно для любого `next_hop`, не полагаясь на строку `"incoming"` в `node_url`.

**Критерии приёмки (тесты).**
- Новый юнит-тест `p5_forward_to_incoming_neighbor`: построить топологию, где `next_hop` известен **только** как входящее соединение (зарегистрирован через `incoming_senders`, `node_url` не содержит реального url), и проверить, что `forward_inner` доставляет событие в `incoming_senders[next_hop]`.
- Существующие тесты FIB остаются зелёными.

---

## A2. Утечка `PENDING_RESPONSES` / `RESPONSE_PAYLOADS` по `request_id`

**Проблема.**
- `register_wait_response` (`src/plugin/engine.rs:86`) вставляет entry в `PENDING_RESPONSES`; удаляется только в `signal_response` (при реальном ответе). При `wait_for_response_timeout` → таймаут → entry **остаётся** навечно.
- `store_response_payload` (`engine.rs:118`) пишет в `RESPONSE_PAYLOADS` и никогда не очищает.

Для долгоживущего хоста с высокой частотой запросов — монотонный рост двух HashMap (утечка памяти).

**Решение.**
1. В `wait_for_response_timeout` по истечении таймаута вызывать `signal_response`-подобную очистку (удалить entry из `PENDING_RESPONSES`, если он ещё там и не был сигнален).
2. Для `RESPONSE_PAYLOADS`: либо TTL-очистка (后台-задача по `Instant::now() - сохранения > N сек`), либо удаление payload сразу после `take_response_payload` (уже есть `remove` в `take_response_payload` — но payload, который никто не забрал, висит вечно). Добавить фоновую чистку по возрасту.
3. Альтернатива (чистая): ограничить размер через `DashMap::remove` по истечении окна, аналогично `dedup` (см. `src/host/net/dedup.rs`).

**Критерии приёмки (тесты).**
- Юнит-тест: после `wait_for_response_timeout` с истёкшим таймаутом и без ответа — `PENDING_RESPONSES` не содержит `request_id`.
- Интеграционный (по желанию): прогон N запросов с таймаутом → размер мап растёт не более чем на константу.

---

## A3. `history_append` держит write-lock на весь push

**Проблема.**
`src/plugin/engine.rs:305` `history_append`:
```rust
let mut map = session_histories().write().await;   // эксклюзивный lock
let entries = map.entry(...).or_default();
if entries.len() >= max_events { ... drain ... }
entries.push(ev.clone());
```
Весь push идёт под `write()` на глобальном `RwLock<HashMap<String, Vec<Event>>>`. Чтение истории агентом (`get_session_history` из WASM) блокируется. При высокой частоте событий — узкое место пропускной способности шины.

**Решение.**
- Вынесено как известное ограничение; для исправления — перейти на per-session структуры (`DashMap<String, SessionHistory>`) с локальным `Mutex` на `Vec<Event>` (аналогично `SESSION_TOOLS`/`LOCAL_TOOLS`), чтобы запись в одну сессию не блокировала чтение/запись других.
- Либо: снимать snapshot под read-lock, модифицировать локально, заменять под кратким write (как уже сделано в `save_session_to_disk`).

**Критерии приёмки.**
- Бенчмарк/тест: concurrent `history_append` для N сессий не блокирует `history_get` (достаточно проверки отсутствия deadlock + сохранения порядка внутри сессии).

---

## A4. Док-баг в WIT: `request-id`/`session-id` перепутаны в комментариях

**Проблема.** `wit/plugin.wit:5-6`:
```
request-id: string, // Сквозной ID пользовательской сессии   ← неверно
session-id: string, // Сквозной ID пользовательского запроса  ← неверно
```
По коду (`src/messages/bus.rs`, `src/host/*`): `request_id` — ID конкретного запроса/ответа, `session_id` — ID сквозной сессии диалога.

**Решение.** Исправить комментарии на корректные (без изменения WIT-схемы — обратная совместимость сохранена). При желании — расширить WIT-комментарии словарём топиков (уже есть в `types`).

**Критерии приёмки.** Док-ривью; `cargo build` без изменений семантики.

---

# Часть B. Безопасность (по приоритету)

## B1. 🔴 Сетевая аутентификация отсутствует: `token` не проверяется

**Проблема.**
- `src/config/config.rs` определяет `NetConfig.token: Vec<String>` (токены для входящих) и `NetRemote.token: String` (токен на удалённый хост).
- `src/host/net/server.rs` (`run_incoming_server`, `handle_incoming`) принимает любое WS-подключение к `0.0.0.0:listen_port/net` и сразу обрабатывает `Capabilities`/`Hello`/`Event`. **Ни одной проверки токена нет.**
- `src/host/net/outbound.rs` (`run_outbound_loop`) шлёт `NetRemote.token` удалённому хосту, но принимающая сторона его не валидирует.

Следствие (RCE-класс через mesh): любой, кто может достучаться до `listen_port`, может
(а) впрыснуть произвольные `Event` в шину хоста (вызвать любые локальные инструменты/агентов);
(б) анонсировать фейковые `Capabilities` — внедрить вредоносные инструменты в mesh (перехват вызовов через `resolve_tool_target` Tier 3, `orchestrator.rs`);
(в) читать ответы/сессии других узлов.

Поле `token` создаёт ложное чувство защищённости.

**Решение (fail-closed, аддитивно к handshake).**
1. Добавить в `NetMessage` тип `Auth { token: String }` (или передавать токен первым сообщением / query-параметром `?token=`).
2. В `handle_incoming` (`server.rs`): до обработки `Capabilities`/`Hello`/`Event` требовать `Auth`; сверять токен со `inner.cfg.token` (любое совпадение из списка). При несовпадении — `ws_sink.close()` и `return`.
3. В `run_outbound_loop` (`outbound.rs`): отправлять `Auth` первым сообщением при установлении соединения (использовать уже имеющийся `remote.token`).
4. Опционально: подписывать `Hello`/`Event` (HMAC от `node_id`+сообщение+ключ), чтобы исключить spoofing `origin`.

**Критерии приёмки (тесты).**
- Юнит/интеграционный: соединение без/с неверным токеном → отклоняется, `incoming_senders` не пополняется, `origin_tools` не добавляются.
- Соединение с верным токеном → обмен `Capabilities`/`Hello` проходит (существующие тесты discovery остаются зелёными).
- Тест на стороне `outbound`: исходящее соединение шлёт `Auth` первым.

---

## B2. 🔴 `PluginAccess::Network` не проверяется в `post_json`

**Проблема.**
`src/plugin/engine.rs:756` (`HostWithStore::post_json`):
```rust
async fn post_json(_accessor, url, json_body) -> (u16, String) {
    // TODO(P1): здесь можно добавить проверку прав плагина на сетевой
    // доступ (PluginAccess::Network) — fail-closed, как для консоли.
    let client = reqwest::Client::builder().timeout(Duration::from_secs(60)).build()...;
    client.post(&url)...
}
```
Право `PluginAccess::Network(Vec<(String, u16)>)` объявлено в `src/plugin/config.rs:11` и в WIT (`plugin.wit:66-69`), но **никогда не читается** в `post_json`. Любой загруженный плагин имеет неограниченный исходящий сетевой доступ (SSRF/эксфильтрация/сканирование). Песочница WASM не помогает — запрос идёт из хоста.

**Решение (fail-closed, как для консоли).**
1. В `post_json` получить `current_plugin_permissions` (как уже делается в `print_line`/`read_line` через `accessor.with(...)`).
2. Проверить наличие `PluginAccess::Network(allowed)`; привести `url` к `(host, port)` и сверить со списком `allowed`. При отсутствии права / несовпадении — возвращать `(403, {"error":"network access denied"})` (fail-closed).
3. Вынести проверку в чистую функцию `can_plugin_network(perms, url) -> bool` и покрыть юнит-тестами (аналог `can_plugin_print`/`can_plugin_read_file`).

**Критерии приёмки (тесты).**
- `can_plugin_network` разрешает точное `(host, port)` совпадение, запрещает всё остальное при пустом списке, запрещает без права `Network`.
- Интеграционный: плагин без `Network` получает `(403, …)` на любой `post_json`.

---

## B3. 🟠 MCP-транспорт = произвольный spawn процесса хоста

**Проблема.**
`src/host/mcp_transport.rs:43` `stdio_open`:
```rust
let mut cmd = tokio::process::Command::new(command);
cmd.args(args).stdin(Stdio::piped())...;
```
Любая `command` + `args` + `env` от плагина → spawn бинарника хоста. Плагин с доступом к интерфейсу `mcp_transport` получает выполнение произвольного процесса (эскейп песочницы через легитимный интерфейс). Допустимо **только** при полностью доверенных плагинах; в mesh с чужими плагинами — критично.

**Решение.**
1. Белый список разрешённых команд/бинарей для `stdio_open` (конфиг `mcp.allowed_binaries`), fail-closed при несовпадении.
2. Либо явно задокументировать trust-модель: «плагин с `mcp_transport` = root-эквивалент хоста» (в `PLUGIN-API.md`/README).

**Критерии приёмки.**
- `stdio_open` с командой вне белого списка → возвращает `"-"` (отказ), не спавнит процесс.
- Док-ривью trust-модели.

---

## B4. 🟠 HTTP/WS-фронты без auth + обход `session_local`

**Проблема.**
- `src/host/http_server.rs:231` / `src/host/ws_server.rs:70` слушают `0.0.0.0` без аутентификации.
- Прямой HTTP-вызов к `front:http` вызывает `begin_frontend_request` (`http_server.rs:141`), который **регистрирует сессию как локальную и активную** → запрос получает доступ к `session_local`-инструментам (`src/messages/bus.rs:485` `should_deny_session_local` вернёт `false`, т.к. сессия локальная и активная).
- Защита `session_local` (предназначенная против *сетевых* чужих сессий) **не защищает** от прямого локального HTTP/WS-клиента. Если фронт слушает публично — любой вызывает приватные инструменты.

**Решение.**
1. По умолчанию bind фронтов на `127.0.0.1` (конфиг `http.bind`/`ws.bind`, default `127.0.0.1`).
2. Опционально: auth на фронтах (токен в заголовке/query, сверять с `cfg`).
3. Либо: `session_local` должен требовать доп. признак «доверенный фронт» (отдельный флаг в `begin_frontend_request`), чтобы произвольный HTTP-клиент не получал приватные инструменты.

**Критерии приёмки (тесты).**
- `start_http_servers`/`start_ws_servers` биндятся на `127.0.0.1` по умолчанию (тест на `SocketAddr`).
- Интеграционный: прямой HTTP-вызов к `session_local`-инструменту без доверенного признака → отказ (если выбран вариант 3).

---

## B5. 🟠 Утечка секретов в лог (`config` плагина)

**Проблема.**
`src/plugin/engine.rs:1257`:
```rust
info!("[Хост] Конфигурация плагина: {:?}", config_str);
```
Печатает **весь JSON-конфиг плагина** (может содержать API-ключи/токены) в файл лога (`flexi_logger`, `src/host/log.rs`). При этом `mcp_transport` корректно НЕ логирует env-значения (`mcp_transport.rs:42` «значения env НЕ логируются») — непоследовательно.

**Решение.**
1. Не логировать `config` целиком; логировать только «безопасные» поля (имя, список `access`, `session_local`) или маскировать поля `token`/`api_key`/`secret`/`password`.
2. Добавить хелпер `redact_config(json) -> json` и использовать его перед `info!`.

**Критерии приёмки.**
- Тест/ривью: в лог не попадают значения ключей из `config`.

---

## B6. 🟡 `PluginAccess::Filesystem` объявлено, но не реализовано

**Проблема.** `src/plugin/config.rs:9` `Filesystem(String, String, String)` (path, dir_perms, file_perms) — мёртвое право; в WIT/WASI только `read_file` (read-only). Запись из WASM не предусмотрена. Вводит в заблуждение (намёкает на контроль записи, которого нет).

**Решение.**
1. Либо удалить `Filesystem` из `PluginAccess` (и WIT), пока не реализовано.
2. Либо реализовать `write_file` с этим же белым списком (fail-closed) — но это расширение поверхности, не требуемое текущим аудитом. Рекомендуется вариант 1 (честность контракта).

**Критерии приёмки.**
- `cargo build` без неиспользуемого варианта enum (или документированный `#[allow(dead_code)]` с пометкой «planned»).

---

## B7. 🟡 Metrics-экспортер без auth на `0.0.0.0`

**Проблема.** `src/metrics.rs:251` `TcpListener::bind(&addr)` где `addr = host:port` (default `127.0.0.1:9090`, но конфигурируемо). По умолчанию `enabled=false`. При включении — информационная утечка метрик (активные сессии, топология косвенно) без auth.

**Решение.**
1. Оставить default `127.0.0.1`; документировать, что публичный bind требует внешнего auth (reverse-proxy).
2. Опционально: простой bearer-токен на `/metrics`.

**Критерии приёмки.**
- Default bind = `127.0.0.1` (тест на `SocketAddr`); док-ривью.

---

## B8. 🟡 `build_http_response`: `unwrap` на невалидном `status` из payload

**Проблема.** `src/host/http_server.rs:204`:
```rust
.status(StatusCode::from_u16(status).unwrap_or(StatusCode::OK))
```
`status` берётся из недоверенного `payload` плагина как `u64`. `from_u16` паникует при значениях >999, но `.unwrap_or(OK)` это ловит — на самом деле безопасно. Мелочь: при `status = 0` (неверный) `from_u16(0)` → `Err` → `OK`, что может скрывать ошибки плагина. Не паника, но семантика «молча 200» неидеальна.

**Решение.**
- При невалидном `status` логировать warning и возвращать `502 Bad Gateway` (ошибка upstream-плагина), а не `200 OK`.

**Критерии приёмки.**
- Тест: `status=999` (или `0`) → ответ `502`, warning в лог.

---

## B9. 🟡 `std::process::exit(1)` внутри tokio-задачи при стартовом таймауте

**Проблема.** `src/main.rs:154` внутри `tokio::spawn` при стартовом таймауте готовности:
```rust
std::thread::sleep(Duration::from_millis(200));
std::process::exit(1);
```
Грубый выход из задачи (не дождавшись корректного shutdown других задач/логгера). Функционально работает (fail-fast), но мешает graceful shutdown и может оборвать flush логов.

**Решение.**
1. Сигнализировать через `tokio::sync::watch`/`Notify` основному таску (`main`) о неготовности, и делать `exit(1)` в `main` после общей процедуры shutdown (аналогично существующему блоку сохранения историй).
2. Либо оставить как есть, но убрать `std::thread::sleep` (заменить на flush через `flexi_logger` handle).

**Критерии приёмки.**
- Поведенческий тест/smoke: при неготовности плагинов за `startup.timeout_secs` процесс завершается с кодом 1, логи дописаны.

---

# Порядок реализации (предлагаемый)

1. **B1 + B2** (🔴 HIGH) — сделать в первую очередь, до любого публичного запуска mesh / загрузки недоверенных плагинов. Обе fail-closed, аддитивны к существующему коду.
2. **B3, B4, B5** (🟠 MEDIUM) — следующая итерация; касаются модели доверия и утечки.
3. **A1, A2, A3, A4, B6–B9** (🟡 LOW) — бэклог/robustness; A1 важнее остальных LOW (ломает mesh-корректность).

# Заметка по стилю патчей (конвенция проекта)

- Все проверки прав выносить в чистые функции (`can_plugin_*`) и покрывать юнит-тестами (образец: `src/plugin/engine.rs` `can_plugin_print`/`can_plugin_read_file`, `src/messages/bus.rs` `session_local_deny_decision`).
- Fail-closed для границ доверия (консоль/fs уже так сделаны — держать единообразие для сети/`Network`).
- Не ломать обратную совместимость WIT без необходимости (B6 — удаление `Filesystem` не меняет схему вызовов плагинов, только enum прав).
