# Plugin API (на русском)

Как писать плагины для `andychoir` и какие возможности доступны каждому
плагину. Английская версия: [PLUGIN-API.md](PLUGIN-API.md).

---

## 1. Обзор

В `andychoir` **всё является плагином**. Хост-процесс — тонкая среда выполнения;
всё поведение — агенты, инструменты, фронт-енды, MCP-мосты — реализовано
**WebAssembly-плагинами**, загружаемыми из `.wasm`-файлов.

Ключевые свойства:

* **Песочница.** Плагин выполняется внутри WASM-песочницы (wasmtime). У него
  **нет прямого** доступа к файловой системе, сети, окружению или времени.
  Любая привилегированная операция идёт через **host-API** (см. §4), где хост
  применяет контроль доступа и аудит.
* **Async по умолчанию.** Гость экспортирует асинхронный интерфейс
  `plugin-lifecycle`; хост ожидает его кооперативно (WASM-нить не блокируется
  во время `await` хост-вызова).
* **Событийная модель.** Плагины общаются только публикуя/подписываясь на
  `Event` в in-process шине событий хоста (см. §5). Между плагинами нет общего
  состояния.
* **Незнание топологии.** Плагин-агент запрашивает инструмент *по имени*
  (`tool:calculator`); хост разрешает его в конкретный узел mesh
  (см. `docs/tool-prioritization.md`). Плагины никогда не обращаются к другим
  хостам напрямую.

---

## 2. Структура плагина

### 2.1 `Cargo.toml`

```toml
[package]
name = "my_plugin"
version = "0.1.0"
edition = "2024"
license = "MIT OR Apache-2.0"

[lib]
# WASM-компонент должен быть cdylib, потребляемым wit-bindgen.
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.58"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
# ... прочие crate, допустимые в песочнице (чистый Rust, без прямого OS-доступа)
```

### 2.2 Каркас `src/lib.rs`

```rust
wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use exports::ai::host::plugin_lifecycle::Guest;
use ai::host::types::Event;

struct MyPlugin;

impl Guest for MyPlugin {
    async fn init(config_json: String) -> Vec<String> {
        // Распарсить конфиг, инициализировать состояние, вернуть список
        // масок target, которые вы хотите получать (напр. ["agent:*", "my:plugin"]).
        vec!["agent:*".to_string()]
    }

    async fn run() {
        // Опциональный фоновый цикл (фронт-енды читают stdin,
        // или демоны). Хост вызывает его один раз после init всех плагинов.
    }

    async fn handle_event(ev: Event) {
        // Вызывается для каждого события, чей target совпал с вашей маской из init.
    }
}

export!(MyPlugin);
```

Контракт WIT лежит в [`wit/plugin.wit`](../wit/plugin.wit) и является
единственным источником истины для описанного ниже API.

---

## 3. Конфигурация плагина (`PluginConfig`, YAML)

Каждый плагин регистрируется в конфиге хоста (YAML/JSON/TOML) через
`PluginConfig`:

| Поле              | Тип                   | По умолч. | Значение                                                                 |
|-------------------|-----------------------|-----------|--------------------------------------------------------------------------|
| `file`            | `string`              | —         | Путь к скомпилированному `.wasm`-компоненту.                              |
| `name`            | `string`              | —         | Имя плагина. Хост выводит **класс** из префикса: `agent:`, `tool:`, `front:`, `mcp:` (см. §6). |
| `access`          | `Vec<PluginAccess>`    | `[]`      | Выданные capability (см. ниже).                                          |
| `allow_background`| `bool`                | `false`   | Если `true`, хост держит долгоживущий инстанс и вызывает `run()`.         |
| `session_local`   | `bool`                | `false`   | Если `true`, инструменты плагина **приватны** для активных локальных сессий (см. §7.3). |
| `config`          | `serde_json::Value`   | `{}`      | Свободные параметры инициализации, передаются как есть аргументом в `init()`. |

### Варианты `PluginAccess`

| Вариант            | Аргумент                    | Даёт                                                         |
|--------------------|-----------------------------|--------------------------------------------------------------|
| `console_input`    | `String` (prompt)           | Право вызывать `console.read-line` с заданным приглашением.   |
| `console_print`    | `u32` (макс. байт строки)   | Право вызывать `console.print-line` / `print-markdown`.        |
| `filesystem`       | `(path, dir_perms, file_perms)` | Песок путей; perms — `"ro"`/`"rw"`.                    |
| `network`          | `Vec<(host, port)>`         | Разрешённые исходящие адреса для `http.post-json`.            |
| `read_file`        | `Vec<String>` (пути)        | Белый список файлов/каталогов для `host-control.read-file`.   |

> **Fail-closed:** если capability не выдан, соответствующий хост-вызов
> возвращает ошибку (или отклоняется) — никогда не разрешается молча.

---

## 4. Host API (WIT `ai:host`)

Все вызовы делаются как `ai::host::<interface>::<func>(...)`. Асинхронные
функции возвращают `Future` и отдают управление хосту во время ожидания.

### 4.1 `event-bus`
* `publish_event(ev: event)` — вставить `Event` в шину. Это единственный способ
  плагина общаться с другими плагинами / mesh.

### 4.2 `console`
* `print_line(line: string)` — напечатать сырую строку.
* `print_markdown(markdown: string)` — отрендерить Markdown (хост использует
  `termimad` → ANSI жирный/курсив/цвет/код-блоки).
* `read_line(prompt: string) -> option<string>` — чтение из консоли в стиле
  блокировки (только при наличии доступа `console_input`). `None` при EOF/Ctrl-C.

### 4.3 `log`
* `debug/info/warn/error(line: string)` — структурированное логирование хоста.

### 4.4 `http` (прокси в песочнице)
* `post_json(url, json_body) -> (u16, string)` — хост выполняет реальный запрос
  и возвращает `(статус, тело)`. Подчиняется списку доступа `network`.

### 4.5 `host-control` (async)
* `wait_for_ready()` — дождаться, пока **все** плагины сообщат о готовности.
* `wait_for_response(request_id: string)` — дождаться события `topic:"response"`
  для этого `request_id`.
* `wait_for_response_timeout(request_id, timeout_ms) -> bool` — то же с защитой по
  таймауту (рекомендуется против зависаний).
* `take_response_payload(request_id) -> option<string>` — забрать сохранённый
  payload ответа (хост буферизует его, т.к. wasmtime сериализует `handle_event`).
* `get_session_history(session_id) -> list<event>` — история диалога
  (request/response) сессии.
* `clear_session(session_id)` — очистить историю сессии (напр. `/new`).
* `new_session_id() -> string` — выпустить контролируемый хостом UUID сессии.
* `get_current_session_id() -> string` — текущая сессия (восстановлена из
  `~/.andychour/current_session.json` при наличии).
* `get_session_tools(session_id) -> list<tool-definition>` — полный список
  инструментов, доступных агенту **для этой сессии** (инструменты сессии +
  локальные инструменты хоста, после фильтра белого списка агента).
* `get_plugin_config() -> string` — собственный `config` JSON этого плагина
  (доступен также фоновому инстансу `run()`).
* `read_file(path) -> result<string, string>` — прочитать файл в рамках
  whitelist `read_file` и прав `filesystem`. Пути канонизируются (абсолютный +
  развёрнутые `..`) перед проверкой whitelist.

### 4.6 `http-server` / `ws-server`
Фронт-енды регистрируют слушатели и получают входящий трафик как события шины:
* `listen_http(listener {port, path, target}) -> bool`
* `remove_listener(port, path) -> bool`
* `get_listeners() -> list<listener>`
* (аналогичное трио для `ws-server` с `ws-listener`).
  Входящий HTTP/WS-трафик становится событием шины с `source:"host:http"` /
  `source:"host:ws"` и `target:"<плагин>:<порт>:<uri>"`.

### 4.7 `mcp-transport`
MCP-клиентские плагины реализуют протокол MCP; хост даёт только транспорт:
* `stdio_open(command, args, env) -> string` (transport-id, или `"-"` при ошибке).
* `request(transport_id, jsonrpc, timeout_ms) -> option<string>`.
* `close(transport_id)`.

### 4.8 `plugin-lifecycle` (экспортируется гостем)
* `init(config_json) -> list<string>` — распарсить конфиг, вернуть маски
  подписок. Опубликовать событие `status:"ready"` при инициализации.
* `run()` — опциональный фоновый цикл (требует `allow_background: true`).
* `handle_event(ev)` — обработать подошедшее входящее событие.

---

## 5. Модель событий и топики

`Event { request_id, session_id, source, target, topic, payload }`.

### Канонические топики
| Топик        | Направление          | Значение                                  |
|--------------|----------------------|-------------------------------------------|
| `discovery`  | agent → tool         | запрос `ToolDefinition` инструмента       |
| `definition` | tool → agent         | ответ с `ToolDefinition` (JSON)            |
| `request`    | any → any            | вызов (user→agent, agent→tool)            |
| `response`   | any → any            | результат вызова                         |
| `error`      | any → any            | ошибка                                   |
| `info`       | any → any            | служебное уведомление (логи/статус)        |
| `print`      | agent → front        | текст для печати в консоли                |

### Маски target (диспетчер хоста)
* точное имя — `"front:console"` → только этот плагин
* `"*"` — все плагины
* префикс `"*"` — `"agent:*"` → все плагины с именем, начинающимся на `agent:`

---

## 6. Классы плагинов (по префиксу имени)

Хост маршрутизирует и обрабатывает плагины по-разному в зависимости от префикса
`name`:

| Префикс  | Роль                                                 | Типичный `target` для отправки     |
|----------|------------------------------------------------------|------------------------------------|
| `agent:` | Диалоговый/LLM-агент; оркестрирует инструменты       | публикует `request` в `tool:*` / `agent:*` |
| `tool:`  | Безсостоятельная возможность (calculator, fs, …)      | получает `request`, отвечает `response` + `definition` |
| `front:` | Пользовательский ввод-вывод (console, http, ws)      | публикует `request` в `agent:*`, печатает `response`/`print` |
| `mcp:`   | MCP-клиентский мост (STDIO/HTTP MCP-сервер)           | экспонирует удалённые MCP-инструменты локально |

---

## 7. Особенности фронт-ендов

**Фронт-енд** — это плагин, соединяющий пользователя (или внешнюю систему) с
агентом. `andychoir` поставляет три фронт-енда, равных по возможностям:

### 7.1 `front:console` (встроенный плагин `front_console_plugin`)
* Читает строки из stdin (`console.read_line`), печатает `response`/`print`
  через `console.print_markdown` (жёлтые промпты пользователя, markdown-ответы).
* Публикует `request` с `source:"host:console"`, `target:"agent:*"`,
  `topic:"request"` и стабильным `session_id` + `request_id`.
* Команды: `/new` (новая сессия), `/help`.

### 7.2 `front:http` / `front:ws` (хост-серверы, не WASM)
Сам хост запускает HTTP и WebSocket серверы. Входящие запросы становятся
событиями шины с `source:"host:http"` / `source:"host:ws"` и
`target:"<плагин>:<порт>:<uri>"`. Ответы доставляются обратно в сокет по
`request_id` + `session_id`.

### 7.3 Защита `session_local` (все фронт-енды)
Приватные инструменты (`session_local: true` в `PluginConfig`)
**анонсируются** в mesh (удалённые агенты могут о них узнать), но выполнение на
этом хосте разрешено **только для активной локальной сессии**:

* На **любом** фронт-енде при старте запроса хост вызывает
  `begin_frontend_request(session_id) -> request_id`. Это регистрирует сессию
  как локальную **и** помечает её активной (есть живой `request_id`).
* При доставке ответа (или по таймауту/закрытию) хост вызывает
  `end_frontend_request(session_id, request_id)`, снимая флаг активности.
* В `bus.rs` перед выполнением `session_local`-инструмента хост отказывает, если
  сессия **чужая** (пришла из сети) **ИЛИ** **не активна** в данный момент (нет
  живого запроса от фронт-енда). Так приватный инструмент достижим только пока
  легитимная локальная сессия активно им пользуется — на **любом** фронт-енде
  (console / http / ws) через один и тот же код.

> Это делает защиту **фронт-агностичной**: console, http и ws проходят через
> `begin/end_frontend_request`, поэтому поведение везде идентично.

### 7.4 Идентичность сессии и запроса
* `session_id` — нить диалога (восстанавливается/хранится на хосте).
* `request_id` — один вызов; используется для корреляции `request`↔`response` и
  для контроля активности `session_local`.
* Фронт-енды должны копировать `session_id`/`request_id` из входящего трафика в
  исходящее `Event`, чтобы хост мог маршрутизировать ответ обратно.

---

## 8. Минимальный пример `tool:` (шаблон)

```rust
wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });
use exports::ai::host::plugin_lifecycle::Guest;
use ai::host::types::{Event, ToolDefinition};

struct Calc;

impl Guest for Calc {
    async fn init(_cfg: String) -> Vec<String> {
        // Сообщаем о готовности и подписываемся на своё имя.
        ai::host::event_bus::publish_event(&Event {
            request_id: "-".into(), session_id: "-".into(),
            source: "tool:calculator".into(), target: "*".into(),
            topic: "status".into(), payload: "ready".into(),
        });
        vec!["tool:calculator".into()]
    }
    async fn handle_event(ev: Event) {
        if ev.topic == "request" {
            let r: f64 = ev.payload.parse().unwrap_or(0.0);
            ai::host::event_bus::publish_event(&Event {
                request_id: ev.request_id, session_id: ev.session_id,
                source: "tool:calculator".into(), target: ev.source,
                topic: "response".into(), payload: (r * 2.0).to_string(),
            });
        }
    }
}
export!(Calc);
```

---

## 9. Сборка и регистрация плагина

1. `cargo build --release` в crate плагина → `target/release/*.wasm`.
2. (При необходимости) конвертировать core `wasm` в **компонент** через
   `wasm-tools component new` (согласно сборочной цепочке хоста).
3. Добавить запись `PluginConfig` в конфиг хоста, указав `file` на артефакт,
   задать `name`, `access` и флаги.
4. Перезапустить хост (по правилам проекта перезапуски выполняются пользователем).

Полные рабочие примеры см. в [`plugins/`](../plugins) (`agent_plugin`,
`front_console_plugin`, MCP-мост).
