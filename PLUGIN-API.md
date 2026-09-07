# Plugin API

How to write plugins for `andychoir`, and what capabilities each plugin gets.
Companion Russian version: [PLUGIN-API.ru.md](PLUGIN-API.ru.md).

---

## 1. Overview

In `andychoir` **everything is a plugin**. The host process is a thin runtime;
all behavior — agents, tools, front-ends, MCP bridges — is provided by
**WebAssembly plugins** loaded from `.wasm` files.

Key properties:

* **Sandboxed.** A plugin runs inside a WASM sandbox (wasmtime). It has **no
  direct** filesystem, network, environment, or clock access. Every privileged
  operation goes through the **host API** (see §4), where the host enforces
  access control and audit.
* **Async by default.** The guest exports an async `plugin-lifecycle`
  interface; the host awaits it cooperatively (no WASM thread is blocked while
  the guest `await`s a host call).
* **Event-driven.** Plugins communicate only by publishing/subscribing to
  `Event`s on the host's in-process event bus (see §5). There is no shared
  memory between plugins.
* **Topology-blind.** An agent plugin asks for a tool *by name*
  (`tool:calculator`); the host resolves it to a concrete node in the mesh
  (see `docs/tool-prioritization.md`). Plugins never address other hosts
  directly. Inter-host mesh traffic can be encrypted and mutually authenticated
  with mTLS (internal CA) — see `docs/NET-concept.md` §8.6; it is off by default.

---

## 2. Plugin structure

### 2.1 `Cargo.toml`

```toml
[package]
name = "my_plugin"
version = "0.1.0"
edition = "2024"
license = "MIT OR Apache-2.0"

[lib]
# A WASM component must be a cdylib consumed by wit-bindgen.
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.58"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
# ... other crates allowed in the sandbox (pure-Rust, no raw OS access)
```

### 2.2 `src/lib.rs` skeleton

```rust
wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });

use exports::ai::host::plugin_lifecycle::Guest;
use ai::host::types::Event;

struct MyPlugin;

impl Guest for MyPlugin {
    async fn init(config_json: String) -> Vec<String> {
        // Parse your config, set up state, return the list of target
        // masks you want to receive (e.g. ["agent:*", "my:plugin"]).
        vec!["agent:*".to_string()]
    }

    async fn run() {
        // Optional background loop (used by front-ends to read stdin,
        // or by daemons). Called once by the host after all plugins init.
    }

    async fn handle_event(ev: Event) {
        // Called for every event whose target matches one of your init masks.
    }
}

export!(MyPlugin);
```

The WIT contract lives in [`wit/plugin.wit`](../wit/plugin.wit) and is the
single source of truth for the API described below.

---

## 3. Plugin configuration (`PluginConfig`, YAML)

Each plugin is registered in the host config (YAML/JSON/TOML) with a
`PluginConfig`:

| Field             | Type                  | Default | Meaning                                                                 |
|-------------------|-----------------------|---------|-------------------------------------------------------------------------|
| `file`            | `string`              | —       | Path to the compiled `.wasm` component.                                 |
| `name`            | `string`              | —       | Plugin name. The host derives its **class** from the prefix: `agent:`, `tool:`, `front:`, `mcp:` (see §6). |
| `access`          | `Vec<PluginAccess>`    | `[]`    | Capability grants (see below).                                         |
| `allow_background`| `bool`                | `false` | If `true`, the host keeps a long-lived instance and calls `run()`.      |
| `session_local`   | `bool`                | `false` | If `true`, the plugin's tools are **private** to active local sessions (see §7.3). |
| `config`          | `serde_json::Value`   | `{}`    | Free-form init parameters, passed verbatim as the `init()` argument.     |

### `PluginAccess` variants

| Variant                       | Argument                | Grants                                                      |
|-------------------------------|------------------------|-------------------------------------------------------------|
| `console_input`               | `String` (prompt)      | Right to call `console.read-line` with the given prompt.    |
| `console_print`               | `u32` (max line bytes) | Right to call `console.print-line` / `print-markdown`.       |
| `filesystem`                  | `(path, dir_perms, file_perms)` | Path sandbox; perms are `"ro"`/`"rw"`.              |
| `network`                     | `Vec<(host, port)>`    | Allowed outbound destinations for `http.post-json`.         |
| `read_file`                   | `Vec<String>` (paths)  | Whitelist of files/dirs the plugin may read via `host-control.read-file`. |

> **Fail-closed:** if a capability is not granted, the corresponding host call
> returns an error (or is denied) — never silently allowed.

### MCP transport trust model

The `mcp:client` plugin asks the host to spawn a stdio subprocess (the MCP server)
via `ai::host::mcp-transport::stdio_open`. **The host spawns a real OS process**, so
a plugin that can configure an MCP server effectively gets arbitrary process execution
on the host. Treat `mcp_transport` as **host root-equivalent**.

To contain this, the host enforces a **binary allowlist** (`Config.mcp.allowed_binaries`):
- empty (default) → any binary is allowed, but the host logs a `warn!` ("not safe in a mesh
  with untrusted plugins");
- non-empty → **fail-closed**: `stdio_open` refuses (returns `"-"`, no spawn) any command
  not in the list (exact match or `basename`). This is enforced on the host side and cannot
  be bypassed by the plugin. Set `mcp.allowed_binaries` to the exact servers you run (e.g.
  `["npx", "uv", "node"]`) before loading any plugin you do not fully trust.

---

## 4. Host API (WIT `ai:host`)

All calls are made as `ai::host::<interface>::<func>(...)`. Async functions
return `Future`s and yield control to the host while waiting.

### 4.1 `event-bus`
* `publish_event(ev: event)` — inject an `Event` into the bus. This is the only
  way a plugin talks to other plugins / the mesh.

### 4.2 `console`
* `print_line(line: string)` — print a raw line.
* `print_markdown(markdown: string)` — render Markdown (host uses `termimad` →
  ANSI bold/italic/color/code blocks).
* `read_line(prompt: string) -> option<string>` — blocking-style read from the
  console (only with `console_input` access). `None` on EOF/Ctrl-C.

### 4.3 `log`
* `debug/info/warn/error(line: string)` — structured host logging.

### 4.4 `http` (sandboxed proxy)
* `post_json(url, json_body) -> (u16, string)` — the host performs the real
  request and returns `(status, body)`. Subject to the `network` access list.

### 4.5 `host-control` (async)
* `wait_for_ready()` — await until **all** plugins signal readiness.
* `wait_for_response(request_id: string)` — await a `topic:"response"` event
  for this `request_id`.
* `wait_for_response_timeout(request_id, timeout_ms) -> bool` — same with a
  timeout guard (recommended to avoid hangs).
* `take_response_payload(request_id) -> option<string>` — retrieve the stored
  payload of the response (the host buffers it because wasmtime serializes
  `handle_event`).
* `get_session_history(session_id) -> list<event>` — dialogue history
  (request/response) for a session.
* `clear_session(session_id)` — drop a session's history (e.g. `/new`).
* `new_session_id() -> string` — mint a host-controlled session UUID.
* `get_current_session_id() -> string` — current session (restored from
  `~/.andychour/current_session.json` if present).
* `get_session_tools(session_id) -> list<tool-definition>` — full tool list
  available to the agent **for this session** (session tools + host local
  tools, after the agent's whitelist filter).
* `get_plugin_config() -> string` — this plugin's own `config` JSON (also
  available to the background `run()` instance).
* `read_file(path) -> result<string, string>` — read a file, constrained by the
  `read_file` whitelist and `filesystem` perms. Paths are canonicalized
  (absolute + resolved `..`) before the whitelist check.

### 4.6 `http-server` / `ws-server`
Front-ends register listeners and receive inbound traffic as bus events:
* `listen_http(listener {port, path, target}) -> bool`
* `remove_listener(port, path) -> bool`
* `get_listeners() -> list<listener>`
* (same trio for `ws-server` with `ws-listener`).
  Inbound HTTP/WS traffic becomes a bus event with `source:"host:http"` /
  `source:"host:ws"` and `target:"<plugin>:<port>:<uri>"`.

### 4.7 `mcp-transport`
MCP client plugins implement the MCP protocol; the host provides only transport:
* `stdio_open(command, args, env) -> string` (transport-id, or `"-"` on error).
* `request(transport_id, jsonrpc, timeout_ms) -> option<string>`.
* `close(transport_id)`.

### 4.8 `plugin-lifecycle` (exported by the guest)
* `init(config_json) -> list<string>` — parse config, return subscription
  masks. Publish a `status:"ready"` event when initialized.
* `run()` — optional background loop (requires `allow_background: true`).
* `handle_event(ev)` — process a matched inbound event.

---

## 5. Event model & topics

`Event { request_id, session_id, source, target, topic, payload }`.

### Canonical topics
| Topic         | Direction            | Meaning                                  |
|---------------|----------------------|------------------------------------------|
| `discovery`   | agent → tool         | request a tool's `ToolDefinition`        |
| `definition`  | tool → agent         | reply with `ToolDefinition` (JSON)       |
| `request`     | any → any            | a call (user→agent, agent→tool)          |
| `response`    | any → any            | result of a call                         |
| `error`       | any → any            | error                                   |
| `info`        | any → any            | service notification (logs/status)        |
| `print`       | agent → front        | text to print in the console             |

### Target masks (host dispatcher)
* exact name — `"front:console"` → only that plugin
* `"*"` — all plugins
* prefix `"*"` — `"agent:*"` → all plugins whose name starts with `agent:`

---

## 6. Plugin classes (by name prefix)

The host routes and treats plugins differently based on the `name` prefix:

| Prefix   | Role                                              | Typical `target` to send to |
|----------|---------------------------------------------------|------------------------------|
| `agent:` | Conversational/LLM agent; orchestrates tools      | publishes `request` to `tool:*` / `agent:*` |
| `tool:`  | Stateless capability (calculator, filesystem, …)  | receives `request`, replies `response` + `definition` |
| `front:` | User-facing I/O (console, http, ws)               | publishes `request` to `agent:*`, prints `response`/`print` |
| `mcp:`   | MCP-client bridge (STDIO/HTTP MCP server)         | exposes remote MCP tools locally |

---

## 7. Front-end specifics

A **front-end** is the plugin that connects a user (or external system) to the
agent. `andychoir` ships three front-ends, all equivalent in capability:

### 7.1 `front:console` (built-in plugin `front_console_plugin`)
* Reads lines from stdin (`console.read_line`), prints `response`/`print` via
  `console.print_markdown` (yellow user prompts, markdown answers).
* Publishes `request` with `source:"host:console"`, `target:"agent:*"`,
  `topic:"request"`, and a stable `session_id` + `request_id`.
* Commands: `/new` (new session), `/help`.

### 7.2 `front:http` / `front:ws` (host servers, not WASM)
The host itself runs HTTP and WebSocket servers. Inbound requests become bus
events with `source:"host:http"` / `source:"host:ws"` and
`target:"<plugin>:<port>:<uri>"`. Responses are delivered back to the socket
by `request_id` + `session_id`.

### 7.3 `session_local` protection (all front-ends)
Private tools (`session_local: true` in `PluginConfig`) are **advertised** in
the mesh (remote agents may learn about them), but execution on this host is
allowed **only for an active local session**:

* On **every** front, when a request starts, the host calls
  `begin_frontend_request(session_id) -> request_id`. This registers the
  session as local **and** marks it active (there is a live `request_id`).
* When the response is delivered (or on timeout/close), the host calls
  `end_frontend_request(session_id, request_id)`, clearing the active flag.
* In `bus.rs`, before executing a `session_local` tool, the host denies access
  if the session is **foreign** (came from the network) **or** **not currently
  active** (no live request from a front). Thus a private tool is reachable
  only while a legitimate local session is actively using it — on **any**
  front (console / http / ws) through the same code path.

> This makes the protection **front-agnostic**: console, http and ws all go
> through `begin/end_frontend_request`, so behavior is identical everywhere.

### 7.4 Session & request identity
* `session_id` — a conversation thread (restored/stored per host).
* `request_id` — a single call; used to correlate `request`↔`response` and to
  gate `session_local` activity.
* Fronts must copy `session_id`/`request_id` from inbound traffic into the
  outgoing `Event` so the host can route the response back.

---

## 8. Minimal `tool:` example (pattern)

```rust
wit_bindgen::generate!({ world: "host-plugin", path: "../../wit" });
use exports::ai::host::plugin_lifecycle::Guest;
use ai::host::types::{Event, ToolDefinition};

struct Calc;

impl Guest for Calc {
    async fn init(_cfg: String) -> Vec<String> {
        // Announce we are ready and subscribe to our own name.
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

## 9. Building & registering a plugin

1. `cargo build --release` in the plugin crate → `target/release/*.wasm`.
2. (If needed) convert the core `wasm` to a **component** with
   `wasm-tools component new` (per the host's build chain).
3. Add a `PluginConfig` entry in the host config pointing `file` at the
   artifact, set `name`, `access`, and flags.
4. Restart the host (per project policy, restarts are performed by the user).

See existing plugins under [`plugins/`](../plugins) (`agent_plugin`,
`front_console_plugin`, `mcp` bridge) for complete, working references.
