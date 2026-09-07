# `andychoir` Improvement Plan (v0.1.0)

> Source: audit of method/algorithm correctness and security.
> Scope: two sections — "Algorithmic/logical defects" and "Security (by priority)".
> Base status: `cargo test` — 112 passed, 0 failed (host logic and network algorithms covered by unit tests).
> Rule: `git add`/`git commit` only on explicit command. This file is not auto-committed.

## Priority legend

| Priority | Meaning | Guidance |
|----------|---------|----------|
| 🔴 HIGH   | Trust-boundary exploitation (RCE/exfiltration/mesh injection) | before any public-network release |
| 🟠 MEDIUM | Requires trusted environment; leak/bypass under untrusted plugins or local clients | next iteration |
| 🟡 LOW    | Quality/robustness/docs; no impact under current trust model | backlog |

## Task summary

| ID  | Area | Title | Priority | Files |
|-----|------|-------|----------|-------|
| A1  | Algo | `node_url` for incoming connections = `"incoming"` breaks FIB to incoming neighbor | 🟡 LOW* | `src/host/net/server.rs`, `src/host/net/forward.rs`, `src/host/net/net.rs` |
| A2  | Algo | `PENDING_RESPONSES` / `RESPONSE_PAYLOADS` leak per `request_id` | 🟡 LOW | `src/plugin/engine.rs` |
| A3  | Algo | `history_append` holds write-lock for the whole push (contention) | 🟡 LOW | `src/plugin/engine.rs` |
| A4  | Algo | WIT doc bug: `request-id`/`session-id` comments swapped | 🟡 LOW | `wit/plugin.wit` |
| B1  | Sec  | Network auth missing: `token` never validated | 🔴 HIGH | `src/host/net/server.rs`, `src/host/net/outbound.rs`, `src/config/config.rs` |
| B2  | Sec  | `PluginAccess::Network` not enforced in `post_json` | 🔴 HIGH | `src/plugin/engine.rs`, `src/plugin/config.rs`, `wit/plugin.wit` |
| B3  | Sec  | MCP transport = arbitrary host process spawn | 🟠 MEDIUM | `src/host/mcp_transport.rs` |
| B4  | Sec  | HTTP/WS frontends without auth + `session_local` bypass | 🟠 MEDIUM | `src/host/http_server.rs`, `src/host/ws_server.rs`, `src/plugin/engine.rs` |
| B5  | Sec  | Secret leak to log (plugin `config`) | 🟠 MEDIUM | `src/plugin/engine.rs` |
| B6  | Sec  | `PluginAccess::Filesystem` declared but unused (dead right) | 🟡 LOW | `src/plugin/config.rs`, `src/plugin/engine.rs` |
| B7  | Sec  | Metrics exporter without auth on `0.0.0.0` | 🟡 LOW | `src/metrics.rs` |
| B8  | Sec  | `build_http_response`: `unwrap` on invalid `status` from payload | 🟡 LOW | `src/host/http_server.rs` |
| B9  | Sec  | `std::process::exit(1)` inside tokio task on startup timeout | 🟡 LOW | `src/main.rs` |

> \*A1 is not security but breaks mesh correctness for asymmetric links — listed separately per request.

---

# Part A. Algorithmic / logical defects

## A1. `node_url` for incoming connections = `"incoming"` breaks FIB to incoming neighbor

**Problem.** In `src/host/net/server.rs:73`, when registering a node connected via an *incoming* WS, the code writes:
```rust
inner.node_url.write().await.insert(origin.clone(), "incoming".to_string());
```
In `src/host/net/forward.rs` (branch P5.1), FIB routing looks up:
```rust
let node_url_map = inner.node_url.read().await;
if let Some(url) = node_url_map.get(&next_hop) {       // next_hop = "incoming"
    let outbound = inner.outbound.read().await;
    if let Some(tx) = outbound.get(url) { ... }          // outbound key = real ws://…, not "incoming"
}
```
Consequence: for a node known *only* as an incoming connection (asymmetric link: A dial-out → B, but C dial-out → A, i.e. A knows C only incoming), the FIB route to C is built (`rebuild_fib` is correct) but sending via it is impossible — `outbound.get("incoming")` is empty. The event only goes out if the `request_origin` branch (backward-compat) fires, i.e. only as a reply to a previously received request. Arbitrary forwarding to such a neighbor (via mesh) does not work.

**Masked by tests.** Tests `p5_forward_routes_via_fib`, `p7_end_to_end_mesh_routing`, etc. manually set `node_url = real url`, so the bug is not caught.

**Fix (additive, no breakage of current logic).**
1. Maintain a separate return-path map for *incoming* neighbors, or store the real return-path in `node_url`.
2. Minimal: in `server.rs` `handle_incoming`, register `node_url[origin] = "<incoming>:<peer-addr>"` (or add `incoming_return_path: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>`), and in `forward.rs` P5.1 check `incoming_senders.get(&next_hop)` first (as already done in P5.2), then `outbound`.
3. Clean variant: `forward_inner` after `route_next_hop` tries `incoming_senders → outbound` uniformly for any `next_hop`, not relying on the string `"incoming"` in `node_url`.

**Acceptance (tests).**
- New unit test `p5_forward_to_incoming_neighbor`: build a topology where `next_hop` is known *only* as an incoming connection (registered via `incoming_senders`, `node_url` has no real url), assert `forward_inner` delivers to `incoming_senders[next_hop]`.
- Existing FIB tests stay green.

---

## A2. `PENDING_RESPONSES` / `RESPONSE_PAYLOADS` leak per `request_id`

**Problem.**
- `register_wait_response` (`src/plugin/engine.rs:86`) inserts into `PENDING_RESPONSES`; removed only in `signal_response` (real reply). On `wait_for_response_timeout` → timeout → entry **stays** forever.
- `store_response_payload` (`engine.rs:118`) writes to `RESPONSE_PAYLOADS` and never clears.

For a long-lived host under high request rate — monotonic growth of two HashMaps (memory leak).

**Fix.**
1. In `wait_for_response_timeout`, on expiry, perform a `signal_response`-like cleanup (remove entry if still present and not signaled).
2. For `RESPONSE_PAYLOADS`: either TTL cleanup (background task by `Instant::now() - stored > N sec`), or remove payload immediately after `take_response_payload` (already does `remove`, but unclaimed payloads live forever). Add age-based background cleanup.
3. Alternative (clean): bound size via `DashMap::remove` on expiry, analogous to `dedup` (`src/host/net/dedup.rs`).

**Acceptance (tests).**
- Unit test: after `wait_for_response_timeout` with expiry and no reply — `PENDING_RESPONSES` does not contain `request_id`.
- (Optional) integration: N timed-out requests → map grows at most by a constant.

---

## A3. `history_append` holds write-lock for the whole push

**Problem.** `src/plugin/engine.rs:305` `history_append`:
```rust
let mut map = session_histories().write().await;   // exclusive lock
let entries = map.entry(...).or_default();
if entries.len() >= max_events { ... drain ... }
entries.push(ev.clone());
```
Whole push is under a `write()` on the global `RwLock<HashMap<String, Vec<Event>>>`. Agent history reads (`get_session_history` from WASM) are blocked. Under high event rate — throughput bottleneck for the bus.

**Fix.**
- Move to per-session structures (`DashMap<String, SessionHistory>` with a local `Mutex` on `Vec<Event>`, analogous to `SESSION_TOOLS`/`LOCAL_TOOLS`) so one session's write doesn't block others' read/write.
- Or: snapshot under read-lock, modify locally, replace under a short write (as already done in `save_session_to_disk`).

**Acceptance.**
- Benchmark/test: concurrent `history_append` for N sessions does not block `history_get` (no deadlock + per-session order preserved).

---

## A4. WIT doc bug: `request-id`/`session-id` comments swapped

**Problem.** `wit/plugin.wit:5-6`:
```
request-id: string, // end-to-end user SESSION id   ← wrong
session-id: string, // end-to-end user REQUEST id    ← wrong
```
Per code (`src/messages/bus.rs`, `src/host/*`): `request_id` = per-request/response id, `session_id` = end-to-end dialog session id.

**Fix.** Correct the comments (no schema change — backward compatible). Optionally extend WIT comments with the topic dictionary (already in `types`).

**Acceptance.** Doc review; `cargo build` semantics unchanged.

---

# Part B. Security (by priority)

## B1. 🔴 Network authentication missing: `token` never validated

**Problem.**
- `src/config/config.rs` defines `NetConfig.token: Vec<String>` (incoming tokens) and `NetRemote.token: String` (token to remote).
- `src/host/net/server.rs` (`run_incoming_server`, `handle_incoming`) accepts any WS connection to `0.0.0.0:listen_port/net` and immediately processes `Capabilities`/`Hello`/`Event`. **No token check at all.**
- `src/host/net/outbound.rs` (`run_outbound_loop`) sends `NetRemote.token` to the remote, but the receiving side never validates it.

Consequence (RCE-class via mesh): anyone who can reach `listen_port` can
(a) inject arbitrary `Event`s into the host bus (invoke any local tool/agent);
(b) announce fake `Capabilities` — inject malicious tools into the mesh (hijack calls via `resolve_tool_target` Tier 3, `orchestrator.rs`);
(c) read other nodes' responses/sessions.

The `token` field creates a false sense of security.

**Fix (fail-closed, additive to handshake).**
1. Add `Auth { token: String }` to `NetMessage` (or send token as first message / `?token=` query param).
2. In `handle_incoming` (`server.rs`): before processing `Capabilities`/`Hello`/`Event`, require `Auth`; compare against `inner.cfg.token` (any match). On mismatch — `ws_sink.close()` and `return`.
3. In `run_outbound_loop` (`outbound.rs`): send `Auth` as the first message on connect (use existing `remote.token`).
4. Optional: sign `Hello`/`Event` (HMAC over `node_id`+message+key) to prevent `origin` spoofing.

**Acceptance (tests).**
- Unit/integration: connection without/with wrong token → rejected, `incoming_senders` not populated, `origin_tools` not added.
- Connection with valid token → `Capabilities`/`Hello` exchange proceeds (existing discovery tests stay green).
- Outbound-side test: outgoing connection sends `Auth` first.

---

## B2. 🔴 `PluginAccess::Network` not enforced in `post_json`

**Problem.** `src/plugin/engine.rs:756` (`HostWithStore::post_json`):
```rust
async fn post_json(_accessor, url, json_body) -> (u16, String) {
    // TODO(P1): add plugin network-access check (PluginAccess::Network) — fail-closed, like console.
    let client = reqwest::Client::builder().timeout(Duration::from_secs(60)).build()...;
    client.post(&url)...
}
```
`PluginAccess::Network(Vec<(String, u16)>)` is declared in `src/plugin/config.rs:11` and WIT (`plugin.wit:66-69`) but **never read** in `post_json`. Any loaded plugin has unrestricted outbound network access (SSRF/exfiltration/scanning). The WASM sandbox doesn't help — the request goes from the host.

**Fix (fail-closed, like console).**
1. In `post_json`, get `current_plugin_permissions` (as already done in `print_line`/`read_line` via `accessor.with(...)`).
2. Check for `PluginAccess::Network(allowed)`; resolve `url` to `(host, port)` and match against `allowed`. On missing right / mismatch — return `(403, {"error":"network access denied"})` (fail-closed).
3. Extract a pure function `can_plugin_network(perms, url) -> bool` and cover with unit tests (analog of `can_plugin_print`/`can_plugin_read_file`).

**Acceptance (tests).**
- `can_plugin_network` allows exact `(host, port)`, denies everything else on empty list, denies without `Network` right.
- Integration: plugin without `Network` gets `(403, …)` on any `post_json`.

---

## B3. 🟠 MCP transport = arbitrary host process spawn

**Problem.** `src/host/mcp_transport.rs:43` `stdio_open`:
```rust
let mut cmd = tokio::process::Command::new(command);
cmd.args(args).stdin(Stdio::piped())...;
```
Any `command` + `args` + `env` from a plugin → spawn a host binary. A plugin with `mcp_transport` access gets arbitrary process execution (sandbox escape via a legitimate interface). Acceptable **only** for fully trusted plugins; critical in a mesh with untrusted plugins.

**Fix.**
1. Allowlist of permitted commands/binaries for `stdio_open` (config `mcp.allowed_binaries`), fail-closed on mismatch.
2. Or explicitly document the trust model: "plugin with `mcp_transport` = host root-equivalent" (in `PLUGIN-API.md`/README).

**Acceptance.**
- `stdio_open` with a command outside the allowlist → returns `"-"` (refuse), does not spawn.
- Trust-model doc review.

---

## B4. 🟠 HTTP/WS frontends without auth + `session_local` bypass

**Problem.**
- `src/host/http_server.rs:231` / `src/host/ws_server.rs:70` listen on `0.0.0.0` without authentication.
- A direct HTTP call to `front:http` calls `begin_frontend_request` (`http_server.rs:141`), which **registers the session as local and active** → the request gets access to `session_local` tools (`src/messages/bus.rs:485` `should_deny_session_local` returns `false`, since the session is local and active).
- The `session_local` guard (meant against *network* foreign sessions) **does not protect** against a direct local HTTP/WS client. If the frontend is public — anyone invokes private tools.

**Fix.**
1. Default frontend bind to `127.0.0.1` (config `http.bind`/`ws.bind`, default `127.0.0.1`).
2. Optional: auth on frontends (token in header/query, matched against config).
3. Or: `session_local` should require an extra "trusted frontend" flag (separate flag in `begin_frontend_request`) so an arbitrary HTTP client cannot reach private tools.

**Acceptance (tests).**
- `start_http_servers`/`start_ws_servers` bind `127.0.0.1` by default (test on `SocketAddr`).
- Integration: direct HTTP call to a `session_local` tool without trusted flag → denied (if option 3 chosen).

---

## B5. 🟠 Secret leak to log (plugin `config`)

**Problem.** `src/plugin/engine.rs:1257`:
```rust
info!("[Хост] Конфигурация плагина: {:?}", config_str);
```
Prints the **entire plugin JSON config** (may contain API keys/tokens) to the log file (`flexi_logger`, `src/host/log.rs`). Meanwhile `mcp_transport` correctly does NOT log env values (`mcp_transport.rs:42` "env values are NOT logged") — inconsistent.

**Fix.**
1. Do not log `config` in full; log only "safe" fields (name, `access` list, `session_local`) or redact `token`/`api_key`/`secret`/`password` fields.
2. Add a `redact_config(json) -> json` helper and use it before `info!`.

**Acceptance.**
- Test/review: no key values from `config` appear in the log.

---

## B6. 🟡 `PluginAccess::Filesystem` declared but unused

**Problem.** `src/plugin/config.rs:9` `Filesystem(String, String, String)` (path, dir_perms, file_perms) — dead right; WIT/WASI only has `read_file` (read-only). No write from WASM. Misleading (implies write control that doesn't exist).

**Fix.**
1. Either remove `Filesystem` from `PluginAccess` (and WIT) until implemented.
2. Or implement `write_file` with the same allowlist (fail-closed) — but that widens the surface, not required by this audit. Variant 1 (honest contract) recommended.

**Acceptance.**
- `cargo build` without the unused enum variant (or documented `#[allow(dead_code)]` marked "planned").

---

## B7. 🟡 Metrics exporter without auth on `0.0.0.0`

**Problem.** `src/metrics.rs:251` `TcpListener::bind(&addr)` where `addr = host:port` (default `127.0.0.1:9090`, configurable). Default `enabled=false`. When enabled — info leak of metrics (active sessions, indirect topology) without auth.

**Fix.**
1. Keep default `127.0.0.1`; document that a public bind needs external auth (reverse-proxy).
2. Optional: simple bearer token on `/metrics`.

**Acceptance.**
- Default bind = `127.0.0.1` (test on `SocketAddr`); doc review.

---

## B8. 🟡 `build_http_response`: `unwrap` on invalid `status` from payload

**Problem.** `src/host/http_server.rs:204`:
```rust
.status(StatusCode::from_u16(status).unwrap_or(StatusCode::OK))
```
`status` comes from the untrusted plugin `payload` as `u64`. `from_u16` panics on >999, but `.unwrap_or(OK)` catches it — actually safe. Minor: on `status = 0` (invalid) `from_u16(0)` → `Err` → `OK`, which can hide plugin errors. Not a panic, but "silently 200" semantics are suboptimal.

**Fix.**
- On invalid `status`, log a warning and return `502 Bad Gateway` (upstream plugin error) instead of `200 OK`.

**Acceptance.**
- Test: `status=999` (or `0`) → `502` response, warning in log.

---

## B9. 🟡 `std::process::exit(1)` inside tokio task on startup timeout

**Problem.** `src/main.rs:154` inside `tokio::spawn` on startup readiness timeout:
```rust
std::thread::sleep(Duration::from_millis(200));
std::process::exit(1);
```
Abrupt exit from a task (without waiting for other tasks'/logger's graceful shutdown). Functionally works (fail-fast) but blocks graceful shutdown and may truncate log flush.

**Fix.**
1. Signal via `tokio::sync::watch`/`Notify` to the main task (`main`) about unreadiness, and `exit(1)` in `main` after the common shutdown procedure (analogous to the existing history-save block).
2. Or keep as-is but drop `std::thread::sleep` (replace with flush via `flexi_logger` handle).

**Acceptance.**
- Behavioral/smoke: on plugin unreadiness within `startup.timeout_secs`, process exits code 1, logs flushed.

---

# Suggested implementation order

1. **B1 + B2** (🔴 HIGH) — first, before any public mesh run / loading untrusted plugins. Both fail-closed, additive to existing code.
2. **B3, B4, B5** (🟠 MEDIUM) — next iteration; trust model and leakage.
3. **A1, A2, A3, A4, B6–B9** (🟡 LOW) — backlog/robustness; A1 is more important than the other LOWs (breaks mesh correctness).

# Patch-style note (project convention)

- Extract all permission checks into pure functions (`can_plugin_*`) and cover with unit tests (model: `src/plugin/engine.rs` `can_plugin_print`/`can_plugin_read_file`, `src/messages/bus.rs` `session_local_deny_decision`).
- Fail-closed at trust boundaries (console/fs already done — keep consistent for network/`Network`).
- Do not break WIT backward compatibility unnecessarily (B6 removing `Filesystem` does not change plugin call schema, only the rights enum).
