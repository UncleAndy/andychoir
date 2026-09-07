# `andychoir` Improvement Plan (v0.1.0)

> Source: audit of method/algorithm correctness and security.
> Scope: two sections — "Algorithmic/logical defects" and "Security (by priority)".
> Base status: `cargo test` — 129 passed, 0 failed (host logic, network algorithms, and mTLS covered by unit tests).
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
| A2  | Algo | `PENDING_RESPONSES` / `RESPONSE_PAYLOADS` leak per `request_id` | 🟢 DONE | `src/plugin/engine.rs`, `src/messages/bus.rs`, `wit/plugin.wit`, `plugins/agent_plugin/src/handle_event.rs` |
| A3  | Algo | `history_append` holds write-lock for the whole push (contention) | 🟡 LOW | `src/plugin/engine.rs` |
| A4  | Algo | WIT doc bug: `request-id`/`session-id` comments swapped | 🟡 LOW | `wit/plugin.wit` |
| B1  | Sec  | Network auth missing: `token` never validated | 🔴 HIGH | `src/host/net/server.rs`, `src/host/net/outbound.rs`, `src/config/config.rs` |
| B2  | Sec  | `PluginAccess::Network` not enforced in `post_json` | 🔴 HIGH | `src/plugin/engine.rs`, `src/plugin/config.rs`, `wit/plugin.wit` |
| B3  | Sec  | MCP transport = arbitrary host process spawn | 🟢 DONE | `src/host/mcp_transport.rs`, `src/config/config.rs`, `src/main.rs` |
| B4  | Sec  | HTTP/WS frontends without auth + `session_local` bypass | 🟢 DONE | `src/host/http_server.rs`, `src/host/ws_server.rs`, `src/plugin/engine.rs`, `wit/plugin.wit`, `plugins/front_*/src/lib.rs` |
| B5  | Sec  | Secret leak to log (plugin `config`) | 🟢 DONE | `src/plugin/engine.rs` |
| B6  | Sec  | `PluginAccess::Filesystem` declared but unused (dead right) | 🟡 LOW | `src/plugin/config.rs`, `src/plugin/engine.rs` |
| B7  | Sec  | Metrics exporter without auth on `0.0.0.0` | 🟡 LOW | `src/metrics.rs` |
| B8  | Sec  | `build_http_response`: `unwrap` on invalid `status` from payload | 🟡 LOW | `src/host/http_server.rs` |
| B9  | Sec  | `std::process::exit(1)` inside tokio task on startup timeout | 🟡 LOW | `src/main.rs` |
| B10 | Sec  | mTLS for inter-host links (internal CA, mutual auth, optional SAN binding) | 🟢 DONE | `src/host/net/tls.rs`, `src/host/net/server.rs`, `src/host/net/outbound.rs`, `src/config/config.rs` |

> \*A1 is not security but breaks mesh correctness for asymmetric links — listed separately per request.

---

# Part A. Algorithmic / logical defects

## A1. 🟢 `node_url` for incoming connections = `"incoming"` breaks FIB to incoming neighbor (IMPLEMENTED)

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

**Fix (implemented, 2026-09-07).**
- Removed the bogus `node_url[origin] = "incoming"` write in `server.rs` `handle_incoming` (Capabilities handler). The incoming neighbor is still registered in `incoming_senders` and exposed to discovery via `incoming_senders` (so `pack_hello` keeps listing it as a direct link — no topology regression).
- In `forward.rs` P5.1, after `route_next_hop` we now check `incoming_senders.get(&next_hop)` **first**; if present, the event is delivered back over the already-open incoming channel and returns `true`. Only then falls through to the `node_url`/outbound path. This makes asymmetric (incoming-only) neighbors routable via mesh, not just as reply-to-origin.

**Acceptance (tests).**
- New unit test `a1_incoming_neighbor_routes_via_incoming_senders`: topology where `next_hop` is known only as incoming (`incoming_senders` registered, `node_url` has no real url, and explicitly no `"incoming"` stub) → `forward_inner` delivers to `incoming_senders[next_hop]` and does **not** buffer into `pending_outbound["incoming"]`.
- Existing FIB tests stay green. Full suite: **134 passed, 0 warnings**.

---

## A2. 🟢 `PENDING_RESPONSES` / `RESPONSE_PAYLOADS` leak per `request_id` (IMPLEMENTED)

**Problem.**
- `register_wait_response` (`src/plugin/engine.rs:86`) inserts into `PENDING_RESPONSES`; removed only in `signal_response` (real reply). On `wait_for_response_timeout` → timeout → entry **stays** forever.
- `store_response_payload` (`engine.rs:118`) writes to `RESPONSE_PAYLOADS` and never clears.

For a long-lived host under high request rate — monotonic growth of two HashMaps (memory leak).

**Fix (implemented, 2026-09-07).** Two distinct leaks closed:
1. **Cross-session info-leak (primary).** `RESPONSE_PAYLOADS` / `PENDING_RESPONSES` were keyed only by `request_id`; any agent could `take_response_payload(any_request_id)` and read another session's reply (request_id is visible on the wire). Now keyed by `(session_id, request_id)`: to read a reply one must know **both** the `session_id` and the `request_id` — a foreign session cannot read another's reply knowing only `request_id`. (Variant 1: no extra `is_local_session` guard, so agents invoked from *remote* sessions are not broken.) WIT `wait-for-response(-timeout)` / `take-response-payload` gained a `session-id` parameter; `bus.rs` stores/clears under `(event.session_id, event.request_id)`.
2. **Memory leak (per original plan).** Orphan entries: on `wait_for_response_timeout` expiry the `PENDING_RESPONSES` entry is now removed; unclaimed `RESPONSE_PAYLOADS` carry an `Instant` and are reaped by a background task (`start_response_payload_reaper`, spawned in `main.rs`) every 60 s, dropping entries older than `RESPONSE_PAYLOAD_TTL_SECS = 300`.

**Acceptance (tests).**
- `a2_response_payload_isolated_by_session`: same `request_id` in different sessions stores different values; a foreign session cannot read another's reply.
- `a2_take_works_for_remote_session_by_exact_key`: an agent from a *remote* (non-local) session can still read its own reply by exact `(session_id, request_id)`; a foreign session cannot read another's reply (key isolation).
- `a2_pending_response_cleaned_after_signal`: entry removed from `PENDING_RESPONSES` after `signal_response`.
- Full suite: **137 passed, 0 warnings**.

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

## B3. 🟢 MCP transport = arbitrary host process spawn (IMPLEMENTED)

**Problem.** `src/host/mcp_transport.rs:43` `stdio_open`:
```rust
let mut cmd = tokio::process::Command::new(command);
cmd.args(args).stdin(Stdio::piped())...;
```
Any `command` + `args` + `env` from a plugin → spawn a host binary. A plugin with `mcp_transport` access gets arbitrary process execution (sandbox escape via a legitimate interface). Acceptable **only** for fully trusted plugins; critical in a mesh with untrusted plugins.

**Fix (implemented, 2026-09-07).**
1. `McpHostConfig { allowed_binaries: Vec<String> }` added under `Config.mcp` (`src/config/config.rs`, `#[serde(default)]`).
2. `src/host/mcp_transport.rs`: `static MCP_ALLOWED_BINARIES: OnceLock<Vec<String>>` +
   `init_mcp_policy(allowed)` (called from `main.rs` at startup from `Config.mcp.allowed_binaries`)
   + `mcp_binary_allowed(command)` checked at the top of `stdio_open`. **Fail-closed:**
   non-empty list ⇒ command outside it (exact match or `basename`) is refused (`return "-"`,
   no spawn). Empty list ⇒ allow all **with a `warn!`** (backward compatible for trusted
   deployments). No WIT change — enforcement is on the host side, plugin cannot bypass.
3. Trust model documented in `PLUGIN-API.md`.

**Acceptance (tests).**
- `mcp_transport::tests::stdio_open_denies_disallowed_binary`: `init_mcp_policy(["bash"])` ⇒
  `stdio_open("rm", …)` returns `"-"`; `init_mcp_policy(["sh"])` ⇒ `stdio_open("/usr/bin/sh", …)`
  allowed (basename match). Regression `stdio_request_echo` stays green. Full suite: **129
  passed, 0 warnings**.

---

## B4. 🟢 HTTP/WS frontends without auth + `session_local` bypass (IMPLEMENTED)

**Problem.**
- `src/host/http_server.rs:231` / `src/host/ws_server.rs:70` listen on `0.0.0.0` without authentication.
- A direct HTTP call to `front:http` calls `begin_frontend_request` (`http_server.rs:141`), which **registers the session as local and active** → the request gets access to `session_local` tools (`src/messages/bus.rs:485` `should_deny_session_local` returns `false`, since the session is local and active).
- The `session_local` guard (meant against *network* foreign sessions) **does not protect** against a direct local HTTP/WS client. If the frontend is public — anyone invokes private tools.

**Fix (implemented, 2026-09-07).**
1. **session_local bypass closed (fail-closed).** HTTP/WS frontends no longer call
   `begin_frontend_request` with the **client-supplied** `session_id`. The client's
   `session_id` (`x-session-id` header for HTTP, `session_id` in JSON for WS) is used
   *only for response correlation* and is **never registered as a local session**.
   Therefore `session_local` tools are unreachable through HTTP/WS fronts (the session
   is never marked local+active). Local session registration stays only for the real
   local console/frontend. No WIT change — enforcement is in the host.
2. **Bind address (opt-in).** WIT `listener`/`ws-listener` gained `bind: option<string>`
   (default `0.0.0.0` for backward compat). Host binds the TCP listener to this address,
   so operators can restrict to `127.0.0.1`. Invalid address → server logs and skips.
3. **Frontend auth token (opt-in).** WIT gained `auth-token: option<string>`.
   - HTTP: if set, requests require `Authorization: Bearer <token>` (or raw token);
     mismatch/absent → `401 Unauthorized`.
   - WS: if set, each message must carry `"auth": "<token>"`; mismatch → socket closed.
   Both are fail-closed when the token is configured. Empty/absent token = compat (no auth).

**Acceptance (tests).**
- `http_server::tests::http_session_id_from_header_not_registered_as_local`: a client
  `x-session-id` reaches the event (for correlation) but `is_local_session` stays `false`
  → `session_local` denied. `http_listener_serde_roundtrip_keeps_bind_and_token`: config
  parse of `bind`/`auth_token` round-trips. Full suite: **132 passed, 0 warnings**.

---

## B5. 🟢 Secret leak to log (plugin `config`) (IMPLEMENTED)

**Problem.** `src/plugin/engine.rs:1368` (was 1257):
```rust
let config_str = plugin_config.config.to_string();
info!("[Хост] Конфигурация плагина: {:?}", config_str);
```
Prints the **entire plugin JSON config** (may contain API keys/tokens) to the log file (`flexi_logger`, `src/host/log.rs`). Meanwhile `mcp_transport` correctly does NOT log env values (`mcp_transport.rs` "env values are NOT logged") — inconsistent.

**Fix (implemented, 2026-09-07).**
The full plugin config is **no longer logged by default** (fail-closed). A new `should_log_plugin_config()` helper reads the `ANDYCHOIR_LOG_PLUGIN_CONFIG` env var:
- default / unset → `false` → only `plugin name` is logged (no config values);
- `=1` / `=true` → the full config is logged (local debugging only, never in prod/mesh with other operators).

This makes secret logging opt-in and explicit, consistent with `mcp_transport`'s "env values are NOT logged" rule.

**Acceptance (tests).**
- `engine::tests::should_log_plugin_config_default_false`: without the env var, config is not logged (fail-closed). Full suite: **133 passed, 0 warnings**.

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

## B10. 🟢 mTLS for inter-host links (IMPLEMENTED)

**Problem.** The mesh transport is plain `ws://` (tokio-tungstenite over TCP). Even with
`Auth` (B1), traffic is unencrypted in transit — a network observer can read events,
tool calls, and sessions; and any host holding our CA-issued cert could impersonate
another node. There was no transport-layer confidentiality or mutual host authentication.

**Fix (implemented, 2026-09-07).**
- New module `src/host/net/tls.rs`: internal **private CA** (self-signed, generated via
  `rcgen` or supplied as files); `MtlsConfig` parsing; `ensure_certificates()` (auto-generates
  CA + node cert on first start if files are missing, idempotent); `load_server_config`
  (rustls `ServerConfig` with **required client cert** chained to our CA); `load_client_config`
  (rustls `ClientConfig`; with `require_node_id_in_san` it uses a custom `ServerCertVerifier`
  that additionally checks the server cert's DNS SAN == the peer `node_id` taken from the URL
  hostname).
- `server.rs` (`run_incoming_server`): when `mtls.enabled`, the host listens on **`wss://`**
  (tokio `TcpListener` + `TlsAcceptor` + tungstenite); shared `process_netmsg` handles both
  plain and TLS paths. `Auth` (B1) still runs on top of the encrypted channel.
- `outbound.rs` (`run_outbound_loop`): when `mtls.enabled` (remote or global), dials over
  `wss://` via `connect_async_tls_with_config` with `Connector::Rustls`; expected peer
  `node_id` is the URL hostname.
- `config.rs`: `MtlsConfig { enabled, dir, ca_cert, cert, key, require_node_id_in_san }` under
  `NetConfig.mtls` and `NetRemote.mtls` (per-link override). `#[serde(default)]` → disabled by
  default (backward compatible: plain `ws://`).
- `net.rs` (`start_net`): when `mtls.enabled`, calls `ensure_certificates` before bringing up
  the server/clients; fails fast if generation/load errors (no half-encrypted mesh).

**Limitation (documented honestly).** Strict SAN↔node_id binding is enforced on the **client
side** only. Server-side strict SAN check of the client is not implemented because the server
learns the client's `node_id` only after `Auth`/`Hello`, which arrive *after* the TLS
handshake (enforcing it in the verifier would require a wire-protocol change). The base
mutual-CA check (client cert required + chained to our CA) is fully implemented and closes the
main threat (encryption + mutual CA authentication). For production, distribute per-host certs
and keep `require_node_id_in_san: true` on the dialing side.

**Acceptance (tests).**
- `tls::tests`: `generate_then_load_server_config` (auto-gen + load), `cert_san_contains_node_id`
  (SAN == node_id), `verify_peer_node_id_matches` (match/mismatch), `ensure_noop_when_disabled`.
- `server::b1_auth_tests`: `mtls_accepts_valid_cert_and_registers` (wss client with valid cert
  registers), `mtls_rejects_plain_ws` (mTLS server rejects a plain ws client).
- Regression: B1 plain-ws tests stay green (backward compatible). Full suite: **129 passed,
  0 warnings**.

See `docs/mtls-plan.md` and `docs/NET-concept.md` §8.6 for the full design and config examples.

---

# Suggested implementation order

1. **B1 + B2** (🔴 HIGH) — first, before any public mesh run / loading untrusted plugins. Both fail-closed, additive to existing code.
2. **B3, B4, B5** (🟠 MEDIUM) — next iteration; trust model and leakage.
3. **A1, A2, A3, A4, B6–B9** (🟡 LOW) — backlog/robustness; A1 is more important than the other LOWs (breaks mesh correctness).

# Patch-style note (project convention)

- Extract all permission checks into pure functions (`can_plugin_*`) and cover with unit tests (model: `src/plugin/engine.rs` `can_plugin_print`/`can_plugin_read_file`, `src/messages/bus.rs` `session_local_deny_decision`).
- Fail-closed at trust boundaries (console/fs already done — keep consistent for network/`Network`).
- Do not break WIT backward compatibility unnecessarily (B6 removing `Filesystem` does not change plugin call schema, only the rights enum).
