# Plan: mTLS for inter-node communication in `andychoir`

> Status: proposal (implement after approval).
> Depends on: B1 (network token auth) — already implemented.
> Goal: encrypt mesh traffic and mutually authenticate nodes using the system's
> **internal private CA** (no public/external CAs). The `Auth` token (B1) is kept
> as a second factor (defense-in-depth).

---

## 1. Motivation and threat

Currently (`server.rs` / `outbound.rs`) the mesh runs over **plain `ws://`** (tokio-tungstenite
0.24 over tokio TCP). B1 added `Auth` token checking, but:
- traffic is **unencrypted** (network sniffing → read events/tools/sessions);
- the token is **sent in cleartext** in the first message (sniffing → replay/impersonation);
- there is no cryptographic binding of a node's identity to its `node_id`.

mTLS closes all of this: both peers present a certificate from the **system's internal CA**,
verify the chain to that CA, and (optionally) check SAN == `node_id`. Traffic is encrypted
(wss://), and the token is no longer exposed in plaintext.

## 2. Design (additive, fail-closed, no external CAs)

- **Internal private CA** of the system (self-signed root) issues a certificate per node
  (commonName / SAN = `node_id`, or an arbitrary SAN — see §4).
- **mTLS = mutual**: both server and client **require** the peer certificate and verify it to the CA.
- Keys/certs live **on disk** at paths from config (not embedded in the binary).
- The `Auth` token **remains** on top of the encrypted channel (second factor).
- **Backward compatible**: `mtls: None` (unset) → current behavior (plain ws). Opt-in only.

### 2.1 Identity verification (both modes, per "both options" request)
- **Basic (always when `enabled=true`)**: peer cert is issued by our internal CA (chain to
  `ca_cert` in `RootCertStore`). Guarantees "node is trusted by the system".
- **Strict (optional, `require_node_id_in_san: true`)**: extract SAN/subject from the presented
  cert and compare to the expected peer `node_id` (server: `node_id` from `Auth`/Hello; client:
  `node_id` from the remote URL). Binds TLS identity to mesh identity (a node cannot impersonate
  another even with a valid cert from our CA).

## 3. Dependencies (build risk verified)

`Cargo.lock` already contains `rustls 0.23.43` and `tokio-rustls 0.26.4` (transitive).
`tokio-tungstenite 0.24` supports the `rustls-tls` feature (pulls `rustls 0.23`) — **compatible**.

Add to `Cargo.toml`:
```toml
tokio-tungstenite = { version = "0.24", features = ["rustls-tls"] }
rustls = "0.23"
rustls-pemfile = "2"          # PEM parsing (cert/key) from files
```
For tests (in-memory self-signed CA + node certs, no files):
```toml
[dev-dependencies]
rcgen = "0.13"                # cert generation in unit tests
```

> Note: `rustls` is pure-rust, no openssl, builds fine in the NixOS-flake env (unlike
> `native-tls`/`openssl-sys`). Verified by the presence of `rustls 0.23` in the current lock.

## 4. Configuration

In `src/config/config.rs` add:

```rust
/// mTLS settings for inter-node connection (system internal CA).
#[derive(Deserialize, Clone, Default)]
#[serde(default)]
pub struct MtlsConfig {
    /// Enable mTLS. false/None → plain ws (backward compatible).
    pub enabled: bool,
    /// Path to the internal CA (PEM, root cert) we trust.
    pub ca_cert: String,
    /// Path to THIS node's certificate (PEM, issued by the internal CA).
    pub cert: String,
    /// Path to THIS node's private key (PEM).
    pub key: String,
    /// Strictly require peer cert SAN/subject == its node_id.
    /// false → "cert from our CA" is sufficient.
    pub require_node_id_in_san: bool,
}
```

In `NetConfig`:
```rust
pub struct NetConfig {
    // ...existing fields...
    #[serde(default)]
    pub mtls: MtlsConfig,
}
```

In `NetRemote` (outgoing):
```rust
pub struct NetRemote {
    pub url: String,            // with mtls: "wss://host-b:8092/net"
    pub token: String,          // kept (second factor)
    #[serde(default)]
    pub mtls: MtlsConfig,       // per-remote override (or fall back to NetConfig)
    // ...other fields...
}
```

## 5. File changes

### 5.1 New module `src/host/net/tls.rs` (pure, testable functions)
- `load_server_config(&MtlsConfig) -> Result<rustls::ServerConfig>`
  - `ca_cert` → `RootCertStore`; `cert`/`key` (PEM via `rustls-pemfile`) → `CertifiedKey`.
  - `server_config.set_client_certificate_verifier(WebPkiClientVerifier::builder(roots).build())`
    with **mandatory** client cert verification (client auth = Required).
- `load_client_config(&MtlsConfig) -> Result<rustls::ClientConfig>`
  - `RootCertStore` from `ca_cert`; `set_certificate` from `cert`/`key`.
  - when `require_node_id_in_san == true` — custom `ServerCertVerifier` checking SAN.
- `verify_peer_node_id(cert_der, expected_node_id) -> bool`
  - parse presented cert, extract SAN (DNSName/IP) / CN, compare to `expected_node_id`.
- `load_cert_chain(path) / load_private_key(path)` — via `rustls-pemfile`.

### 5.2 `src/host/net/server.rs` — `run_incoming_server`
- If `cfg.mtls.enabled`:
  - bind `TcpListener` as now, but wrap with a `TlsAcceptor` from
    `load_server_config(&cfg.mtls)` (fail-fast: load error → `error!` + return, server does not
    start in a half-encrypted state).
  - `axum::serve(listener, app)` → wrap accept in `acceptor.accept(conn)` before handing to axum.
  - `handle_incoming` **unchanged** (Auth over wss works like over ws).
- If `!enabled` — current behavior (plain ws).
- Client config URL with mTLS enabled must be `wss://` (documented).

### 5.3 `src/host/net/outbound.rs` — `run_outbound_loop`
- If `remote.mtls.enabled` (or `cfg.mtls.enabled` as fallback):
  - `let client_cfg = load_client_config(&mtls);`
  - `tokio_tungstenite::connect_async_tls_with_config(
       &remote.url, None, None, Some(Arc::new(client_cfg))).await`
    — `Connector::Rustls` is selected automatically when `Some(client_config)` is passed.
  - when `require_node_id_in_san`: after handshake, extract the server cert (tungstenite
    `Stream` → `rustls` peer cert) and call `verify_peer_node_id(cert, expected_node_id)`
    where `expected_node_id` comes from the `remote.url` host or from the `Auth` handshake (B1).
- If `!enabled` — current `connect_async` (plain ws).

### 5.4 `Cargo.toml`
- Add dependencies/features from §3.

### 5.5 `src/config/config.rs`
- Add `MtlsConfig` and `mtls` field to `NetConfig`/`NetRemote` (§4).

## 6. fail-closed semantics

- `mtls.enabled == true`, but files (`ca_cert`/`cert`/`key`) missing/invalid →
  `start_net` (or `run_incoming_server`/`run_outbound_loop`) **panics/fail-fast** with an error.
  The network does not start in a half-encrypted state.
- One side requires mTLS, the other sends plain ws → TLS handshake fails → connection not
  established. **No plaintext fallback.**
- Invalid/foreign-CA peer cert → handshake error (both sides reject).
- `require_node_id_in_san == true` and SAN≠node_id → verifier rejects (even with a valid cert
  from our CA).

## 7. Tests (every change covered)

### 7.1 `src/host/net/tls.rs` (unit)
- `load_server_config`/`load_client_config` build successfully from in-test (via `rcgen`)
  self-signed CA + node certs.
- `verify_peer_node_id`:
  - cert with SAN == "node-A" → `true` for expected "node-A";
  - cert with SAN == "node-A" → `false` for expected "node-B".
- **Negative**: client with cert from a **foreign** CA → in integration test `connect_async_tls`
  fails (handshake error).

### 7.2 Integration (like `b1_auth_tests`)
- Start `run_incoming_server` with `mtls.enabled`; client with a **valid** cert from our CA
  successfully sends `Auth` + `Capabilities` → node registered.
- Client **without** TLS (plain ws) → connection rejected, node **not** registered.
- (with `require_node_id_in_san`) client with valid CA cert but SAN≠node_id → rejected.

### 7.3 Regression
- `mtls` unset (`None`/enabled=false) → behavior identical to current (existing 123 tests stay
  green; add explicit test "plain ws without mtls still works").

## 8. Risks and open points

- **rustls version conflict**: verified — `rustls 0.23` already in lock, compatible with
  `tokio-tungstenite 0.24`'s `rustls-tls`. Low risk.
- **Performance**: TLS handshake on connection setup (one-time) + encryption in hot-path.
  Acceptable for mesh (not high-frequency new connections). If needed — session resumption.
- **Cert distribution**: out of code scope — operational task (how nodes obtain certs from the
  internal CA). Code only *consumes* ready files at paths.
- **SAN ↔ node_id with `require_node_id_in_san`**: requires the internal CA to issue certs with
  SAN == node's `node_id`. This is an issuance convention (documented in PLUGIN-API/README).

## 9. Implementation order

1. `Cargo.toml` — add deps + feature.
2. `config.rs` — `MtlsConfig`, fields in `NetConfig`/`NetRemote`.
3. `tls.rs` — `load_*_config`, `verify_peer_node_id`, PEM parsers. Unit tests (§7.1).
4. `server.rs` — `TlsAcceptor` when `mtls.enabled` (§5.2).
5. `outbound.rs` — `connect_async_tls_with_config` when `mtls.enabled` (§5.3).
6. Integration tests (§7.2, §7.3).
7. `cargo test` in `nix develop` → green, 0 warnings.

## 10. Style note (project convention)

- All TLS checks — pure functions in `tls.rs`, covered by unit tests (like `can_plugin_*`).
- fail-closed at trust boundary (like console/network/B1).
- Backward compatible: `mtls` disabled by default (`#[serde(default)]`).
