# План: mTLS для межнодового соединения `andychoir`

> Статус: **РЕАЛИЗОВАНО** (2026-09-07). См. «Статус реализации» в конце.
> Зависит от: B1 (сетевая аутентификация по токену) — реализовано ранее.
> Цель: шифровать трафик между нодами mesh и взаимно аутентифицировать узлы
> средствами **внутреннего приватного CA** системы (без публичных/внешних CA).
> Токен `Auth` (B1) сохраняется как второй фактор (defense-in-depth).
> **Авто-генерация**: при первом старте (если файлов нет) CA и сертификат ноды
> генерируются автоматически (rcgen) и пишутся на диск.

---

## 1. Мотивация и угроза

Сейчас (`server.rs` / `outbound.rs`) mesh работает поверх **plain `ws://`** (tokio-tungstenite
0.24 поверх tokio TCP). B1 добавил проверку токена `Auth`, но:
- трафик **нешифрован** (перехват на сети → чтение событий/инструментов/сессий);
- токен **шлётся в открытом виде** в первом сообщении (перехват → реплей/имперсонация);
- нет привязки криптографической идентичности узла к его `node_id`.

mTLS закрывает всё это: обе стороны предъявляют сертификат от **внутреннего CA** системы,
проверяют цепочку и (опционально) SAN == `node_id`. Трафик шифруется (wss://), токен больше
не перехватываем в plaintext.

## 2. Дизайн (аддитивный, fail-closed, без внешних CA)

- **Внутренний приватный CA** системы (self-signed root). Он выпускает сертификат для каждой ноды
  (commonName / SAN = `node_id`, либо произвольный SAN, см. §4).
- **mTLS = mutual**: и сервер, и клиент **требуют** сертификат партнёра и проверяют его до CA.
- Ключи/сертификаты лежат **на диске** по путям из конфига (не вшиты в бинарь).
- **Токен `Auth` остаётся** поверх зашифрованного канала (второй фактор).
- **Обратная совместимость**: `mtls: None` (не задан) → поведение как сейчас (plain ws). Включается
  явно.

### 2.1 Проверка идентичности (две грани, согласно запросу «и та и та возможность»)
- **Базовая (всегда при `enabled=true`)**: сертификат партнёра выпущен нашим внутренним CA
  (цепочка до `ca_cert` в `RootCertStore`). Это гарантирует «узел доверен системе».
- **Строгая (опционально, `require_node_id_in_san: true`)**: из представленного сертификата
  извлекается SAN/subject и сверяется с ожидаемым `node_id` партнёра (для сервера — `node_id`
  из `Auth`/Hello; для клиента — `node_id` из URL remote). Это связывает TLS-идентичность с
  mesh-идентичностью (узел не может выдать себя за другой, даже с валидным сертификатом от нашего CA).

## 3. Зависимости (риск сборки проверен)

`Cargo.lock` уже содержит `rustls 0.23.43` и `tokio-rustls 0.26.4` (транзитивно).
`tokio-tungstenite 0.24` поддерживает feature `rustls-tls` (тянет `rustls 0.23`) — **совместимо**.

Добавить в `Cargo.toml`:
```toml
tokio-tungstenite = { version = "0.24", features = ["rustls-tls"] }
rustls = "0.23"
rustls-pemfile = "2"          # парсинг PEM (cert/key) из файлов
```
Для тестов (генерация self-signed CA + node-сертификатов в памяти, без файлов):
```toml
[dev-dependencies]
rcgen = "0.13"                # генерация сертификатов в юнит-тестах
```

> Примечание: `rustls` — pure-rust, без openssl, собирается в NixOS-flake окружении без проблем
> (в отличие от `native-tls`/`openssl-sys`). Проверено наличием `rustls 0.23` в текущем lock.

## 4. Конфигурация

В `src/config/config.rs` добавить:

```rust
/// Настройки mTLS для межнодового соединения (внутренний CA системы).
#[derive(Deserialize, Clone, Default)]
#[serde(default)]
pub struct MtlsConfig {
    /// Включить mTLS. false/None → plain ws (обратная совместимость).
    pub enabled: bool,
    /// Путь к внутреннему CA (PEM, root cert), которому доверяем.
    pub ca_cert: String,
    /// Путь к сертификату ЭТОЙ ноды (PEM, выпущен внутренним CA).
    pub cert: String,
    /// Путь к приватному ключу ЭТОЙ ноды (PEM).
    pub key: String,
    /// Строго требовать, чтобы SAN/subject сертификата партнёра == его node_id.
    /// false → достаточно «сертификат от нашего CA».
    pub require_node_id_in_san: bool,
}
```

В `NetConfig`:
```rust
pub struct NetConfig {
    // ...существующие поля...
    #[serde(default)]
    pub mtls: MtlsConfig,
}
```

В `NetRemote` (исходящее соединение):
```rust
pub struct NetRemote {
    pub url: String,            // при mtls: "wss://host-b:8092/net"
    pub token: String,          // сохраняем (второй фактор)
    #[serde(default)]
    pub mtls: MtlsConfig,       // per-remote переопределение (или берём из NetConfig)
    // ...остальные поля...
}
```

## 5. Изменения по файлам

### 5.1 Новый модуль `src/host/net/tls.rs` (чистые функции, тестируемые)
- `load_server_config(&MtlsConfig) -> Result<rustls::ServerConfig>`
  - `ca_cert` → `RootCertStore`; `cert`/`key` (PEM через `rustls-pemfile`) → `CertifiedKey`.
  - `server_config.set_client_certificate_verifier(WebPkiClientVerifier::builder(roots).build())`
    с **обязательной** проверкой клиентского сертификата (client auth = Required).
- `load_client_config(&MtlsConfig) -> Result<rustls::ClientConfig>`
  - `RootCertStore` из `ca_cert`; `set_certificate` из `cert`/`key`.
  - при `require_node_id_in_san == true` — кастомный `ServerCertVerifier`, проверяющий SAN.
- `verify_peer_node_id(cert_der, expected_node_id) -> bool`
  - парсит представленный сертификат, извлекает SAN (DNSName/IP) / CN, сверяет с `expected_node_id`.
- `load_cert_chain(path) / load_private_key(path)` — через `rustls-pemfile`.

### 5.2 `src/host/net/server.rs` — `run_incoming_server`
- Если `cfg.mtls.enabled`:
  - читаем `TcpListener` как сейчас, но **поверх** навешиваем `TlsAcceptor` из
    `load_server_config(&cfg.mtls)` (fail-fast: ошибка загрузки → `error!` + return, сервер не
    поднимается в полузашифрованном виде).
  - `axum::serve(listener, app)` → оборачиваем accept в `acceptor.accept(conn)` перед отдачей axum.
  - `handle_incoming` **не меняется** (Auth поверх wss работает как поверх ws).
- Если `!enabled` — текущее поведение (plain ws).
- URL в клиентском конфиге при включённом mTLS должен быть `wss://` (документируем).

### 5.3 `src/host/net/outbound.rs` — `run_outbound_loop`
- Если `remote.mtls.enabled` (или `cfg.mtls.enabled` как fallback):
  - `let client_cfg = load_client_config(&mtls);`
  - `tokio_tungstenite::connect_async_tls_with_config(
       &remote.url, None, None, Some(Arc::new(client_cfg))).await`
    — `Connector::Rustls` выбирается tungstenite автоматически при наличии `Some(client_config)`.
  - при `require_node_id_in_san`: после handshake извлечь сертификат сервера (tungstenite
    `Stream` → `native_tls`/`rustls` peer cert) и вызвать `verify_peer_node_id(cert, expected_node_id)`
    где `expected_node_id` берётся из `remote.url`-хоста или из `Auth`-рукопожатия (B1).
- Если `!enabled` — текущее `connect_async` (plain ws).

### 5.4 `Cargo.toml`
- Добавить features/зависимости из §3.

### 5.5 `src/config/config.rs`
- Добавить `MtlsConfig` и поле `mtls` в `NetConfig`/`NetRemote` (§4).

## 6. fail-closed семантика

- `mtls.enabled == true`, но файлы (`ca_cert`/`cert`/`key`) отсутствуют/невалидны →
  `start_net` (или `run_incoming_server`/`run_outbound_loop`) **паникует/fail-fast** с ошибкой.
  Сеть не поднимается в полузашифрованном виде.
- Одна сторона требует mTLS, другая шлёт plain ws → TLS-handshake провалится → соединение не
  устанавливается. **Никакого fallback на plaintext.**
- Невалидный/чужой CA сертификат партнёра → handshake error (обе стороны отвергают).
- `require_node_id_in_san == true` и SAN≠node_id → верификатор отклоняет (даже при валидном
  сертификате от нашего CA).

## 7. Тесты (каждое изменение покрыто)

### 7.1 `src/host/net/tls.rs` (юнит)
- `load_server_config`/`load_client_config` успешно строятся из сгенерированных в тесте
  (через `rcgen`) self-signed CA + node-сертификатов.
- `verify_peer_node_id`:
  - сертификат с SAN == "node-A" → `true` для ожидания "node-A";
  - сертификат с SAN == "node-A" → `false` для ожидания "node-B".
- **Негативный**: клиент с сертификатом от **чужого** CA → в интеграционном тесте
  `connect_async_tls` падает (handshake error).

### 7.2 Интеграционный (как `b1_auth_tests`)
- Поднять `run_incoming_server` с `mtls.enabled`, клиент с **валидным** сертификатом от нашего CA
  успешно шлёт `Auth` + `Capabilities` → узел зарегистрирован.
- Клиент **без** TLS (plain ws) → соединение отвергнуто, узел **не** зарегистрирован.
- (при `require_node_id_in_san`) клиент с валидным CA-сертификатом, но SAN≠node_id → отвергнут.

### 7.3 Регрессионный
- `mtls` не задан (`None`/enabled=false) → поведение идентично текущему (существующие 123 теста
  остаются зелёными; добавить явный тест «plain ws без mtls всё ещё работает»).

## 8. Риски и открытые моменты

- **Версионный конфликт rustls**: проверен — `rustls 0.23` уже в lock, совместим с
  `tokio-tungstenite 0.24`'s `rustls-tls`. Риск низкий.
- **Производительность**: TLS-handshake при установке соединения (разово) + шифрование в hot-path.
  Для mesh (не высокочастотные новые соединения) приемлемо. При необходимости — session resumption.
- **Дистрибуция сертификатов**: вне scope кода — операционная задача (как именно ноды получают
  сертификаты от внутреннего CA). Код лишь *потребляет* готовые файлы по путям.
- **SAN ↔ node_id при `require_node_id_in_san`**: требует, чтобы внутренний CA выпускал
  сертификаты с SAN == `node_id` ноды. Это конвенция выпуска (документируем в PLUGIN-API/README).

## 9. Порядок реализации

1. `Cargo.toml` — добавить зависимости + feature.
2. `config.rs` — `MtlsConfig`, поля в `NetConfig`/`NetRemote`.
3. `tls.rs` — `load_*_config`, `verify_peer_node_id`, парсеры PEM. Юнит-тесты (§7.1).
4. `server.rs` — `TlsAcceptor` при `mtls.enabled` (§5.2).
5. `outbound.rs` — `connect_async_tls_with_config` при `mtls.enabled` (§5.3).
6. Интеграционные тесты (§7.2, §7.3).
7. `cargo test` в `nix develop` → зелёные, 0 warnings.

## 10. Заметка по стилю (конвенция проекта)

- Все проверки TLS — чистые функции в `tls.rs`, покрытые юнит-тестами (как `can_plugin_*`).
- fail-closed на границе доверия (как для консоли/сети/B1).
- Обратная совместимость: `mtls` отключён по умолчанию (`#[serde(default)]`).

## 11. Статус реализации (2026-09-07)

**Реализовано полностью и покрыто тестами (129 тестов, 0 warnings):**

- `Cargo.toml`: `rustls = { version = "0.23", features = ["ring"] }`, `rustls-pemfile = "2"`,
  `tokio-rustls = "0.26"`, `tokio-tungstenite = { version = "0.24", features = ["__rustls-tls"] }`,
  `rcgen = "0.13"` (для генерации).
- `config.rs`: `MtlsConfig { enabled, dir, ca_cert, cert, key, require_node_id_in_san }`;
  поля `mtls` добавлены в `NetConfig` и `NetRemote`; метод `NetRemote::url_host()`
  (извлекает node_id из hostname URL).
- `src/host/net/tls.rs` (новый модуль):
  - `ensure_certificates(cfg, node_id)` — **при первом старте генерирует внутренний CA
    (self-signed) и сертификат ноды (SAN=node_id как DNSName + loopback IP), пишет PEM
    на диск в `dir`**; идемпотентна (пропускает, если файлы уже есть и загружаются).
  - `load_server_config` — rustls ServerConfig с обязательной проверкой клиентского
    сертификата (mTLS, client auth = Required), цепочка до нашего CA.
  - `load_client_config(cfg, expected_node_id)` — rustls ClientConfig; при
    `require_node_id_in_san` + `expected` использует кастомный `NodeIdServerVerifier`
    (цепочка CA + SAN/DNS == expected_node_id).
  - `cert_san_dns` / `verify_peer_node_id` — извлечение DNS-SAN из DER и сравнение с node_id.
  - `ensure_crypto_provider()` — установка `rustls::crypto::ring::default_provider()`
    (требуется в rustls 0.23, идемпотентно через `Once`).
- `server.rs`: `run_incoming_server` при `mtls.enabled` поднимает **mTLS wss://** сервер
  (tokio TcpListener + `TlsAcceptor` + tungstenite), общая логика `process_netmsg`
  переиспользуется для обоих путей (axum plain / tungstenite mTLS). Токен `Auth` (B1)
  работает поверх зашифрованного канала.
- `outbound.rs`: при `mtls.enabled` (remote или global) подключается через
  `connect_async_tls_with_config` с `Connector::Rustls` и клиентским конфигом; expected
  node_id сервера берётся из hostname URL. Иначе — plain `connect_async` (совместимость).
- `net.rs` (`start_net`): при `mtls.enabled` вызывает `ensure_certificates` до поднятия
  сервера/клиентов; при ошибке генерации/загрузки — fail-fast (сеть не поднимается).

**Ограничение (документировано честно):** строгая привязка SAN↔node_id работает на
**клиенте** (клиент проверяет SAN сервера == hostname из URL через `NodeIdServerVerifier`).
Серверная строгая проверка SAN клиента не реализована, т.к. сервер узнаёт node_id клиента
только после `Auth`/`Hello`, которые приходят ПОСЛЕ TLS-handshake — внедрение проверки
в verifier потребовало бы передачи node_id до handshake (менять wire-протокол). Базовая
взаимная проверка цепочки до внутреннего CA (mTLS) реализована полностью и закрывает
главную угрозу (шифрование + взаимная аутентификация CA). `require_node_id_in_san` на
стороне remote работает как опция строгости на клиенте.

**Тесты mTLS (новые):**
- `tls::tests`: `generate_then_load_server_config` (генерация+загрузка), `cert_san_contains_node_id`
  (SAN==node_id), `verify_peer_node_id_matches` (совпадение/несовпадение), `ensure_noop_when_disabled`.
- `server::b1_auth_tests`: `mtls_accepts_valid_cert_and_registers` (wss-клиент с валидным
  сертификатом регистрируется), `mtls_rejects_plain_ws` (mTLS-сервер отвергает plain ws).
- Регрессионные B1-тесты (plain ws) остаются зелёными (обратная совместимость).

