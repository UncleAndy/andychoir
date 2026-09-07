//! mTLS для межнодового соединения (B1+): внутренний приватный CA системы.
//!
//! Обе стороны (сервер и клиент) предъявляют сертификат, выпущенный
//! внутренним CA, и взаимно проверяют цепочку (это делает сам TLS-handshake
//! через `rustls` verifier). Трафик шифруется (wss://), токен `Auth` (B1)
//! остаётся вторым фактором поверх канала.
//!
//! Сертификаты хранятся на диске. При первом старте (если файлов нет)
//! CA и сертификат ноды генерируются автоматически (см. `ensure_certificates`).
//!
//! Используется pure-rust `rustls` (без openssl) — собирается в NixOS-flake.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::config::config::MtlsConfig;
use crate::{info, warn};

/// Гарантировать, что rustls crypto-provider установлен (требуется в 0.23).
/// Идемпотентно через Once.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::ring::default_provider(),
        );
    });
}

/// Разрешить путь к файлу: если задан явно — он, иначе `<dir>/<fallback>`.
fn resolve_path(cfg: &MtlsConfig, explicit: &str, fallback: &str) -> String {
    if !explicit.is_empty() {
        explicit.to_string()
    } else {
        let dir = if cfg.dir.is_empty() {
            "tls".to_string()
        } else {
            cfg.dir.clone()
        };
        format!("{}/{}", dir.trim_end_matches('/'), fallback)
    }
}

/// Путь к CA (PEM).
pub(crate) fn ca_cert_path(cfg: &MtlsConfig) -> String {
    resolve_path(cfg, &cfg.ca_cert, "ca.pem")
}

/// Путь к сертификату ноды (PEM).
pub(crate) fn node_cert_path(cfg: &MtlsConfig) -> String {
    resolve_path(cfg, &cfg.cert, "node.pem")
}

/// Путь к приватному ключу ноды (PEM).
pub(crate) fn node_key_path(cfg: &MtlsConfig) -> String {
    resolve_path(cfg, &cfg.key, "node.key")
}

/// Прочитать цепочку сертификатов (PEM) из файла.
fn load_cert_chain(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let pem = std::fs::read(path).with_context(|| format!("чтение сертификата {path}"))?;
    let mut certs = Vec::new();
    for item in rustls_pemfile::read_all(&mut &pem[..]) {
        let item = item.with_context(|| format!("парсинг PEM {path}"))?;
        if let rustls_pemfile::Item::X509Certificate(c) = item {
            certs.push(c);
        }
    }
    if certs.is_empty() {
        return Err(anyhow!("в {path} не найдено ни одного сертификата (X509)"));
    }
    Ok(certs)
}

/// Прочитать приватный ключ (PEM: Pkcs1/Sec1/Pkcs8) из файла.
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let pem = std::fs::read(path).with_context(|| format!("чтение приватного ключа {path}"))?;
    for item in rustls_pemfile::read_all(&mut &pem[..]) {
        let item = item.with_context(|| format!("парсинг PEM ключа {path}"))?;
        match item {
            rustls_pemfile::Item::Pkcs8Key(k) => return Ok(PrivateKeyDer::Pkcs8(k)),
            rustls_pemfile::Item::Sec1Key(k) => return Ok(PrivateKeyDer::Sec1(k)),
            rustls_pemfile::Item::Pkcs1Key(k) => return Ok(PrivateKeyDer::Pkcs1(k)),
            _ => {}
        }
    }
    Err(anyhow!("в {path} не найден приватный ключ"))
}

/// Построить `RootCertStore` из файла CA.
fn load_root_store(ca_path: &str) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for c in load_cert_chain(ca_path)? {
        roots.add(c).with_context(|| format!("добавление CA {ca_path} в RootCertStore"))?;
    }
    Ok(roots)
}

/// Серверный конфиг: требует обязательной проверки клиентского сертификата
/// (mTLS). Клиент должен представить сертификат, валидный до нашего CA.
pub(crate) fn load_server_config(cfg: &MtlsConfig) -> Result<ServerConfig> {
    ensure_crypto_provider();
    let ca_path = ca_cert_path(cfg);
    let cert_path = node_cert_path(cfg);
    let key_path = node_key_path(cfg);

    let roots = load_root_store(&ca_path)?;
    let cert_chain = load_cert_chain(&cert_path)?;
    let key = load_private_key(&key_path)?;

    // Обязательная проверка клиентского сертификата (client auth = Required).
    // WebPkiClientVerifier::builder(...).build() уже возвращает Arc<WebPkiClientVerifier>.
    let verifier = WebPkiClientVerifier::builder(roots.into()).build()?;

    let mut sc = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, key)
        .context("сборка ServerConfig")?;
    sc.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(sc)
}

/// Клиентский конфиг: проверяет серверный сертификат до нашего CA.
/// Если `expected_node_id` задан и `require_node_id_in_san` — доп. проверка,
/// что SAN/CN сервера == expected_node_id (обычно hostname из URL remote).
pub(crate) fn load_client_config(
    cfg: &MtlsConfig,
    expected_node_id: Option<&str>,
) -> Result<ClientConfig> {
    ensure_crypto_provider();
    let ca_path = ca_cert_path(cfg);
    let cert_path = node_cert_path(cfg);
    let key_path = node_key_path(cfg);

    let roots = load_root_store(&ca_path)?;
    let cert_chain = load_cert_chain(&cert_path)?;
    let key = load_private_key(&key_path)?;

    let builder = ClientConfig::builder();
    let mut cc = if cfg.require_node_id_in_san {
        match expected_node_id {
            Some(expected) => {
                let verifier: Arc<dyn rustls::client::danger::ServerCertVerifier> =
                    Arc::new(NodeIdServerVerifier::new(load_root_store(&ca_path)?, expected));
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(verifier)
                    .with_client_auth_cert(cert_chain, key)
                    .context("сборка ClientConfig (custom verifier)")?
            }
            None => {
                return Err(anyhow!(
                    "require_node_id_in_san включён, но node_id сервера не задан (hostname в URL)"
                ));
            }
        }
    } else {
        builder
            .with_root_certificates(roots)
            .with_client_auth_cert(cert_chain, key)
            .context("сборка ClientConfig (client auth)")?
    };
    cc.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(cc)
}

/// Извлечь первый DNS SAN из сертификата (для проверки node_id).
/// Ищем OID 2.5.29.17 (subjectAltName), внутри — entry с тегом 0x82 (DNSName).
fn cert_san_dns(cert_der: &CertificateDer) -> Option<String> {
    let bytes = &cert_der[..];
    // OID 2.5.29.17 = 55 1d 11
    let oid = [0x06, 0x03, 0x55, 0x1d, 0x11];
    let mut i = 0;
    while i + oid.len() <= bytes.len() {
        if &bytes[i..i + oid.len()] == oid {
            // Дальше: SEQUENCE (0x30), затем OCTET STRING (0x04), длина, данные SAN.
            let mut j = i + oid.len();
            // пропускаем SEQUENCE-обёртку SAN (может быть 0x30 или сразу 0x04).
            while j < bytes.len() && bytes[j] != 0x04 {
                j += 1;
            }
            if j >= bytes.len() {
                break;
            }
            // 0x04 (OCTET STRING), длина, данные.
            let len = bytes[j + 1] as usize;
            let start = j + 2;
            if start + len > bytes.len() {
                break;
            }
            let san = &bytes[start..start + len];
            // Внутри ищем DNSName (context tag 0x82).
            let mut k = 0;
            while k + 2 <= san.len() {
                if san[k] == 0x82 {
                    let dlen = san[k + 1] as usize;
                    let dstart = k + 2;
                    if dstart + dlen <= san.len() {
                        if let Ok(s) = std::str::from_utf8(&san[dstart..dstart + dlen]) {
                            return Some(s.to_string());
                        }
                    }
                }
                k += 1;
            }
            return None;
        }
        i += 1;
    }
    None
}

/// Проверить, что SAN (DNS) сертификата == expected_node_id (используется verifier-ом).
fn verify_peer_node_id(peer_cert_der: &CertificateDer, expected_node_id: &str) -> bool {
    match cert_san_dns(peer_cert_der) {
        Some(san) => san == expected_node_id,
        None => false,
    }
}

/// Кастомный верификатор сервера: цепочка до CA + SAN/CN == expected.
/// Делегирует проверку подписей стандартному `WebPkiServerVerifier`.
struct NodeIdServerVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    expected: String,
}

impl std::fmt::Debug for NodeIdServerVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdServerVerifier")
            .field("expected", &self.expected)
            .finish()
    }
}

impl NodeIdServerVerifier {
    fn new(roots: RootCertStore, expected: &str) -> Self {
        let inner = rustls::client::WebPkiServerVerifier::builder(roots.into())
            .build()
            .expect("WebPkiServerVerifier");
        Self {
            inner,
            expected: expected.to_string(),
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for NodeIdServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // 1) Цепочка до нашего CA.
        self.inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)?;
        // 2) SAN/CN == expected.
        if verify_peer_node_id(end_entity, &self.expected) {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "сертификат сервера: SAN '{}' != ожидаемый node_id '{}'",
                cert_san_dns(end_entity).unwrap_or_default(),
                self.expected
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dsig: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dsig)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dsig: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dsig)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

// ---------------------------------------------------------------------------
// Генерация внутреннего CA + сертификата ноды (при первом старте).
// ---------------------------------------------------------------------------

/// Убедиться, что TLS-файлы присутствуют. Если отсутствуют — сгенерировать
/// внутренний CA и сертификат ноды (CN/SAN = node_id) и записать на диск.
///
/// Идемпотентно: если все три файла уже существуют и загружаются — ничего
/// не делает. При ошибке — возвращает Err (fail-fast: сеть не поднимается
/// в полузашифрованном виде).
pub(crate) fn ensure_certificates(cfg: &MtlsConfig, node_id: &str) -> Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    ensure_crypto_provider();
    let ca = ca_cert_path(cfg);
    let cert = node_cert_path(cfg);
    let key = node_key_path(cfg);

    if std::path::Path::new(&ca).exists()
        && std::path::Path::new(&cert).exists()
        && std::path::Path::new(&key).exists()
    {
        if load_server_config(cfg).is_ok() {
            info!("[mTLS] Сертификаты уже присутствуют: {}", cert);
            return Ok(());
        }
        warn!("[mTLS] Файлы сертификатов найдены, но не загружаются — перегенерирую.");
    }

    info!("[mTLS] Генерация внутреннего CA и сертификата ноды {} ...", node_id);
    generate_and_write(&ca, &cert, &key, node_id).context("генерация TLS-материалов")?;
    info!("[mTLS] Сертификаты записаны: {}", cert);
    Ok(())
}

/// Сгенерировать self-signed CA + сертификат ноды (подписан CA) и записать PEM.
fn generate_and_write(ca_path: &str, cert_path: &str, key_path: &str, node_id: &str) -> Result<()> {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType,
    };
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    // CA: self-signed root.
    let ca_kp = KeyPair::generate().map_err(|e| anyhow!("генерация CA key: {e}"))?;
    let mut ca_params = CertificateParams::new(vec![format!("andychoir-internal-ca-{}", node_id)])
        .map_err(|e| anyhow!("CA params: {e}"))?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_cert = ca_params
        .self_signed(&ca_kp)
        .map_err(|e| anyhow!("self-sign CA: {e}"))?;

    // Node cert: подписан CA, SAN = node_id (DNS) + loopback.
    let node_kp = KeyPair::generate().map_err(|e| anyhow!("генерация node key: {e}"))?;
    let mut node_params = CertificateParams::new(vec![node_id.to_string()])
        .map_err(|e| anyhow!("node params: {e}"))?;
    node_params
        .distinguished_name
        .push(DnType::CommonName, node_id);
    node_params.subject_alt_names = vec![
        SanType::DnsName(node_id.to_string().try_into().expect("ia5")),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        SanType::IpAddress(IpAddr::V6(Ipv6Addr::LOCALHOST)),
    ];
    let node_cert = node_params
        .signed_by(&node_kp, &ca_cert, &ca_kp)
        .map_err(|e| anyhow!("подпись node CA: {e}"))?;

    if let Some(parent) = std::path::Path::new(cert_path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| format!("создание каталога {parent:?}"))?;
        }
    }

    std::fs::write(ca_path, ca_cert.pem()).with_context(|| format!("запись {ca_path}"))?;
    std::fs::write(cert_path, node_cert.pem()).with_context(|| format!("запись {cert_path}"))?;
    std::fs::write(key_path, node_kp.serialize_pem())
        .with_context(|| format!("запись {key_path}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Временный каталог для TLS-материалов: `tmp/` в корне проекта (CARGO_MANIFEST_DIR),
    /// чтобы не засорять системный /tmp и корень проекта в nix-shell.
    fn temp_cfg() -> (MtlsConfig, std::path::PathBuf) {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("mtls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = MtlsConfig {
            enabled: true,
            dir: dir.to_string_lossy().to_string(),
            ..Default::default()
        };
        cfg.ca_cert = dir.join("ca.pem").to_string_lossy().to_string();
        cfg.cert = dir.join("node.pem").to_string_lossy().to_string();
        cfg.key = dir.join("node.key").to_string_lossy().to_string();
        (cfg, dir)
    }

    #[test]
    fn generate_then_load_server_config() {
        let (cfg, dir) = temp_cfg();
        let node_id = "00000000-0000-0000-0000-0000000000a1";
        ensure_certificates(&cfg, node_id).expect("генерация");
        assert!(std::path::Path::new(&cfg.ca_cert).exists());
        assert!(std::path::Path::new(&cfg.cert).exists());
        assert!(std::path::Path::new(&cfg.key).exists());

        let sc = load_server_config(&cfg);
        assert!(sc.is_ok(), "server config: {:?}", sc.err());
        let cc = load_client_config(&cfg, None);
        assert!(cc.is_ok(), "client config: {:?}", cc.err());

        ensure_certificates(&cfg, node_id).expect("повтор — ок");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cert_san_contains_node_id() {
        let (cfg, dir) = temp_cfg();
        let node_id = "node-test-id-123";
        ensure_certificates(&cfg, node_id).unwrap();
        let certs = load_cert_chain(&cfg.cert).unwrap();
        let san = cert_san_dns(&certs[0]).expect("SAN извлечён");
        assert_eq!(san, node_id, "SAN (DNS) должен == node_id");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn verify_peer_node_id_matches() {
        let (cfg, dir) = temp_cfg();
        let node_id = "peer-node-1";
        ensure_certificates(&cfg, node_id).unwrap();
        let node = load_cert_chain(&cfg.cert).unwrap();
        assert!(verify_peer_node_id(&node[0], node_id), "CN==node_id → true");
        assert!(
            !verify_peer_node_id(&node[0], "other-node"),
            "чужой node_id → false"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_noop_when_disabled() {
        let (mut cfg, dir) = temp_cfg();
        cfg.enabled = false;
        ensure_certificates(&cfg, "x").expect("disabled → Ok");
        assert!(!std::path::Path::new(&cfg.cert).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
