pub mod console;
pub mod http_server;
pub mod log;
pub mod mcp_transport;
pub mod net;
pub mod ws_server;

/// Собрать `SocketAddr` из строки `bind` и порта.
///
/// B4: `bind` в конфиге слушателя — это адрес привязки, а НЕ готовый `ip:port`
/// (в документации `bind: "127.0.0.1"`, дефолт `"0.0.0.0"`). Раньше строка
/// разбиралась напрямую как `SocketAddr`, поэтому и дефолт, и документированное
/// значение давали ошибку разбора и TCP-слушатель не поднимался вовсе — ни у
/// HTTP-, ни у WS-фронта.
///
/// Принимает `"0.0.0.0"`, `"127.0.0.1"`, `"::1"`, `"[::1]"`, а также готовый
/// `"127.0.0.1:8080"` / `"[::1]:8080"` (он используется как есть).
pub(crate) fn resolve_bind_addr(
    bind: &str,
    port: u16,
) -> Result<std::net::SocketAddr, std::net::AddrParseError> {
    let bind = bind.trim();
    if let Ok(addr) = bind.parse::<std::net::SocketAddr>() {
        return Ok(addr);
    }
    if let Ok(addr) = format!("{}:{}", bind, port).parse::<std::net::SocketAddr>() {
        return Ok(addr);
    }
    let bare = bind.trim_matches(['[', ']']);
    format!("[{}]:{}", bare, port).parse::<std::net::SocketAddr>()
}

#[cfg(test)]
mod tests {
    use super::resolve_bind_addr;

    #[test]
    fn resolve_bind_addr_bare_ip_and_default() {
        assert_eq!(
            resolve_bind_addr("127.0.0.1", 8080).unwrap().to_string(),
            "127.0.0.1:8080"
        );
        assert_eq!(
            resolve_bind_addr("0.0.0.0", 18090).unwrap().to_string(),
            "0.0.0.0:18090"
        );
    }

    #[test]
    fn resolve_bind_addr_full_socket_addr_kept() {
        assert_eq!(
            resolve_bind_addr("127.0.0.1:5000", 8080).unwrap().to_string(),
            "127.0.0.1:5000"
        );
    }

    #[test]
    fn resolve_bind_addr_ipv6() {
        assert_eq!(
            resolve_bind_addr("::1", 8080).unwrap().to_string(),
            "[::1]:8080"
        );
        assert_eq!(
            resolve_bind_addr("[::1]", 8080).unwrap().to_string(),
            "[::1]:8080"
        );
    }

    #[test]
    fn resolve_bind_addr_invalid() {
        assert!(resolve_bind_addr("не-адрес", 8080).is_err());
    }
}
