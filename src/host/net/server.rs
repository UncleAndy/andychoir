//! Входящий WS/mTLS-сервер и обработка входящих соединений от удалённых хостов.

use super::*;
use super::dedup::check_dedup;
use super::discovery::handle_hello;
use super::message::{pack_capabilities, unpack_message, value_to_event, NetMessage};
use super::tls;
use axum::extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade};
use axum::Router;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

/// Входящий WS-сервер на /net.
pub(crate) async fn run_incoming_server(inner: Arc<NetInner>) {
    let port = inner.cfg.listen_port;
    if port == 0 {
        info!("[Хост] Net-мост: входящий порт не задан (listen_port=0), пропускаю сервер.");
        return;
    }
    // mTLS: поднимаем защищённый сервер вместо plain (тот же порт).
    if inner.cfg.mtls.enabled {
        run_incoming_tls_server(inner).await;
        return;
    }
    let app = Router::new()
        .route(
            "/net",
            axum::routing::get({
                let inner = inner.clone();
                move |ws: WebSocketUpgrade| {
                    let inner = inner.clone();
                    async move { ws.on_upgrade(move |s| handle_incoming_axum(inner, s)) }
                }
            }),
        )
        .with_state(inner.clone());

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            info!("[Хост] Net-мост слушает 0.0.0.0:{} /net (plain ws)", port);
            if let Err(e) = axum::serve(listener, app).await {
                error!("[Хост] Net-сервер на :{} упал: {}", port, e);
            }
        }
        Err(e) => error!("[Хост] Не удалось занять net-порт {}: {}", port, e),
    }
}

/// Обработать входящее plain WS-соединение (axum).
pub(crate) async fn handle_incoming_axum(inner: Arc<NetInner>, socket: WebSocket) {
    let (mut ws_sink, mut ws_stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);

    let send_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_sink
                .send(AxumMessage::Text(msg.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut this_origin: Option<String> = None;
    // B1: состояние аутентификации. До валидного Auth не обрабатываем
    // Capabilities/Hello/Event. Если token пустой — открытый режим.
    let mut authed = inner.cfg.token.is_empty();
    while let Some(msg) = ws_stream.next().await {
        let Ok(AxumMessage::Text(text)) = msg else { continue };
        let Some(netmsg) = unpack_message(&text) else { continue };
        let drop = process_netmsg(inner.clone(), &mut this_origin, &mut authed, &out_tx, netmsg).await;
        if drop {
            break;
        }
    }
    send_task.abort();
    finish_incoming(inner, this_origin).await;
}

/// Входящий mTLS-сервер (wss://) поверх tokio-tungstenite + rustls.
async fn run_incoming_tls_server(inner: Arc<NetInner>) {
    let port = inner.cfg.listen_port;
    let server_cfg = match tls::load_server_config(&inner.cfg.mtls) {
        Ok(c) => c,
        Err(e) => {
            error!("[Хост] Net (mTLS): не удалось загрузить TLS-конфиг: {:#}", e);
            return;
        }
    };
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("[Хост] Net (mTLS): не удалось занять порт {}: {}", port, e);
            return;
        }
    };
    info!("[Хост] Net-мост слушает 0.0.0.0:{} /net (mTLS wss)", port);

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                error!("[Хост] Net (mTLS): ошибка accept: {}", e);
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let inner = inner.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    warn!("[Хост] Net (mTLS): TLS-handshake не удался: {}", e);
                    return;
                }
            };
            let ws = match tokio_tungstenite::accept_async(tls_stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    warn!("[Хост] Net (mTLS): WS-рукопожатие не удалось: {}", e);
                    return;
                }
            };
            handle_incoming_tls(inner, ws).await;
        });
    }
}

/// Обработать входящее mTLS-соединение (tungstenite WebSocketStream).
async fn handle_incoming_tls(
    inner: Arc<NetInner>,
    ws: tokio_tungstenite::WebSocketStream<tokio_rustls::server::TlsStream<TcpStream>>,
) {
    let (mut ws_sink, mut ws_stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);

    let send_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_sink
                .send(tokio_tungstenite::tungstenite::Message::Text(msg.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut this_origin: Option<String> = None;
    let mut authed = inner.cfg.token.is_empty();
    while let Some(msg) = ws_stream.next().await {
        let Ok(tokio_tungstenite::tungstenite::Message::Text(text)) = msg else { continue };
        let Some(netmsg) = unpack_message(&text) else { continue };
        let drop = process_netmsg(inner.clone(), &mut this_origin, &mut authed, &out_tx, netmsg).await;
        if drop {
            break;
        }
    }
    send_task.abort();
    finish_incoming(inner, this_origin).await;
}

/// Общая логика обработки одного NetMessage (оба пути: axum и mTLS).
/// Возвращает `true`, если соединение нужно разорвать (неверный токен).
async fn process_netmsg(
    inner: Arc<NetInner>,
    this_origin: &mut Option<String>,
    authed: &mut bool,
    out_tx: &mpsc::Sender<String>,
    netmsg: NetMessage,
) -> bool {
    match netmsg {
        // B1: первое сообщение должно быть Auth. Сверяем с NetConfig.token.
        NetMessage::Auth { token } => {
            if inner.cfg.token.iter().any(|t| t == &token) {
                *authed = true;
                info!("[Хост] Net: входящее соединение аутентифицировано (токен совпал).");
                false
            } else {
                error!("[Хост] Net: отклонено соединение — неверный токен аутентификации.");
                let _ = out_tx.try_send("{\"error\":\"auth failed\"}".to_string());
                true // разорвать
            }
        }
        // B1: любое не-Auth сообщение до аутентификации — отбрасываем.
        _ if !*authed => {
            warn!("[Хост] Net: проигнорировано сообщение до аутентификации (Auth требуется первым).");
            false
        }
        NetMessage::Capabilities { origin, source_id: _, neighbors: _, tools } => {
            if origin == inner.cfg.node_id {
                return false;
            }
            info!("[Хост] Net: получены возможности хоста {}: {:?} инструментов", origin, tools.len());
            inner.origin_tools.write().await.insert(origin.clone(), tools);
            inner.node_url.write().await.insert(origin.clone(), "incoming".to_string());
            inner.incoming_senders.write().await.insert(origin.clone(), out_tx.clone());
            *this_origin = Some(origin);
            // Отвечаем своими возможностями.
            let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
            let caps = pack_capabilities(&inner.cfg.node_id, my_tools);
            let _ = out_tx.try_send(caps);
            false
        }
        NetMessage::Hello { source_id, seq, neighbors, tools } => {
            handle_hello(&inner, source_id, neighbors, tools, seq).await;
            false
        }
        NetMessage::Bye { source_id } => {
            super::discovery::handle_bye(&inner, &source_id).await;
            false
        }
        NetMessage::Event { origin, source_id: _, event_id, ttl: _, hop: _hop, event } => {
            if origin == inner.cfg.node_id {
                return false;
            }
            if !check_dedup(&inner, &event_id).await {
                info!("[Хост] Net: дубликат события {} отброшен (dedup)", event_id);
                return false;
            }
            let Some(ev) = value_to_event(&event) else { return false };
            inner
                .session_origin
                .write()
                .await
                .insert(ev.session_id.clone(), origin.clone());
            if let Some(tools) = inner.origin_tools.read().await.get(&origin).cloned() {
                crate::plugin::engine::add_session_tools(&ev.session_id, tools).await;
            }
            if inner.tx.send(ev).await.is_err() {
                return false;
            }
            false
        }
    }
}

/// Завершение входящего соединения: убрать обратный канал.
async fn finish_incoming(inner: Arc<NetInner>, this_origin: Option<String>) {
    if let Some(origin) = this_origin {
        inner.incoming_senders.write().await.remove(&origin);
    }
    info!("[Хост] Net: входящее соединение закрыто");
}

#[cfg(test)]
mod b1_auth_tests {
    use super::*;
    use crate::config::config::MtlsConfig;
    use crate::plugin::engine::ToolDef;
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    async fn free_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    }

    async fn spawn_server(cfg: NetConfig) -> Arc<NetInner> {
        let inner = NetInner::new_test_with_cfg(cfg);
        let inner_c = inner.clone();
        tokio::spawn(async move { run_incoming_server(inner_c).await });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        inner
    }

    async fn wait_registration(inner: &Arc<NetInner>, origin: &str, present: bool) -> bool {
        for _ in 0..50 {
            let has = inner.incoming_senders.read().await.contains_key(origin);
            if has == present {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        inner.incoming_senders.read().await.contains_key(origin) == present
    }

    fn caps_msg(origin: &str) -> NetMessage {
        NetMessage::Capabilities {
            origin: origin.to_string(),
            source_id: origin.to_string(),
            neighbors: vec![],
            tools: vec![ToolDef {
                name: "calculator".into(),
                description: "".into(),
                parameters_json: "{}".into(),
                session_local: false,
            }],
        }
    }

    // --- B1: аутентификация (plain ws) ---

    #[tokio::test]
    async fn b1_rejects_without_auth() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec!["sekret".to_string()];
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;
        let registered = wait_registration(&inner, "node-B", false).await;
        assert!(registered, "без Auth узел НЕ должен регистрироваться");
        assert!(!inner.origin_tools.read().await.contains_key("node-B"));
        let _ = ws.close(None).await;
    }

    #[tokio::test]
    async fn b1_rejects_wrong_token() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec!["sekret".to_string()];
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        let _ = ws
            .send(Message::Text(
                serde_json::to_string(&NetMessage::Auth { token: "wrong".to_string() })
                    .unwrap()
                    .into(),
            ))
            .await;
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;
        let registered = wait_registration(&inner, "node-B", false).await;
        assert!(registered, "при неверном токене узел НЕ должен регистрироваться");
    }

    #[tokio::test]
    async fn b1_accepts_valid_token() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec!["sekret".to_string()];
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        let _ = ws
            .send(Message::Text(
                serde_json::to_string(&NetMessage::Auth { token: "sekret".to_string() })
                    .unwrap()
                    .into(),
            ))
            .await;
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;
        let registered = wait_registration(&inner, "node-B", true).await;
        assert!(registered, "при верном токене узел ДОЛЖЕН регистрироваться");
        assert!(inner.origin_tools.read().await.contains_key("node-B"));
        let _ = ws.close(None).await;
    }

    #[tokio::test]
    async fn b1_open_mode_no_token_required() {
        let port = free_port().await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.token = vec![];
        let inner = spawn_server(cfg).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net"))
            .await
            .expect("connect");
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;
        let registered = wait_registration(&inner, "node-B", true).await;
        assert!(registered, "в открытом режиме узел ДОЛЖЕН регистрироваться без Auth");
        let _ = ws.close(None).await;
    }

    // --- mTLS: зашифрованное соединение ---

    /// Сгенерировать TLS-материалы во временном каталоге и вернуть MtlsConfig.
    /// Каталог — `tmp/` в корне проекта (CARGO_MANIFEST_DIR), чтобы не засорять
    /// системный /tmp и корень проекта в nix-shell.
    async fn temp_mtls(require_san: bool) -> (MtlsConfig, std::path::PathBuf) {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("mtls-it-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut m = MtlsConfig {
            enabled: true,
            dir: dir.to_string_lossy().to_string(),
            require_node_id_in_san: require_san,
            ..Default::default()
        };
        m.ca_cert = dir.join("ca.pem").to_string_lossy().to_string();
        m.cert = dir.join("node.pem").to_string_lossy().to_string();
        m.key = dir.join("node.key").to_string_lossy().to_string();
        let node_id = "00000000-0000-0000-0000-0000000000a1";
        super::super::tls::ensure_certificates(&m, node_id).expect("gen");
        (m, dir)
    }

    #[tokio::test]
    async fn mtls_accepts_valid_cert_and_registers() {
        let port = free_port().await;
        let (mtls, dir) = temp_mtls(false).await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.mtls = mtls.clone();
        let inner = spawn_server(cfg).await;

        // Клиент с валидным сертификатом от нашего CA.
        let client_cfg = tls::load_client_config(&mtls, None).expect("client cfg");
        let (mut ws, _resp) = tokio_tungstenite::connect_async_tls_with_config(
            format!("wss://127.0.0.1:{port}/net").as_str(),
            None,
            false,
            Some(tokio_tungstenite::Connector::Rustls(std::sync::Arc::new(client_cfg))),
        )
        .await
        .expect("wss connect");
        let _ = ws
            .send(Message::Text(serde_json::to_string(&caps_msg("node-B")).unwrap().into()))
            .await;
        let registered = wait_registration(&inner, "node-B", true).await;
        assert!(registered, "mTLS: валидный сертификат → узел регистрируется");
        let _ = ws.close(None).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn mtls_rejects_plain_ws() {
        let port = free_port().await;
        let (mtls, dir) = temp_mtls(false).await;
        let mut cfg = NetConfig::default();
        cfg.listen_port = port;
        cfg.mtls = mtls;
        let inner = spawn_server(cfg).await;

        // Plain ws (без TLS) → handshake провалится, узел не зарегистрируется.
        let res = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/net")).await;
        // Соединение либо не установится, либо сервер разорвёт при попытке WS поверх TLS.
        let _ = res;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !inner.incoming_senders.read().await.contains_key("node-B"),
            "mTLS-сервер не должен принимать plain ws"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
