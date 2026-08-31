//! Входящий WS-сервер и обработка входящих соединений от удалённых хостов.

use super::*;
use super::dedup::check_dedup;
use super::discovery::handle_hello;
use super::message::{pack_capabilities, unpack_message, value_to_event, NetMessage};
use axum::extract::ws::WebSocket;
use axum::Router;

/// Входящий WS-сервер на /net.
pub(crate) async fn run_incoming_server(inner: Arc<NetInner>) {
    let port = inner.cfg.listen_port;
    if port == 0 {
        info!("[Хост] Net-мост: входящий порт не задан (listen_port=0), пропускаю сервер.");
        return;
    }
    let app = Router::new()
        .route(
            "/net",
            axum::routing::get({
                let inner = inner.clone();
                move |ws: WebSocketUpgrade| {
                    let inner = inner.clone();
                    async move { ws.on_upgrade(move |s| handle_incoming(inner, s)) }
                }
            }),
        )
        .with_state(inner.clone());

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            info!("[Хост] Net-мост слушает 0.0.0.0:{} /net", port);
            if let Err(e) = axum::serve(listener, app).await {
                error!("[Хост] Net-сервер на :{} упал: {}", port, e);
            }
        }
        Err(e) => error!("[Хост] Не удалось занять net-порт {}: {}", port, e),
    }
}

/// Обработать входящее WS-соединение от удалённого хоста.
pub(crate) async fn handle_incoming(inner: Arc<NetInner>, socket: WebSocket) {
    info!("[Хост] Net: входящее соединение открыто");
    let (mut ws_sink, mut ws_stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);

    // Задача-отправитель ответов на это входящее соединение.
    let send_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_sink
                .send(axum::extract::ws::Message::Text(msg.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut this_origin: Option<String> = None;
    while let Some(msg) = ws_stream.next().await {
        let Ok(axum::extract::ws::Message::Text(text)) = msg else { continue };
        let Some(netmsg) = unpack_message(&text) else { continue };
        match netmsg {
            NetMessage::Capabilities { origin, source_id: _, neighbors: _, tools } => {
                if origin == inner.cfg.node_id {
                    continue;
                }
                info!("[Хост] Net: получены возможности хоста {}: {:?} инструментов", origin, tools.len());
                inner.origin_tools.write().await.insert(origin.clone(), tools);
                // P4: регистрируем node_id -> url для FIB-маршрутизации.
                inner.node_url.write().await.insert(origin.clone(), "incoming".to_string());
                // Регистрируем обратный канал для этого origin (П1).
                this_origin = Some(origin.clone());
                inner.incoming_senders.write().await.insert(origin, out_tx.clone());
                // Отвечаем своими возможностями (локальные инструменты), чтобы
                // удалённый хост узнал, что мы умеем.
                let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
                let caps = pack_capabilities(&inner.cfg.node_id, my_tools);
                let _ = out_tx.try_send(caps);
            }
            // P1: Hello (discovery).
            NetMessage::Hello { source_id, neighbors, tools } => {
                handle_hello(&inner, source_id, neighbors, tools).await;
            }
            // P1: Bye (graceful leave) — обрабатывается в P6.
            NetMessage::Bye { source_id } => {
                super::discovery::handle_bye(&inner, &source_id).await;
            }
            NetMessage::Event { origin, source_id: _, event_id, ttl: _, hop: _hop, event } => {
                if origin == inner.cfg.node_id {
                    continue;
                }
                // P2: dedup по event_id (Bloom-фильтр). Если дубликат — отбрасываем.
                if !check_dedup(&inner, &event_id).await {
                    info!("[Хост] Net: дубликат события {} отброшен (dedup)", event_id);
                    continue;
                }
                let Some(ev) = value_to_event(&event) else { continue };
                inner
                    .session_origin
                    .write()
                    .await
                    .insert(ev.session_id.clone(), origin.clone());
                if let Some(tools) = inner.origin_tools.read().await.get(&origin).cloned() {
                    crate::plugin::engine::add_session_tools(&ev.session_id, tools).await;
                }
                if inner.tx.send(ev).await.is_err() {
                    break;
                }
            }
        }
    }
    // Соединение закрылось: убрать обратный канал.
    if let Some(origin) = this_origin {
        inner.incoming_senders.write().await.remove(&origin);
    }
    send_task.abort();
    info!("[Хост] Net: входящее соединение закрыто");
}
