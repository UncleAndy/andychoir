//! Исходящие персистентные WS-соединения к remotes (с автопереподключением).

use super::*;
use super::dedup::check_dedup;
use super::discovery::handle_hello;
use super::message::{pack_capabilities, unpack_message, value_to_event, NetMessage};
use crate::config::config::{DEFAULT_CONNECT_RETRY_INTERVAL_SECS, DEFAULT_CONNECT_TIMEOUT_SECS, MtlsConfig, NetRemote};
use tokio_tungstenite;
use rustls::ClientConfig;
use tokio_tungstenite::Connector;

/// Разрешить mTLS-конфиг для исходящего соединения: приоритет — `remote.mtls`,
/// затем `inner.cfg.mtls`. Возвращает Some(client_cfg), если mTLS включён.
fn resolve_outbound_mtls(remote: &NetRemote, cfg: &MtlsConfig) -> Option<Arc<ClientConfig>> {
    let m = if remote.mtls.enabled { &remote.mtls } else { cfg };
    if !m.enabled {
        return None;
    }
    // expected node_id сервера = hostname из URL (договор: wss://<node_id>:port/net).
    let expected = remote.url_host();
    match super::tls::load_client_config(m, expected.as_deref()) {
        Ok(c) => Some(std::sync::Arc::new(c)),
        Err(e) => {
            error!("[Хост] Net: не удалось загрузить mTLS-клиент для {}: {:#}", remote.url, e);
            None
        }
    }
}

/// Исходящее персистентное соединение к одному remote (с автопереподключением).
///
/// Поведение retry настраивается через конфиг:
/// - `retry_interval_secs` (remote) / `connect_retry_interval_secs` (global) —
///   пауза между попытками (default 10с).
/// - `connect_timeout_secs` (remote) / `connect_timeout_secs` (global) —
///   окно попыток (default 300с = 5 мин); 0 → бесконечные попытки.
/// Если за окно подключиться не удалось — задача завершается (remote недоступен).
pub(crate) async fn run_outbound_loop(inner: Arc<NetInner>, remote: NetRemote) {
    let retry_interval = if remote.retry_interval_secs > 0 {
        remote.retry_interval_secs
    } else if inner.cfg.connect_retry_interval_secs > 0 {
        inner.cfg.connect_retry_interval_secs
    } else {
        DEFAULT_CONNECT_RETRY_INTERVAL_SECS
    };
    let timeout = if remote.connect_timeout_secs > 0 {
        remote.connect_timeout_secs
    } else if inner.cfg.connect_timeout_secs > 0 {
        inner.cfg.connect_timeout_secs
    } else {
        DEFAULT_CONNECT_TIMEOUT_SECS
    };
    let deadline = if timeout == 0 {
        None
    } else {
        Some(super::dedup::current_unix_secs().saturating_add(timeout))
    };

    loop {
        // Если задано окно — проверяем, не истекло ли оно.
        if let Some(dl) = deadline {
            if super::dedup::current_unix_secs() >= dl {
                warn!(
                    "[Хост] Net: не удалось подключиться к {} за {}с, отказываюсь",
                    remote.url, timeout
                );
                return;
            }
        }
        // Пытаемся подключиться.
        let client_cfg = resolve_outbound_mtls(&remote, &inner.cfg.mtls);
        let connect_result = match &client_cfg {
            Some(cc) => {
                // mTLS: подключаемся по wss:// с нашим клиентским сертификатом.
                tokio_tungstenite::connect_async_tls_with_config(
                    &remote.url,
                    None,
                    false,
                    Some(Connector::Rustls(cc.clone())),
                )
                .await
            }
            None => {
                // Plain ws (обратная совместимость).
                tokio_tungstenite::connect_async(&remote.url).await
            }
        };
        match connect_result {
            Ok((ws, _resp)) => {
                info!("[Хост] Net: подключились к {} ({})", remote.url, if client_cfg.is_some() { "mTLS" } else { "plain" });
                // Канал для отправки обёрток в это соединение.
                let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
                inner
                    .outbound
                    .write()
                    .await
                    .insert(remote.url.clone(), out_tx.clone());

                // Отправляем накопленные (буферизованные до подключения) события.
                {
                    let mut pending = inner.pending_outbound.write().await;
                    if let Some(buf) = pending.remove(&remote.url) {
                        for msg in buf {
                            let _ = out_tx.try_send(msg);
                        }
                    }
                }

                // split(): отправитель (Sink) и приёмник (Stream) раздельно.
                let (mut ws_sink, mut ws_stream) = ws.split();

                // B1: отправляем Auth первым сообщением (до capabilities/hello),
                // чтобы удалённая входящая сторона могла проверить токен.
                if !remote.token.is_empty() {
                    let auth = NetMessage::Auth { token: remote.token.clone() };
                    if let Ok(auth_text) = serde_json::to_string(&auth) {
                        let _ = ws_sink
                            .send(tokio_tungstenite::tungstenite::Message::Text(auth_text.into()))
                            .await;
                    }
                }

                // При подключении анонсируем свои локальные инструменты
                // (capabilities), чтобы удалённый хост знал, что мы умеем.
                let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
                let caps = pack_capabilities(&inner.cfg.node_id, my_tools);
                let _ = ws_sink
                    .send(tokio_tungstenite::tungstenite::Message::Text(caps.into()))
                    .await;

                // Задача-отправитель: читает из канала и шлёт в сокет.
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

                // Читаем ответы и впрыскиваем.
                let mut closed = false;
                while let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) =
                    ws_stream.next().await
                {
                    let Some(netmsg) = unpack_message(&text) else { continue };
                    match netmsg {
                        NetMessage::Capabilities { origin, source_id: _, neighbors: _, tools } => {
                            if origin == inner.cfg.node_id {
                                continue;
                            }
                            info!("[Хост] Net: возможности {}: {:?}", origin, tools.len());
                            inner.origin_tools.write().await.insert(origin.clone(), tools);
                            // P4: регистрируем node_id -> url для FIB-маршрутизации.
                            inner.node_url.write().await.insert(origin, remote.url.clone());
                        }
                        NetMessage::Hello { source_id, seq, neighbors, tools } => {
                            handle_hello(&inner, source_id, neighbors, tools, seq).await;
                        }
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
                                closed = true;
                                break;
                            }
                        }
                        // B1: Auth от входящей стороны здесь не ожидается — игнорируем.
                        _ => continue,
                    }
                }
                send_task.abort();
                // Соединение потеряно: убираем из карты и пробуем переподключиться.
                inner.outbound.write().await.remove(&remote.url);
                if closed {
                    break;
                }
                warn!("[Хост] Net: соединение с {} закрыто, переподключение...", remote.url);
            }
            Err(e) => {
                error!("[Хост] Net: не удалось подключиться к {}: {}", remote.url, e);
                tokio::time::sleep(std::time::Duration::from_secs(retry_interval)).await;
            }
        }
    }
}
