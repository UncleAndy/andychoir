//! Исходящие персистентные WS-соединения к remotes (с автопереподключением).

use super::*;
use super::dedup::check_dedup;
use super::discovery::handle_hello;
use super::message::{pack_capabilities, unpack_message, value_to_event, NetMessage};
use crate::config::config::NetRemote;
use tokio_tungstenite;

/// Исходящее персистентное соединение к одному remote (с автопереподключением).
pub(crate) async fn run_outbound_loop(inner: Arc<NetInner>, remote: NetRemote) {
    loop {
        // Пытаемся подключиться.
        match tokio_tungstenite::connect_async(&remote.url).await {
            Ok((ws, _resp)) => {
                info!("[Хост] Net: подключились к {}", remote.url);
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
                        NetMessage::Hello { source_id, neighbors, tools } => {
                            handle_hello(&inner, source_id, neighbors, tools).await;
                        }
                        NetMessage::Bye { source_id: _ } => {
                            // TODO P6: удалить из LSDB.
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
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    }
}
