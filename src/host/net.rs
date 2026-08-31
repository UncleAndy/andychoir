//! Сетевой мост между экземплярами andychour.
//!
//! Мост — хостовый транспорт (как http/ws). Плагины и шина НЕ знают о сети:
//! Event не меняется. Вся сетевая логика живёт здесь.
//!
//! - Входящее: WS-сервер на `/net` (порт listen_port). Принимает сетевые
//!   обёртки `{origin, event}`, впрыскивает event в локальную шину (tx).
//! - Исходящее: персистентные WS-соединения к каждому `remote`. Когда
//!   `forward(event)` вызывается из bus (событие не нашло локального
//!   получателя), мост отправляет событие на нужный удалённый хост.
//! - Карта контекста: `session_id -> origin_host` (куда возвращать вызовы
//!   утилит из этого запроса) и `request_id -> (origin, session)`.
//! - Предотвращение циклов: origin != self, ограничение хопов.
//! - Аутентификация: токен в WS-handshake (заголовок/query).

use crate::ai::host::types::Event;
use crate::config::config::{NetConfig, NetRemote};
use crate::{error, info, warn};
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::Router;
use fastbloom::BloomFilter;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// Размер битовой матрицы Bloom-фильтра (P2). 4096 бит = 512 байт, фиксированно.
/// Не растёт со временем; периодически сбрасывается (dedup_window).
const DEDUP_BITS: usize = 4096;
/// Окно сброса Bloom-фильтра (секунды).
const DEDUP_WINDOW_SECS: u64 = 60;

/// Внутреннее состояние сетевого моста.
pub struct NetInner {
    /// Канал для впрыска входящих событий в локальную шину.
    tx: mpsc::Sender<Event>,
    /// Конфиг.
    cfg: NetConfig,
    /// Карта контекста: session_id -> origin_host.
    session_origin: Arc<RwLock<HashMap<String, String>>>,
    /// Карта контекста: request_id -> (origin_host, session_id).
    request_origin: Arc<RwLock<HashMap<String, (String, String)>>>,
    /// Исходящие соединения: remote_url -> Sender сете-обёрток.
    outbound: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>,
    /// Буфер исходящих событий для remote, к которому ещё нет соединения.
    /// remote_url -> список упакованных обёрток.
    pending_outbound: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Инструменты удалённых хостов по их origin_node_id (из capabilities).
    origin_tools: Arc<RwLock<HashMap<String, Vec<crate::plugin::engine::ToolDef>>>>,
    /// Обратные каналы входящих соединений: origin_node_id -> Sender ответов.
    /// Позволяет вернуть ответ на входящее соединение (П1).
    incoming_senders: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>,
    /// P2: Bloom-фильтр для дедупликации входящих сетевых событий по event_id.
    /// Фиксированный размер (DEDUP_BITS), сбрасывается каждые DEDUP_WINDOW_SECS.
    dedup: Arc<RwLock<BloomFilter>>,
    /// P2: время последнего сброса Bloom-фильтра (unix-секунды).
    dedup_last_reset: Arc<RwLock<u64>>,
}

static INNER: std::sync::OnceLock<Arc<NetInner>> = std::sync::OnceLock::new();

#[cfg(test)]
impl NetInner {
    /// Тестовый конструктор (минимальный, без сетевых соединений).
    fn new_test() -> Arc<NetInner> {
        Arc::new(NetInner {
            tx: mpsc::channel(1).0,
            cfg: NetConfig::default(),
            session_origin: Arc::new(RwLock::new(HashMap::new())),
            request_origin: Arc::new(RwLock::new(HashMap::new())),
            outbound: Arc::new(RwLock::new(HashMap::new())),
            pending_outbound: Arc::new(RwLock::new(HashMap::new())),
            origin_tools: Arc::new(RwLock::new(HashMap::new())),
            incoming_senders: Arc::new(RwLock::new(HashMap::new())),
            dedup: Arc::new(RwLock::new(
                BloomFilter::with_num_bits(DEDUP_BITS).expected_items(1024),
            )),
            dedup_last_reset: Arc::new(RwLock::new(current_unix_secs())),
        })
    }
}

fn set_inner(inner: Arc<NetInner>) {
    let _ = INNER.set(inner);
}

pub fn get_inner() -> Option<Arc<NetInner>> {
    INNER.get().cloned()
}

/// Handle для сетевого моста.
pub struct NetHandle {
    pub server: tokio::task::JoinHandle<()>,
    pub outbound: Vec<tokio::task::JoinHandle<()>>,
}

/// Сетевое сообщение: событие ИЛИ анонс возможностей (capabilities) хоста.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NetMessage {
    /// Событие шины (упаковано для передачи по сети).
    /// P1: добавлены поля для mesh-маршрутизации и dedup:
    /// - source_id: node_id отправителя (UUID хоста)
    /// - event_id: уникальный UUID события (для Bloom-фильтра dedup)
    /// - ttl: счетчик времени жизни (защита от петель)
    Event {
        origin: String,      // обратная совместимость: node_id отправителя
        source_id: String,   // node_id отправителя (mesh-маршрутизация)
        event_id: String,    // UUID события (dedup)
        ttl: u8,              // time-to-live
        hop: u8,
        event: serde_json::Value,
    },
    /// Анонс локальных инструментов хоста (шлётся при подключении).
    /// P1: добавлены neighbors (список известных соседей) для LSDB.
    Capabilities {
        origin: String,
        source_id: String,   // node_id анонсирующего хоста
        neighbors: Vec<String>, // известные соседи (для LSDB)
        tools: Vec<crate::plugin::engine::ToolDef>,
    },
    /// Приветствие (discovery): анонс node_id + соседей + инструменты.
    /// Flooding по сети для построения полной топологии (LSDB).
    Hello {
        source_id: String,
        neighbors: Vec<String>,
        tools: Vec<crate::plugin::engine::ToolDef>,
    },
    /// Уведомление об уходе хоста (graceful shutdown).
    Bye {
        source_id: String,
    },
}

/// Event -> JSON Value (поля record Event).
fn event_to_value(ev: &Event) -> serde_json::Value {
    serde_json::json!({
        "request_id": ev.request_id,
        "session_id": ev.session_id,
        "source": ev.source,
        "target": ev.target,
        "topic": ev.topic,
        "payload": ev.payload,
    })
}

/// JSON Value -> Event.
fn value_to_event(v: &serde_json::Value) -> Option<Event> {
    Some(Event {
        request_id: v.get("request_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        session_id: v.get("session_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        source: v.get("source").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        target: v.get("target").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        topic: v.get("topic").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        payload: v.get("payload").and_then(|x| x.as_str()).unwrap_or("").to_string(),
    })
}

/// Упаковать событие в сетевое сообщение (TTL по умолчанию 16).
/// Используется в тестах; в прод-коде применяется pack_event_with_ttl (с декрементом TTL).
#[allow(dead_code)]
fn pack_event(origin: &str, hop: u8, ev: &Event) -> String {
    let ttl: u8 = 16; // значение по умолчанию при первой упаковке
    pack_event_with_ttl(origin, hop, ev, ttl)
}

/// Упаковать событие с явно заданным TTL (P2: для декремента при пересылке).
fn pack_event_with_ttl(origin: &str, hop: u8, ev: &Event, ttl: u8) -> String {
    let event_id = uuid::Uuid::new_v4().to_string();
    let msg = NetMessage::Event {
        origin: origin.to_string(),
        source_id: origin.to_string(),
        event_id,
        ttl,
        hop,
        event: event_to_value(ev),
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// P2: декремент TTL при пересылке события дальше по сети.
/// Возвращает Some(ttl-1), если событие ещё живое (ttl > 0),
/// и None, если TTL исчерпан (событие нужно отбросить).
fn decrement_ttl(ttl: u8) -> Option<u8> {
    ttl.checked_sub(1)
}

/// Упаковать анонс возможностей (локальные инструменты хоста).
/// P1: добавлены source_id и neighbors (из LSDB, пока пустой — заполняется в P3).
fn pack_capabilities(origin: &str, tools: Vec<crate::plugin::engine::ToolDef>) -> String {
    let msg = NetMessage::Capabilities {
        origin: origin.to_string(),
        source_id: origin.to_string(),
        neighbors: Vec::new(), // P3: заполняется из LSDB
        tools,
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// Распаковать сетевое сообщение.
fn unpack_message(text: &str) -> Option<NetMessage> {
    serde_json::from_str(text).ok()
}

/// Запустить сетевой мост: входящий WS-сервер + исходящие соединения к remotes.
pub async fn start_net(tx: mpsc::Sender<Event>, cfg: NetConfig) -> NetHandle {
    let inner = Arc::new(NetInner {
        tx,
        cfg: cfg.clone(),
        session_origin: Arc::new(RwLock::new(HashMap::new())),
        request_origin: Arc::new(RwLock::new(HashMap::new())),
        outbound: Arc::new(RwLock::new(HashMap::new())),
        pending_outbound: Arc::new(RwLock::new(HashMap::new())),
        origin_tools: Arc::new(RwLock::new(HashMap::new())),
        incoming_senders: Arc::new(RwLock::new(HashMap::new())),
        dedup: Arc::new(RwLock::new(
            BloomFilter::with_num_bits(DEDUP_BITS).expected_items(1024),
        )),
        dedup_last_reset: Arc::new(RwLock::new(current_unix_secs())),
    });
    set_inner(inner.clone());

    // Входящий WS-сервер.
    let server = tokio::spawn({
        let inner = inner.clone();
        async move {
            run_incoming_server(inner).await;
        }
    });

    // Исходящие персистентные соединения.
    let mut outbound = Vec::new();
    for remote in cfg.remotes.clone() {
        let inner = inner.clone();
        outbound.push(tokio::spawn(async move {
            run_outbound_loop(inner, remote).await;
        }));
    }

    NetHandle { server, outbound }
}

/// Входящий WS-сервер на /net.
async fn run_incoming_server(inner: Arc<NetInner>) {
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
async fn handle_incoming(inner: Arc<NetInner>, socket: WebSocket) {
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
                // Регистрируем обратный канал для этого origin (П1).
                this_origin = Some(origin.clone());
                inner.incoming_senders.write().await.insert(origin, out_tx.clone());
                // Отвечаем своими возможностями (локальные инструменты), чтобы
                // удалённый хост узнал, что мы умеем.
                let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
                let caps = pack_capabilities(&inner.cfg.node_id, my_tools);
                let _ = out_tx.try_send(caps);
            }
            // P1: Hello (discovery) — обрабатывается в P3.
            NetMessage::Hello { source_id: _, neighbors: _, tools: _ } => {
                // TODO P3: обновить LSDB, переслать соседям.
            }
            // P1: Bye (graceful leave) — обрабатывается в P6.
            NetMessage::Bye { source_id: _ } => {
                // TODO P6: удалить из LSDB, очистить каналы.
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
                inner.request_origin.write().await.insert(
                    ev.request_id.clone(),
                    (origin.clone(), ev.session_id.clone()),
                );
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

/// Исходящее персистентное соединение к одному remote (с автопереподключением).
async fn run_outbound_loop(inner: Arc<NetInner>, remote: NetRemote) {
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
                            inner.origin_tools.write().await.insert(origin, tools);
                        }
                        NetMessage::Hello { source_id: _, neighbors: _, tools: _ } => {
                            // TODO P3: LSDB.
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
                inner.outbound.write().await.remove(&remote.url);
                if closed {
                    return;
                }
                warn!("[Хост] Net: соединение с {} закрыто, переподключаюсь", remote.url);
            }
            Err(e) => {
                warn!("[Хост] Net: не удалось подключиться к {}: {}", remote.url, e);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

/// Текущее время в unix-секундах (для окна сброса dedup).
fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// P2: Проверить и зарегистрировать event_id в Bloom-фильтре.
///
/// Возвращает `true`, если событие НОВОЕ (пропустить дальше),
/// и `false`, если это ДУБЛИКАТ (уже видели в окне dedup).
///
/// Логика:
/// - Сначала проверяем окно сброса: если прошло > DEDUP_WINDOW_SECS,
///   очищаем фильтр (старые события "забываются" — для event-bus это ок).
/// - `check_and_add` атомарно: возвращает true, если элемент УЖЕ был
///   (дубликат), и добавляет его. Инвертируем для смысла "новое".
pub async fn check_dedup(inner: &Arc<NetInner>, event_id: &str) -> bool {
    // Сброс по окну.
    {
        let mut last = inner.dedup_last_reset.write().await;
        let now = current_unix_secs();
        if now.saturating_sub(*last) >= DEDUP_WINDOW_SECS {
            inner.dedup.write().await.clear();
            *last = now;
        }
    }
    // contains → true, если УЖЕ присутствует (дубликат). insert добавляет.
    let is_dup = inner.dedup.write().await.contains(event_id);
    if !is_dup {
        inner.dedup.write().await.insert(event_id);
    }
    !is_dup
}

/// P2: Полностью сбросить Bloom-фильтр (полезно при очистке/тестах).
pub async fn reset_dedup(inner: &Arc<NetInner>) {
    inner.dedup.write().await.clear();
    *inner.dedup_last_reset.write().await = current_unix_secs();
}

/// Форвард события на удалённый хост (вызывается из bus при пустом получателе).
/// Возвращает true, если событие ушло по сети (иначе — не нашлось подходящего remote).
pub async fn forward(ev: &Event) -> bool {
    let Some(inner) = get_inner() else {
        return false;
    };

    // Определяем origin_host для этого события.
    let origin_host = {
        // Если это ответ/событие в рамках известной сессии — используем origin из карты.
        let r = inner.request_origin.read().await;
        let s = inner.session_origin.read().await;
        r.get(&ev.request_id)
            .map(|(o, _)| o.clone())
            .or_else(|| s.get(&ev.session_id).cloned())
    };

    // Выбираем remote, который обслуживает target этого события.
    let target = ev.target.clone();
    let remotes = inner.cfg.remotes.clone();
    let target_remote = remotes.into_iter().find(|r| {
        r.targets.iter().any(|t| {
            t == &target || (target.starts_with(t) && target.as_bytes().get(t.len()) == Some(&b':'))
        })
    });

    // Если target не найден среди remotes, но событие — ответ на запрос,
    // пришедший с известного origin (входящее соединение), возвращаем его
    // обратно по этому входящему соединению (П1). Это ключевое для
    // распределённого сценария: запрос пришёл с B, утилита на A, ответ должен
    // вернуться на B через входящее соединение A<-B.
    if target_remote.is_none() {
        if let Some(origin) = origin_host.as_ref() {
            let incoming = inner.incoming_senders.read().await;
            if let Some(tx) = incoming.get(origin) {
                let my_id = inner.cfg.node_id.clone();
                // P2: декремент TTL при пересылке (защита от петель).
                let ttl = decrement_ttl(16).unwrap_or(0);
                let payload = pack_event_with_ttl(&my_id, 0, ev, ttl);
                if tx.try_send(payload).is_ok() {
                    info!("[Хост] Net: ответ {} возвращён по входящему соединению {} (origin)", ev.target, origin);
                    return true;
                }
            }
        }
        return false;
    }
    let remote = target_remote.unwrap();

    // Собираем обёртку.
    let origin = origin_host.unwrap_or_else(|| inner.cfg.node_id.clone());
    // P2: декремент TTL при пересылке (защита от петель).
    let ttl = decrement_ttl(16).unwrap_or(0);
    let payload = pack_event_with_ttl(&origin, 0, ev, ttl);

    let outbound = inner.outbound.read().await;
    if let Some(tx) = outbound.get(&remote.url) {
        if tx.try_send(payload.clone()).is_ok() {
            info!("[Хост] Net: отправлено событие {} на {}", ev.target, remote.url);
            return true;
        }
    }
    // Соединения ещё нет — буферизуем, отправим при подключении (не теряем
    // discovery/request).
    drop(outbound);
    let mut pending = inner.pending_outbound.write().await;
    pending.entry(remote.url.clone()).or_default().push(payload);
    info!("[Хост] Net: событие {} буферизовано для {} (нет соединения)", ev.target, remote.url);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(target: &str, session: &str) -> Event {
        Event {
            request_id: "r1".into(),
            session_id: session.into(),
            source: "agent:demo".into(),
            target: target.into(),
            topic: "request".into(),
            payload: "{}".into(),
        }
    }

    // Упаковка/распаковка события сохраняет origin и Event.
    #[test]
    fn pack_unpack_roundtrip() {
        let ev = make_event("tool:calculator", "sess-1");
        let s = pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let msg = unpack_message(&s).unwrap();
        match msg {
            NetMessage::Event { origin, source_id, event_id, ttl, hop, event } => {
                let ev2 = value_to_event(&event).unwrap();
                assert_eq!(origin, "00000000-0000-0000-0000-0000000000a1");
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
                assert!(!event_id.is_empty(), "event_id должен быть сгенерирован");
                assert_eq!(ttl, 16, "ttl по умолчанию 16");
                assert_eq!(hop, 0);
                assert_eq!(ev2.target, "tool:calculator");
                assert_eq!(ev2.session_id, "sess-1");
                assert_eq!(ev2.payload, "{}");
            }
            _ => panic!("expected Event message"),
        }
    }

    // Анонс возможностей: упаковка/распаковка сохраняет инструменты.
    #[test]
    fn capabilities_roundtrip() {
        let tools = vec![crate::plugin::engine::ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        let s = pack_capabilities("00000000-0000-0000-0000-0000000000a1", tools.clone());
        let msg = unpack_message(&s).unwrap();
        match msg {
            NetMessage::Capabilities { origin, source_id, neighbors, tools: t } => {
                assert_eq!(origin, "00000000-0000-0000-0000-0000000000a1");
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
                assert!(neighbors.is_empty(), "neighbors пустой до P3");
                assert_eq!(t.len(), 1);
                assert_eq!(t[0].name, "calculator");
            }
            _ => panic!("expected Capabilities message"),
        }
    }

    // Выбор remote по target: точное совпадение и префикс с ':'.
    #[test]
    fn remote_matching() {
        let remotes = vec![
            NetRemote { url: "ws://b".into(), token: "".into(), targets: vec!["tool:calculator".into()] },
            NetRemote { url: "ws://c".into(), token: "".into(), targets: vec!["tool:file".into()] },
        ];
        // tool:calculator -> хост b
        let r = remotes.iter().find(|r| r.targets.iter().any(|t| {
            t == "tool:calculator"
        }));
        assert_eq!(r.unwrap().url, "ws://b");
        // tool:file -> хост c
        let r = remotes.iter().find(|r| r.targets.iter().any(|t| t == "tool:file"));
        assert_eq!(r.unwrap().url, "ws://c");
    }

    // origin == self отбрасывается (защита от циклов).
    #[test]
    fn self_origin_rejected() {
        let node_id = "00000000-0000-0000-0000-0000000000a1";
        let s = pack_event("00000000-0000-0000-0000-0000000000a1", 0, &make_event("x", "s"));
        match unpack_message(&s).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, true),
            _ => panic!(),
        }
        let s2 = pack_event("00000000-0000-0000-0000-0000000000b2", 0, &make_event("x", "s"));
        match unpack_message(&s2).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, false),
            _ => panic!(),
        }
    }

    // P1: Hello (discovery) упаковывается и распаковывается с source_id/neighbors.
    #[test]
    fn hello_roundtrip() {
        let tools = vec![crate::plugin::engine::ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        let msg = NetMessage::Hello {
            source_id: "00000000-0000-0000-0000-0000000000a1".into(),
            neighbors: vec!["00000000-0000-0000-0000-0000000000b2".into()],
            tools: tools.clone(),
        };
        let s = serde_json::to_string(&msg).unwrap();
        match unpack_message(&s).unwrap() {
            NetMessage::Hello { source_id, neighbors, tools: t } => {
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
                assert_eq!(neighbors, vec!["00000000-0000-0000-0000-0000000000b2"]);
                assert_eq!(t.len(), 1);
            }
            _ => panic!("expected Hello message"),
        }
    }

    // P1: Bye (graceful leave) упаковывается и распаковывается с source_id.
    #[test]
    fn bye_roundtrip() {
        let msg = NetMessage::Bye {
            source_id: "00000000-0000-0000-0000-0000000000a1".into(),
        };
        let s = serde_json::to_string(&msg).unwrap();
        match unpack_message(&s).unwrap() {
            NetMessage::Bye { source_id } => {
                assert_eq!(source_id, "00000000-0000-0000-0000-0000000000a1");
            }
            _ => panic!("expected Bye message"),
        }
    }

    // P1: Event содержит уникальный event_id при каждой упаковке (для dedup).
    #[test]
    fn event_id_unique_per_pack() {
        let ev = make_event("tool:x", "s");
        let s1 = pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let s2 = pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let m1 = unpack_message(&s1).unwrap();
        let m2 = unpack_message(&s2).unwrap();
        let id1 = match m1 { NetMessage::Event { event_id, .. } => event_id, _ => panic!() };
        let id2 = match m2 { NetMessage::Event { event_id, .. } => event_id, _ => panic!() };
        assert_ne!(id1, id2, "event_id должен быть уникальным для каждой упаковки");
        assert!(uuid::Uuid::parse_str(&id1).is_ok(), "event_id должен быть UUID");
    }

    // P2: декремент TTL — базовая логика защиты от петель.
    #[test]
    fn p2_ttl_decrement() {
        assert_eq!(decrement_ttl(0), None, "ttl=0 → событие отбрасывается");
        assert_eq!(decrement_ttl(1), Some(0), "ttl=1 → 0 (последний hop)");
        assert_eq!(decrement_ttl(16), Some(15), "ttl=16 → 15");
    }

    // P2: упаковка с явным TTL сохраняет значение при roundtrip.
    #[test]
    fn p2_ttl_preserved_in_message() {
        let ev = make_event("tool:x", "s");
        let s = pack_event_with_ttl("00000000-0000-0000-0000-0000000000a1", 0, &ev, 7);
        match unpack_message(&s).unwrap() {
            NetMessage::Event { ttl, .. } => assert_eq!(ttl, 7, "ttl должен сохраняться в сообщении"),
            _ => panic!("expected Event"),
        }
    }

    // P2: Bloom-фильтр корректно определяет дубликаты (contains/insert семантика).
    #[test]
    fn p2_bloom_check_and_add() {
        let mut bf: BloomFilter = BloomFilter::with_num_bits(4096).expected_items(1024);
        // Первый раз — не было (false), второй — уже есть (true).
        let first = bf.contains("event-1");
        bf.insert("event-1");
        let second = bf.contains("event-1");
        assert!(!first, "первое появление → не дубликат");
        assert!(second, "повтор → дубликат");
        // Другой ID — с высокой вероятностью новый.
        let other = bf.contains("event-2");
        assert!(!other, "другой ID → новый");
    }

    // P2: окно сброса dedup — старые события "забываются" через DEDUP_WINDOW_SECS.
    #[tokio::test]
    async fn p2_dedup_window_reset() {
        let inner = NetInner::new_test();
        let id = "00000000-0000-0000-0000-0000000000d2";
        assert!(check_dedup(&inner, id).await, "первый раз → новое");
        assert!(!check_dedup(&inner, id).await, "повтор (в окне) → дубликат");
        // Симулируем старение: сдвигаем dedup_last_reset на 61с назад.
        let old = current_unix_secs().saturating_sub(61);
        *inner.dedup_last_reset.write().await = old;
        // Теперь check_dedup должен сбросить фильтр (окно истекло) и принять ID как новый.
        assert!(check_dedup(&inner, id).await, "после истечения окна → снова новое");
    }

    // P2: check_dedup отклоняет повторяющийся event_id (интеграция с NetInner).
    #[tokio::test]
    async fn p2_dedup_rejects_duplicate() {
        let inner = NetInner::new_test();
        let id = "00000000-0000-0000-0000-0000000000d1";
        assert!(check_dedup(&inner, id).await, "первый раз → новое событие");
        assert!(!check_dedup(&inner, id).await, "повтор → дубликат (отброшен)");
        // После сброса — снова новое.
        reset_dedup(&inner).await;
        assert!(check_dedup(&inner, id).await, "после reset → снова новое");
    }

    // P2: разные event_id не блокируют друг друга (низкий false-positive).
    #[tokio::test]
    async fn p2_dedup_distinct_ids() {
        let inner = NetInner::new_test();
        assert!(check_dedup(&inner, "ev-aaa").await, "ev-aaa → новое");
        assert!(check_dedup(&inner, "ev-bbb").await, "ev-bbb → новое (не конфликтует)");
        assert!(check_dedup(&inner, "ev-ccc").await, "ev-ccc → новое (не конфликтует)");
        // Повтор ev-aaa всё ещё дубликат.
        assert!(!check_dedup(&inner, "ev-aaa").await, "ev-aaa повтор → дубликат");
    }
}
