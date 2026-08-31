//! Сетевой мост между экземплярами andychour.
//!
//! Мост — хостовый транспорт (как http/ws). Плагины и шина НЕ знают о сети:
//! Event не меняется. Вся сетевая логика живёт здесь и в подмодулях:
//! - `message`  — wire-протокол (NetMessage, pack/unpack, Event <-> JSON)
//! - `dedup`    — дедупликация входящих событий (Bloom-фильтр, P2)
//! - `lsdb`     — топология (LSDB) и маршрутизация (FIB/Dijkstra, P3/P4)
//! - `discovery`— анонсы Hello и flooding (P3)
//! - `net`      — центральный модуль: start_net, handle_incoming,
//!                 run_outbound_loop, forward

pub(crate) mod message;
pub(crate) mod dedup;
pub(crate) mod lsdb;
pub(crate) mod discovery;
pub(crate) mod server;
pub(crate) mod outbound;
pub(crate) mod forward;
pub(crate) mod net;

use crate::ai::host::types::Event;
use crate::config::config::NetConfig;
use crate::error;
use crate::info;
use crate::warn;
use axum::extract::ws::WebSocketUpgrade;
use fastbloom::BloomFilter;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

pub(crate) use message::NetMessage;
pub use net::start_net;
pub use forward::forward;

/// Внутреннее состояние сетевого моста.
pub(crate) struct NetInner {
    /// Канал для впрыска входящих событий в локальную шину.
    pub(crate) tx: mpsc::Sender<Event>,
    /// Конфиг.
    pub(crate) cfg: NetConfig,
    /// Карта контекста: session_id -> origin_host.
    pub(crate) session_origin: Arc<RwLock<HashMap<String, String>>>,
    /// Карта контекста: request_id -> (origin_host, session_id).
    pub(crate) request_origin: Arc<RwLock<HashMap<String, (String, String)>>>,
    /// Исходящие соединения: remote_url -> Sender сете-обёрток.
    pub(crate) outbound: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>,
    /// Буфер исходящих событий для remote, к которому ещё нет соединения.
    /// remote_url -> список упакованных обёрток.
    pub(crate) pending_outbound: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Инструменты удалённых хостов по их origin_node_id (из capabilities).
    pub(crate) origin_tools: Arc<RwLock<HashMap<String, Vec<crate::plugin::engine::ToolDef>>>>,
    /// Обратные каналы входящих соединений: origin_node_id -> Sender ответов.
    /// Позволяет вернуть ответ на входящее соединение (П1).
    pub(crate) incoming_senders: Arc<RwLock<HashMap<String, mpsc::Sender<String>>>>,
    /// P2: Bloom-фильтр для дедупликации входящих сетевых событий по event_id.
    /// Фиксированный размер (DEDUP_BITS), сбрасывается каждые DEDUP_WINDOW_SECS.
    pub(crate) dedup: Arc<RwLock<BloomFilter>>,
    /// P2: время последнего сброса Bloom-фильтра (unix-секунды).
    pub(crate) dedup_last_reset: Arc<RwLock<u64>>,
    /// P3: Link-State Database — полная топология сети.
    /// node_id -> (список соседей node_id, время последнего Hello).
    pub(crate) lsdb: Arc<RwLock<HashMap<String, (Vec<String>, u64)>>>,
    /// P4: FIB (Forwarding Information Base) — таблица маршрутизации.
    /// target_node_id -> next_hop_node_id (через кого слать).
    pub(crate) fib: Arc<RwLock<HashMap<String, String>>>,
    /// P4: мапа node_id -> ws-url исходящего соединения (для отправки по FIB).
    pub(crate) node_url: Arc<RwLock<HashMap<String, String>>>,
}

static INNER: std::sync::Mutex<Option<Arc<NetInner>>> = std::sync::Mutex::new(None);

#[cfg(test)]
impl NetInner {
    /// Тестовый конструктор (минимальный, без сетевых соединений).
    pub(crate) fn new_test() -> Arc<NetInner> {
        Self::new_test_with_cfg(NetConfig::default())
    }

    /// Тестовый конструктор с заданным конфигом (remotes и т.п.).
    pub(crate) fn new_test_with_cfg(cfg: NetConfig) -> Arc<NetInner> {
        Arc::new(NetInner {
            tx: mpsc::channel(1).0,
            cfg,
            session_origin: Arc::new(RwLock::new(HashMap::new())),
            request_origin: Arc::new(RwLock::new(HashMap::new())),
            outbound: Arc::new(RwLock::new(HashMap::new())),
            pending_outbound: Arc::new(RwLock::new(HashMap::new())),
            origin_tools: Arc::new(RwLock::new(HashMap::new())),
            incoming_senders: Arc::new(RwLock::new(HashMap::new())),
            dedup: Arc::new(RwLock::new(
                BloomFilter::with_num_bits(dedup::DEDUP_BITS).expected_items(1024),
            )),
            dedup_last_reset: Arc::new(RwLock::new(dedup::current_unix_secs())),
            lsdb: Arc::new(RwLock::new(HashMap::new())),
            fib: Arc::new(RwLock::new(HashMap::new())),
            node_url: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

pub(crate) fn set_inner(inner: Arc<NetInner>) {
    *INNER.lock().unwrap() = Some(inner);
}

pub(crate) fn get_inner() -> Option<Arc<NetInner>> {
    INNER.lock().unwrap().clone()
}

/// Handle для сетевого моста.
#[allow(dead_code)]
pub struct NetHandle {
    pub(crate) server: tokio::task::JoinHandle<()>,
    pub(crate) outbound: Vec<tokio::task::JoinHandle<()>>,
    pub(crate) inner: Arc<NetInner>,
}

/// P6: graceful shutdown — при drop моста рассылаем Bye всем соседям.
impl Drop for NetHandle {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        // Fire-and-forget: отправляем Bye асинхронно (Drop не может быть async).
        tokio::spawn(async move {
            let bye = discovery::pack_bye(&inner).await;
            discovery::broadcast_to_neighbors(&inner, &bye, "").await;
            crate::info!("[Хост] Net: мост остановлен, разослан Bye соседям");
        });
    }
}
