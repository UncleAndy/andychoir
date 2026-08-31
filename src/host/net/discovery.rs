//! P3: Discovery — анонсы Hello, flooding по сети, обновление LSDB.
//! Hello рассылается периодически и при получении пересылается соседям
//! (flooding), что позволяет каждому узлу построить полную топологию (LSDB).

use super::{lsdb, NetInner};
use crate::plugin::engine::ToolDef;
use std::sync::Arc;

/// P3: интервал периодической рассылки Hello (секунды).
pub(crate) const HELLO_INTERVAL_SECS: u64 = 10;

/// P6: таймаут удаления хоста из LSDB при отсутствии Hello (секунды).
pub(crate) const LSDB_TIMEOUT_SECS: u64 = 30;

/// P3: Упаковать Hello-сообщение (discovery) с текущим списком соседей.
pub(crate) async fn pack_hello(inner: &Arc<NetInner>) -> String {
    let neighbors = inner.cfg.remotes.iter().map(|r| r.url.clone()).collect::<Vec<_>>();
    let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
    let msg = super::NetMessage::Hello {
        source_id: inner.cfg.node_id.clone(),
        neighbors,
        tools: my_tools,
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// P3: Обработать входящий Hello (discovery).
/// Обновляет LSDB, пересылает соседям (flooding), сохраняет tools.
pub(crate) async fn handle_hello(inner: &Arc<NetInner>, source_id: String, neighbors: Vec<String>, tools: Vec<ToolDef>) {
    if source_id == inner.cfg.node_id {
        return; // свои Hello игнорируем
    }
    // Обновляем LSDB: source_id -> (neighbors, now)
    {
        let mut lsdb = inner.lsdb.write().await;
        lsdb.insert(source_id.clone(), (neighbors.clone(), super::dedup::current_unix_secs()));
    }
    // Сохраняем инструменты удалённого хоста.
    inner.origin_tools.write().await.insert(source_id.clone(), tools);
    crate::info!("[Хост] Net: discovery от {} (соседи: {:?})", source_id, neighbors.len());
    // P4: пересчитываем FIB из обновлённой LSDB (маршрутизация по кратчайшему пути).
    lsdb::rebuild_fib(inner).await;
    // Flooding: пересылаем Hello всем соседям, кроме источника.
    let fwd = pack_hello_from(source_id.clone(), neighbors).await;
    broadcast_to_neighbors(inner, &fwd, &source_id).await;
}

/// P3: Собрать Hello от конкретного узла (для flooding).
async fn pack_hello_from(source_id: String, neighbors: Vec<String>) -> String {
    let my_tools = crate::plugin::engine::local_tools().read().await.values().cloned().collect::<Vec<_>>();
    let msg = super::NetMessage::Hello { source_id, neighbors, tools: my_tools };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// P3: Разослать сообщение всем соседям (flooding).
/// except_source — не отправлять обратно источнику (защита от петель).
pub(crate) async fn broadcast_to_neighbors(inner: &Arc<NetInner>, msg: &str, except_source: &str) {
    // Входящие соединения (по node_id).
    {
        let incoming = inner.incoming_senders.read().await;
        for (node_id, tx) in incoming.iter() {
            if node_id == except_source {
                continue;
            }
            let _ = tx.try_send(msg.to_string());
        }
    }
    // Исходящие соединения (по url).
    {
        let outbound = inner.outbound.read().await;
        for (_url, tx) in outbound.iter() {
            let _ = tx.try_send(msg.to_string());
        }
    }
}

/// P3: Периодическая рассылка Hello всем соседям (discovery loop).
pub(crate) async fn run_discovery_loop(inner: Arc<NetInner>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(HELLO_INTERVAL_SECS));
    loop {
        interval.tick().await;
        let hello = pack_hello(&inner).await;
        broadcast_to_neighbors(&inner, &hello, "").await;
    }
}

/// P6: Упаковать Bye-сообщение (graceful leave).
pub(crate) async fn pack_bye(inner: &Arc<NetInner>) -> String {
    let msg = super::NetMessage::Bye {
        source_id: inner.cfg.node_id.clone(),
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// P6: Обработать уход соседа (Bye или timeout).
/// Удаляет узел из LSDB, инструментов, каналов; пересчитывает FIB.
pub(crate) async fn handle_bye(inner: &Arc<NetInner>, source_id: &str) {
    if source_id == inner.cfg.node_id {
        return;
    }
    let was_present = inner.lsdb.write().await.remove(source_id).is_some();
    inner.origin_tools.write().await.remove(source_id);
    inner.incoming_senders.write().await.remove(source_id);
    inner.node_url.write().await.remove(source_id);
    if was_present {
        crate::info!("[Хост] Net: узел {} покинул сеть (Bye), LSDB обновлена", source_id);
        lsdb::rebuild_fib(inner).await;
    }
}

/// P6: Один проход очистки LSDB от устаревших узлов (failure detection).
/// Узел удаляется, если с момента последнего Hello прошло > LSDB_TIMEOUT_SECS.
/// Вынесено из `run_lsdb_cleanup_loop` для тестируемости.
pub(crate) async fn cleanup_expired_once(inner: &Arc<NetInner>) {
    let now = super::dedup::current_unix_secs();
    let mut expired = Vec::new();
    {
        let lsdb = inner.lsdb.read().await;
        for (node_id, (_neighbors, ts)) in lsdb.iter() {
            if now.saturating_sub(*ts) > LSDB_TIMEOUT_SECS {
                expired.push(node_id.clone());
            }
        }
    }
    for node_id in expired {
        crate::warn!("[Хост] Net: узел {} не подавал признаков жизни >{}с, удаляю из LSDB", node_id, LSDB_TIMEOUT_SECS);
        handle_bye(inner, &node_id).await;
    }
}

/// P6: Периодическая очистка LSDB от устаревших узлов (failure detection).
/// Узел удаляется, если с момента последнего Hello прошло > LSDB_TIMEOUT_SECS.
pub(crate) async fn run_lsdb_cleanup_loop(inner: Arc<NetInner>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(LSDB_TIMEOUT_SECS / 2));
    loop {
        interval.tick().await;
        cleanup_expired_once(&inner).await;
    }
}
