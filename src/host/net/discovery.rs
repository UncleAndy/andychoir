//! P3: Discovery — анонсы Hello, flooding по сети, обновление LSDB.
//! Hello рассылается периодически и при получении пересылается соседям
//! (flooding), что позволяет каждому узлу построить полную топологию (LSDB).

use super::{lsdb, NetInner};
use crate::plugin::engine::ToolDef;
use std::sync::Arc;

/// P3: интервал периодической рассылки Hello (секунды).
pub(crate) const HELLO_INTERVAL_SECS: u64 = 10;

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
