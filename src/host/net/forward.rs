//! Форвард события на удалённый хост (маршрутизация по FIB + fallback).

use super::*;
use super::lsdb::route_next_hop;
use super::message::{decrement_ttl, pack_event_with_ttl};

/// Форвард события на удалённый хост (вызывается из bus при пустом получателе).
/// Возвращает true, если событие ушло по сети (иначе — не нашлось подходящего remote).
pub async fn forward(ev: &Event) -> bool {
    let Some(inner) = super::get_inner() else {
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

    // P4: если target указывает на конкретный узел (host:<node_id>:<...>),
    // маршрутизируем через FIB (кратчайший путь), а не через ручной targets.
    if let Some(node_id) = target.strip_prefix("host:") {
        if let Some((_, _tool)) = node_id.split_once(':') {
            let target_node = node_id.split(':').next().unwrap_or(node_id);
            if let Some(next_hop) = route_next_hop(&inner, target_node).await {
                // next_hop -> url исходящего соединения.
                let node_url_map = inner.node_url.read().await;
                if let Some(url) = node_url_map.get(&next_hop) {
                    let outbound = inner.outbound.read().await;
                    if let Some(tx) = outbound.get(url) {
                        let origin = origin_host.clone().unwrap_or_else(|| inner.cfg.node_id.clone());
                        let ttl = decrement_ttl(16).unwrap_or(0);
                        let payload = pack_event_with_ttl(&origin, 0, ev, ttl);
                        if tx.try_send(payload).is_ok() {
                            info!("[Хост] Net: событие {} → узел {} (next-hop {}) по FIB", ev.target, target_node, next_hop);
                            return true;
                        }
                    }
                    drop(outbound);
                }
                drop(node_url_map);
                // Нет соединения — буферизуем по url (если известен).
                // (url может быть неизвестен, если next_hop ещё не анонсировал capabilities)
            }
        }
    }

    // Fallback: ручной роутинг по targets из конфига (обратная совместимость).
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
    let origin = origin_host.clone().unwrap_or_else(|| inner.cfg.node_id.clone());
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
