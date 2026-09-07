//! Форвард события на удалённый хост (маршрутизация по FIB, P5).
//!
//! Приоритет маршрутизации:
//! 1. `host:<node_id>:<tool>` — маршрутизация через FIB (кратчайший путь).
//! 2. Ответ на запрос (target = известный origin из request_origin) —
//!    возврат по входящему соединению (backward-compat П1).
//! 3. Ручной pinned-routing по `cfg.remotes[].targets` (опционально).
//! 4. Иначе — событие локальное, не форвардим.

use super::*;
use super::lsdb::route_next_hop;
use super::message::{decrement_ttl, pack_event_with_ttl};

/// Форвард события на удалённый хост (вызывается из bus при пустом получателе).
/// Возвращает true, если событие ушло по сети (иначе — не нашлось подходящего remote).
pub async fn forward(ev: &Event) -> bool {
    let Some(inner) = super::get_inner() else {
        return false;
    };
    forward_inner(&inner, ev).await
}

/// Внутренняя логика форварда с явным `inner` (без обращения к глобальному
/// состоянию). Удобно для тестирования: тест передаёт подготовленный
/// `NetInner` напрямую, без гонки за глобальный `INNER` между параллельными
/// тестами.
pub(crate) async fn forward_inner(inner: &Arc<NetInner>, ev: &Event) -> bool {
    // Определяем origin_host для этого события (откуда пришёл запрос/сессия).
    let origin_host = {
        let r = inner.request_origin.read().await;
        let s = inner.session_origin.read().await;
        r.get(&ev.request_id)
            .map(|(o, _)| o.clone())
            .or_else(|| s.get(&ev.session_id).cloned())
    };

    let mut target = ev.target.clone();

    // --- Приоритезация инструментов (Origin-Aware, docs/tool-prioritization) ---
    // Агент просит инструмент по имени (`tool:<name>`). Хост разрешает его в
    // сетевой target `host:<node_id>:tool:<name>`, выбирая узел-источник
    // (приоритет 1) или другой узел сети (приоритет 3). Локальный приоритет
    // (Tier 2) уже обработан шиной до вызова forward. Явный `host:<node>:...`
    // таргет НЕ переопределяем (агент сам выбрал конкретный узел).
    if let Some(tool_name) = target.strip_prefix("tool:") {
        if let Some(resolved) = super::orchestrator::resolve_tool_target(inner, &ev.session_id, tool_name).await {
            info!(
                "[Хост] Net: инструмент '{}' разрешён в {} (приоритет: Уровень {})",
                tool_name, resolved.target, resolved.tier
            );
            target = resolved.target;
        }
    }
    // Подменяем target в самом событии, чтобы он ушёл с разрешённым
    // адресом (host:<node>:tool:<name>) во все нижележащие ветки (FIB/pinned).
    let fwd_ev = {
        let mut e = (*ev).clone();
        e.target = target.clone();
        e
    };
    let ev = &fwd_ev;

    // --- P5.1: маршрутизация через FIB по целевому узлу ---
    // Формат target: `host:<node_id>:<tool>` (см. NET-concept.md §6).
    if let Some(node_id) = target.strip_prefix("host:") {
        // Извлекаем <node_id> (до следующего ':').
        let target_node = node_id.split(':').next().unwrap_or(node_id);
        if let Some(next_hop) = route_next_hop(inner, target_node).await {
            // next_hop -> url исходящего соединения (P4: node_url мапа).
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
            // next_hop известен в FIB, но ещё нет соединения (capabilities не пришли) —
            // буферизуем по url, если он известен.
            if let Some(url) = inner.node_url.read().await.get(&next_hop).cloned() {
                let mut pending = inner.pending_outbound.write().await;
                let origin = origin_host.clone().unwrap_or_else(|| inner.cfg.node_id.clone());
                let ttl = decrement_ttl(16).unwrap_or(0);
                pending.entry(url.clone()).or_default().push(pack_event_with_ttl(&origin, 0, ev, ttl));
                info!("[Хост] Net: событие {} буферизовано для {} (next-hop {}, нет соединения)", ev.target, url, next_hop);
                return true;
            }
            // Нет соединения и URL неизвестен — откладываем (FIB пересчитается при Hello).
            info!("[Хост] Net: нет соединения к next-hop {} для {}", next_hop, target_node);
        } else {
            info!("[Хост] Net: нет маршрута в FIB к узлу {} (target {})", target_node, target);
        }
        // FIB не дал маршрута — пробуем fallback (П5.3) ниже.
    }

    // --- П5.2: возврат ответа по входящему соединению (backward-compat П1) ---
    // Если событие — ответ на запрос, пришедший с известного origin (входящее
    // соединение), возвращаем его обратно по этому соединению.
    if let Some(origin) = origin_host.as_ref() {
        let incoming = inner.incoming_senders.read().await;
        if let Some(tx) = incoming.get(origin) {
            let my_id = inner.cfg.node_id.clone();
            let ttl = decrement_ttl(16).unwrap_or(0);
            let payload = pack_event_with_ttl(&my_id, 0, ev, ttl);
            if tx.try_send(payload).is_ok() {
                info!("[Хост] Net: ответ {} возвращён по входящему соединению {} (origin)", ev.target, origin);
                return true;
            }
        }
    }

    // --- П5.3: ручной pinned-routing по targets из конфига (backward-compat) ---
    let remotes = inner.cfg.remotes.clone();
    let target_remote = remotes.into_iter().find(|r| {
        r.targets.iter().any(|t| {
            t == &target || (target.starts_with(t) && target.as_bytes().get(t.len()) == Some(&b':'))
        })
    });

    if let Some(remote) = target_remote {
        let origin = origin_host.clone().unwrap_or_else(|| inner.cfg.node_id.clone());
        let ttl = decrement_ttl(16).unwrap_or(0);
        let payload = pack_event_with_ttl(&origin, 0, ev, ttl);

        let outbound = inner.outbound.read().await;
        if let Some(tx) = outbound.get(&remote.url) {
            if tx.try_send(payload.clone()).is_ok() {
                info!("[Хост] Net: отправлено событие {} на {} (pinned targets)", ev.target, remote.url);
                return true;
            }
        }
        drop(outbound);
        // Соединения ещё нет — буферизуем.
        let mut pending = inner.pending_outbound.write().await;
        pending.entry(remote.url.clone()).or_default().push(payload);
        info!("[Хост] Net: событие {} буферизовано для {} (нет соединения)", ev.target, remote.url);
        return true;
    }

    // --- П5.4: локальное событие (tool:/mcp:/agent: и т.п.) — не форвардим ---
    false
}
