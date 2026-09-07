//! Приоритезация инструментов (Origin-Aware, см. docs/tool-prioritization.md).
//!
//! Когда агент просит инструмент по имени (`tool:<name>`), хост должен
//! выбрать наиболее подходящий узел сети, соблюдая иерархию:
//!   1. Узел-источник (тот, кто инициировал сессию) — минимум задержки/загрузки.
//!   2. Любой другой узел сети (mesh) — выбирается кратчайший путь по FIB.
//!
//! Примечание: локальный приоритет (Tier 2) обрабатывается в шине ДО вызова
//! `forward` (matched_plugins не пуст → событие не уходит в сеть), поэтому
//! здесь реализованы только сетевые уровни (источник + mesh).

use super::*;
use super::lsdb::route_next_hop;

/// Результат разрешения инструмента: сетевой target и уровень приоритета.
/// `tier` = 1 (узел-источник) или 3 (другой узел сети).
#[derive(Debug, PartialEq)]
pub(crate) struct ResolvedTarget {
    pub target: String,
    pub tier: u8,
}

/// Разрешить имя инструмента `tool_name` в сетевой target `host:<node_id>:tool:<name>`
/// по иерархии приоритетов. Если подходящий узел не найден — `None`.
///
/// `session_id` используется для определения узла-источника (Tier 1).
pub(crate) async fn resolve_tool_target(
    inner: &Arc<NetInner>,
    session_id: &str,
    tool_name: &str,
) -> Option<ResolvedTarget> {
    // --- Tier 1: инструмент на узле-источнике (сессия пришла от него) ---
    let source_id = {
        let s = inner.session_origin.read().await;
        s.get(session_id).cloned()
    };
    if let Some(source) = &source_id {
        let origin_tools = inner.origin_tools.read().await;
        if let Some(tools) = origin_tools.get(source) {
            if tools.iter().any(|t| t.name == tool_name) {
                // Источник обладает инструментом → возвращаем его (приоритет 1).
                return Some(ResolvedTarget {
                    target: format!("host:{}:tool:{}", source, tool_name),
                    tier: 1,
                });
            }
        }
        drop(origin_tools);
    }

    // --- Tier 3: инструмент на любом другом узле сети (mesh) ---
    // Перебираем все известные узлы (кроме источника и себя) и выбираем
    // того, у кого инструмент есть. Предпочитаем узлы с маршрутом в FIB
    // (кратчайший путь); если ни у одного нет маршрута — берём первый
    // подходящий (форвард сам буферизует при отсутствии соединения).
    let origin_tools = inner.origin_tools.read().await;
    let mut reachable: Vec<String> = Vec::new();
    let mut any: Vec<String> = Vec::new();
    for (node_id, tools) in origin_tools.iter() {
        if let Some(src) = &source_id {
            if node_id == src {
                continue; // источник уже проверен (Tier 1)
            }
        }
        if node_id == &inner.cfg.node_id {
            continue; // себя не шлём в сеть (локально обработал бы шина)
        }
        if tools.iter().any(|t| t.name == tool_name) {
            if route_next_hop(inner, node_id).await.is_some() {
                reachable.push(node_id.clone());
            } else {
                any.push(node_id.clone());
            }
        }
    }
    drop(origin_tools);

    // Сначала узлы с маршрутом (кратчайший путь), затем любые.
    for node_id in reachable.into_iter().chain(any.into_iter()) {
        return Some(ResolvedTarget {
            target: format!("host:{}:tool:{}", node_id, tool_name),
            tier: 3,
        });
    }

    None
}
