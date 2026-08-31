//! P3/P4: Link-State Database (топология) и FIB (маршрутизация по Дейкстре).
//!
//! LSDB заполняется из Hello-сообщений (discovery). FIB пересчитывается
//! методом Дейкстры: кратчайший путь от своего node_id к каждому узлу,
//! сохраняется next-hop (первый узел на пути).

use super::NetInner;
use crate::info;
use petgraph::algo::dijkstra;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Graph;
use std::collections::HashMap;
use std::sync::Arc;

/// P4: вес каждого ребра графа (все рёбра равны — однородная сеть).
pub(crate) const GRAPH_EDGE_WEIGHT: u32 = 1;

/// P4: Пересчитать FIB (Forwarding Information Base) из LSDB методом Дейкстры.
///
/// Для каждого известного узла строим граф (узлы = хосты, рёбра = соседство из LSDB),
/// запускаем dijkstra от своего node_id и сохраняем next-hop для каждого целевого узла.
/// Если цель — прямой сосед, next-hop = цель. Иначе — первый узел на кратчайшем пути.
pub(crate) async fn rebuild_fib(inner: &Arc<NetInner>) {
    let lsdb = inner.lsdb.read().await;
    // Собираем множество всех узлов и рёбер.
    // Узлы = ключи LSDB + self + ВСЕ соседи, упомянутые в LSDB (узел может быть
    // достижим через next-hop даже до того, как сам анонсировал Hello — mesh).
    let mut nodes: Vec<String> = lsdb.keys().cloned().collect();
    for (_node, (neighbors, _)) in lsdb.iter() {
        for nb in neighbors {
            if !nodes.contains(nb) {
                nodes.push(nb.clone());
            }
        }
    }
    nodes.push(inner.cfg.node_id.clone());
    nodes.sort();
    nodes.dedup();

    let mut graph = Graph::<String, u32>::new();
    let mut idx: HashMap<String, NodeIndex> = HashMap::new();
    for n in &nodes {
        idx.insert(n.clone(), graph.add_node(n.clone()));
    }
    for (node, (neighbors, _)) in lsdb.iter() {
        for nb in neighbors {
            if let (Some(&a), Some(&b)) = (idx.get(node), idx.get(nb)) {
                // Ребро в обе стороны (directed-граф эмулирует неориентированный для Дейкстры).
                graph.add_edge(a, b, GRAPH_EDGE_WEIGHT);
                graph.add_edge(b, a, GRAPH_EDGE_WEIGHT);
            }
        }
    }

    let my_id = inner.cfg.node_id.clone();
    let Some(&start) = idx.get(&my_id) else {
        return;
    };
    let dist = dijkstra(&graph, start, None, |e| *e.weight());

    let mut fib = HashMap::new();
    for (node, &ni) in idx.iter() {
        if node == &my_id {
            continue;
        }
        if let Some(d) = dist.get(&ni) {
            if *d == 0 {
                continue;
            }
            let mut cur = ni;
            let mut next_hop = node.clone();
            loop {
                let mut found = None;
                for edge in graph.edges(cur) {
                    let other = if edge.source() == cur { edge.target() } else { edge.source() };
                    if let Some(od) = dist.get(&other) {
                        if *od + GRAPH_EDGE_WEIGHT == *dist.get(&cur).unwrap_or(&u32::MAX) {
                            found = Some(other);
                            break;
                        }
                    }
                }
                match found {
                    Some(parent) if parent != start => {
                        cur = parent;
                        next_hop = graph[parent].clone();
                    }
                    _ => break,
                }
            }
            fib.insert(node.clone(), next_hop);
        }
    }
    drop(lsdb);
    *inner.fib.write().await = fib;
    info!("[Хост] Net: FIB пересчитан ({} маршрутов)", inner.fib.read().await.len());
}

/// P4: Найти next-hop для target_node по FIB.
/// Возвращает node_id следующего узла или None, если нет маршрута.
pub(crate) async fn route_next_hop(inner: &Arc<NetInner>, target_node: &str) -> Option<String> {
    inner.fib.read().await.get(target_node).cloned()
}
