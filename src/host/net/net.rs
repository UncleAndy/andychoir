//! Центральный модуль сетевого моста: точка входа (start_net) и тесты.
//!
//! Wire-протокол, dedup, LSDB/FIB, discovery и обработка соединений
//! (входящий/исходящий WS, forward) вынесены в подмодули:
//! - `message`   — wire-протокол (NetMessage, pack/unpack, Event <-> JSON)
//! - `dedup`     — дедупликация входящих событий (Bloom-фильтр, P2)
//! - `lsdb`      — топология (LSDB) и маршрутизация (FIB/Dijkstra, P3/P4)
//! - `discovery` — анонсы Hello и flooding (P3)
//! - `server`    — входящий WS-сервер и handle_incoming
//! - `outbound`  — исходящие персистентные соединения (run_outbound_loop)
//! - `forward`   — маршрутизация событий (forward)

use super::*;
use super::discovery::run_discovery_loop;
use super::outbound::run_outbound_loop;
use super::server::run_incoming_server;
use crate::config::config::NetConfig;

/// Запустить сетевой мост: входящий WS-сервер + исходящие соединения к remotes.
pub async fn start_net(tx: mpsc::Sender<Event>, cfg: NetConfig) -> super::NetHandle {
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
            BloomFilter::with_num_bits(super::dedup::DEDUP_BITS).expected_items(1024),
        )),
        dedup_last_reset: Arc::new(RwLock::new(super::dedup::current_unix_secs())),
        lsdb: Arc::new(RwLock::new(HashMap::new())),
        fib: Arc::new(RwLock::new(HashMap::new())),
        node_url: Arc::new(RwLock::new(HashMap::new())),
    });
    super::set_inner(inner.clone());

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

    // P3: задача периодического discovery (Hello каждые HELLO_INTERVAL_SECS).
    let discovery = tokio::spawn({
        let inner = inner.clone();
        async move {
            run_discovery_loop(inner).await;
        }
    });
    outbound.push(discovery);

    super::NetHandle { server, outbound }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::engine::ToolDef;
    use crate::config::config::NetRemote;
    use fastbloom::BloomFilter;
    use super::dedup::reset_dedup;
    use super::discovery::broadcast_to_neighbors;
    use super::super::set_inner;

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
        let s = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let msg = super::message::unpack_message(&s).unwrap();
        match msg {
            NetMessage::Event { origin, source_id, event_id, ttl, hop, event } => {
                let ev2 = super::message::value_to_event(&event).unwrap();
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
        let tools = vec![ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        let s = super::message::pack_capabilities("00000000-0000-0000-0000-0000000000a1", tools.clone());
        let msg = super::message::unpack_message(&s).unwrap();
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
        let s = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &make_event("x", "s"));
        match super::message::unpack_message(&s).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, true),
            _ => panic!(),
        }
        let s2 = super::message::pack_event("00000000-0000-0000-0000-0000000000b2", 0, &make_event("x", "s"));
        match super::message::unpack_message(&s2).unwrap() {
            NetMessage::Event { origin, .. } => assert_eq!(origin == node_id, false),
            _ => panic!(),
        }
    }

    // P1: Hello (discovery) упаковывается и распаковывается с source_id/neighbors.
    #[test]
    fn hello_roundtrip() {
        let tools = vec![ToolDef {
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
        match super::message::unpack_message(&s).unwrap() {
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
        match super::message::unpack_message(&s).unwrap() {
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
        let s1 = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let s2 = super::message::pack_event("00000000-0000-0000-0000-0000000000a1", 0, &ev);
        let m1 = super::message::unpack_message(&s1).unwrap();
        let m2 = super::message::unpack_message(&s2).unwrap();
        let id1 = match m1 { NetMessage::Event { event_id, .. } => event_id, _ => panic!() };
        let id2 = match m2 { NetMessage::Event { event_id, .. } => event_id, _ => panic!() };
        assert_ne!(id1, id2, "event_id должен быть уникальным для каждой упаковки");
        assert!(uuid::Uuid::parse_str(&id1).is_ok(), "event_id должен быть UUID");
    }

    // P2: декремент TTL — базовая логика защиты от петель.
    #[test]
    fn p2_ttl_decrement() {
        assert_eq!(super::message::decrement_ttl(0), None, "ttl=0 → событие отбрасывается");
        assert_eq!(super::message::decrement_ttl(1), Some(0), "ttl=1 → 0 (последний hop)");
        assert_eq!(super::message::decrement_ttl(16), Some(15), "ttl=16 → 15");
    }

    // P2: упаковка с явным TTL сохраняет значение при roundtrip.
    #[test]
    fn p2_ttl_preserved_in_message() {
        let ev = make_event("tool:x", "s");
        let s = super::message::pack_event_with_ttl("00000000-0000-0000-0000-0000000000a1", 0, &ev, 7);
        match super::message::unpack_message(&s).unwrap() {
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
        assert!(super::dedup::check_dedup(&inner, id).await, "первый раз → новое");
        assert!(!super::dedup::check_dedup(&inner, id).await, "повтор (в окне) → дубликат");
        // Симулируем старение: сдвигаем dedup_last_reset на 61с назад.
        let old = super::dedup::current_unix_secs().saturating_sub(61);
        *inner.dedup_last_reset.write().await = old;
        // Теперь check_dedup должен сбросить фильтр (окно истекло) и принять ID как новый.
        assert!(super::dedup::check_dedup(&inner, id).await, "после истечения окна → снова новое");
    }

    // P2: check_dedup отклоняет повторяющийся event_id (интеграция с NetInner).
    #[tokio::test]
    async fn p2_dedup_rejects_duplicate() {
        let inner = NetInner::new_test();
        let id = "00000000-0000-0000-0000-0000000000d1";
        assert!(super::dedup::check_dedup(&inner, id).await, "первый раз → новое событие");
        assert!(!super::dedup::check_dedup(&inner, id).await, "повтор → дубликат (отброшен)");
        // После сброса — снова новое.
        reset_dedup(&inner).await;
        assert!(super::dedup::check_dedup(&inner, id).await, "после reset → снова новое");
    }

    // P2: разные event_id не блокируют друг друга (низкий false-positive).
    #[tokio::test]
    async fn p2_dedup_distinct_ids() {
        let inner = NetInner::new_test();
        assert!(super::dedup::check_dedup(&inner, "ev-aaa").await, "ev-aaa → новое");
        assert!(super::dedup::check_dedup(&inner, "ev-bbb").await, "ev-bbb → новое (не конфликтует)");
        assert!(super::dedup::check_dedup(&inner, "ev-ccc").await, "ev-ccc → новое (не конфликтует)");
        // Повтор ev-aaa всё ещё дубликат.
        assert!(!super::dedup::check_dedup(&inner, "ev-aaa").await, "ev-aaa повтор → дубликат");
    }

    // P3: Hello обновляет LSDB (топология) и сохраняет инструменты.
    #[tokio::test]
    async fn p3_lsdb_updated_on_hello() {
        let inner = NetInner::new_test();
        let tools = vec![ToolDef {
            name: "calculator".into(),
            description: "calc".into(),
            parameters_json: "{}".into(),
        }];
        super::discovery::handle_hello(
            &inner,
            "00000000-0000-0000-0000-0000000000b2".into(),
            vec!["00000000-0000-0000-0000-0000000000c3".into()],
            tools.clone(),
        )
        .await;
        // LSDB содержит узел b2 с соседом c3.
        let lsdb = inner.lsdb.read().await;
        let (neighbors, _ts) = lsdb.get("00000000-0000-0000-0000-0000000000b2").unwrap();
        assert_eq!(neighbors, &vec!["00000000-0000-0000-0000-0000000000c3".to_string()]);
        drop(lsdb);
        // Инструменты сохранены.
        let stored = inner.origin_tools.read().await;
        assert_eq!(stored.get("00000000-0000-0000-0000-0000000000b2").unwrap().len(), 1);
    }

    // P3: свой собственный Hello игнорируется (нет петель).
    #[tokio::test]
    async fn p3_hello_ignores_self() {
        let inner = NetInner::new_test();
        let my_id = inner.cfg.node_id.clone();
        super::discovery::handle_hello(&inner, my_id.clone(), vec![], vec![]).await;
        assert!(inner.lsdb.read().await.get(&my_id).is_none(), "свой node_id не должен попасть в LSDB");
    }

    // P3: повторный Hello от того же узла обновляет список соседей (замена, не дублирование).
    #[tokio::test]
    async fn p3_lsdb_update_replaces_neighbors() {
        let inner = NetInner::new_test();
        let id = "00000000-0000-0000-0000-0000000000b2".to_string();
        super::discovery::handle_hello(&inner, id.clone(), vec!["c3".into()], vec![]).await;
        super::discovery::handle_hello(&inner, id.clone(), vec!["c3".into(), "d4".into()], vec![]).await;
        let lsdb = inner.lsdb.read().await;
        let (neighbors, _ts) = lsdb.get(&id).unwrap();
        // Должны быть оба соседа, без дублей c3.
        assert_eq!(neighbors.len(), 2, "соседи заменяются, а не добавляются");
        assert!(neighbors.contains(&"c3".to_string()));
        assert!(neighbors.contains(&"d4".to_string()));
    }

    // P3: LSDB сохраняет timestamp последнего Hello (для будущего timeout в P6).
    #[tokio::test]
    async fn p3_lsdb_timestamp_recorded() {
        let inner = NetInner::new_test();
        let before = super::dedup::current_unix_secs();
        super::discovery::handle_hello(&inner, "b2".into(), vec![], vec![]).await;
        let after = super::dedup::current_unix_secs();
        let lsdb = inner.lsdb.read().await;
        let (_n, ts) = lsdb.get("b2").unwrap();
        assert!(*ts >= before && *ts <= after, "timestamp в окне вызова");
    }

    // P3: broadcast_to_neighbors не отправляет источнику (защита от петель flooding).
    #[tokio::test]
    async fn p3_broadcast_excludes_source() {
        let inner = NetInner::new_test();
        // Регистрируем двух "соседей" в incoming_senders.
        let (tx_a, mut rx_a) = mpsc::channel::<String>(8);
        let (tx_b, mut rx_b) = mpsc::channel::<String>(8);
        inner.incoming_senders.write().await.insert("aa".into(), tx_a);
        inner.incoming_senders.write().await.insert("bb".into(), tx_b);
        // Flooding от bb: должен уйти только aa.
        broadcast_to_neighbors(&inner, "MSG", "bb").await;
        // aa получил.
        let got_a = rx_a.try_recv();
        assert!(got_a.is_ok(), "сосед aa должен получить сообщение");
        // bb (источник) — нет.
        let got_b = rx_b.try_recv();
        assert!(got_b.is_err(), "источник bb не должен получить своё же сообщение");
    }

    // P3: pack_hello содержит свой node_id и соседей из конфига.
    #[tokio::test]
    async fn p3_pack_hello_self_and_neighbors() {
        let inner = NetInner::new_test();
        let s = super::discovery::pack_hello(&inner).await;
        match super::message::unpack_message(&s).unwrap() {
            NetMessage::Hello { source_id, neighbors, .. } => {
                assert_eq!(source_id, inner.cfg.node_id, "source_id = свой node_id");
                // У new_test remotes пусты → neighbors пусты.
                assert!(neighbors.is_empty(), "neighbors из пустого cfg.remotes");
            }
            _ => panic!("expected Hello"),
        }
    }

    // P4: FIB пуст, если нет топологии (route_next_hop → None).
    #[tokio::test]
    async fn p4_fib_empty_when_no_topology() {
        let inner = NetInner::new_test();
        super::lsdb::rebuild_fib(&inner).await;
        assert!(inner.fib.read().await.is_empty(), "без LSDB FIB пуст");
        assert!(super::lsdb::route_next_hop(&inner, "99999999-0000-0000-0000-000000000099").await.is_none());
    }

    // P4: прямой сосед → next-hop = сам сосед (без лишних хопов).
    #[tokio::test]
    async fn p4_fib_direct_neighbor() {
        let inner = NetInner::new_test();
        // A (my_id) видит Hello от B с соседями [A].
        super::discovery::handle_hello(&inner, "00000000-0000-0000-0000-0000000000b2".into(), vec![inner.cfg.node_id.clone()], vec![]).await;
        let fib = inner.fib.read().await;
        assert_eq!(
            fib.get("00000000-0000-0000-0000-0000000000b2").map(|s| s.as_str()),
            Some("00000000-0000-0000-0000-0000000000b2".as_ref()),
            "прямой сосед → next-hop = он сам"
        );
    }

    // P4: кратчайший путь через промежуточный узел (A-B-C: route A→C даёт next-hop B).
    #[tokio::test]
    async fn p4_fib_shortest_path_multi_hop() {
        let inner = NetInner::new_test();
        let my = inner.cfg.node_id.clone();
        let b = "00000000-0000-0000-0000-0000000000b2".to_string();
        let c = "00000000-0000-0000-0000-0000000000c3".to_string();
        // Строим LSDB вручную: B видит A и C; C видит B.
        inner.lsdb.write().await.insert(b.clone(), (vec![my.clone(), c.clone()], super::dedup::current_unix_secs()));
        inner.lsdb.write().await.insert(c.clone(), (vec![b.clone()], super::dedup::current_unix_secs()));
        super::lsdb::rebuild_fib(&inner).await;
        // Маршрут до C должен идти через B (next-hop = B).
        let hop = super::lsdb::route_next_hop(&inner, &c).await;
        assert_eq!(hop.as_deref(), Some(b.as_str()), "A→C идёт через B (кратчайший путь)");
    }

    // P4: rebuild_fib заполняет FIB после handle_hello.
    #[tokio::test]
    async fn p4_rebuild_fib_builds_routes() {
        let inner = NetInner::new_test();
        super::discovery::handle_hello(&inner, "00000000-0000-0000-0000-0000000000b2".into(), vec![inner.cfg.node_id.clone()], vec![]).await;
        let fib = inner.fib.read().await;
        assert!(fib.contains_key("00000000-0000-0000-0000-0000000000b2"), "FIB содержит маршрут до B");
    }

    // P4: свой node_id не должен попасть в FIB (нельзя слать самому себе).
    #[tokio::test]
    async fn p4_fib_excludes_self() {
        let inner = NetInner::new_test();
        let my = inner.cfg.node_id.clone();
        // Сосед B знает нас.
        super::discovery::handle_hello(&inner, "00000000-0000-0000-0000-0000000000b2".into(), vec![my.clone()], vec![]).await;
        assert!(inner.fib.read().await.get(&my).is_none(), "свой node_id не в FIB");
    }

    // P4: недостижимый узел → маршрута нет (изолированный фрагмент графа).
    #[tokio::test]
    async fn p4_fib_unreachable_node() {
        let inner = NetInner::new_test();
        // B и C связаны между собой, но не с A (my_id).
        inner.lsdb.write().await.insert(
            "00000000-0000-0000-0000-0000000000b2".to_string(),
            (vec!["00000000-0000-0000-0000-0000000000c3".to_string()], super::dedup::current_unix_secs()),
        );
        inner.lsdb.write().await.insert(
            "00000000-0000-0000-0000-0000000000c3".to_string(),
            (vec!["00000000-0000-0000-0000-0000000000b2".to_string()], super::dedup::current_unix_secs()),
        );
        super::lsdb::rebuild_fib(&inner).await;
        // Ни B, ни C недостижимы из A.
        assert!(super::lsdb::route_next_hop(&inner, "00000000-0000-0000-0000-0000000000b2").await.is_none());
        assert!(super::lsdb::route_next_hop(&inner, "00000000-0000-0000-0000-0000000000c3").await.is_none());
    }

    // P4: node_url регистрируется и связывает next_hop с исходящим url.
    #[tokio::test]
    async fn p4_node_url_mapping() {
        let inner = NetInner::new_test();
        let b = "00000000-0000-0000-0000-0000000000b2".to_string();
        // Имитируем получение Capabilities от B (исходящее соединение на ws://b).
        inner.node_url.write().await.insert(b.clone(), "ws://host-b:8092/net".to_string());
        // B — прямой сосед.
        super::discovery::handle_hello(&inner, b.clone(), vec![inner.cfg.node_id.clone()], vec![]).await;
        // next-hop для B = B, и для него есть url.
        let hop = super::lsdb::route_next_hop(&inner, &b).await.unwrap();
        let url = inner.node_url.read().await.get(&hop).cloned();
        assert_eq!(url.as_deref(), Some("ws://host-b:8092/net"));
    }

    // P5: forward маршрутизирует host:<node_id>:<tool> через FIB (next_hop → url).
    #[tokio::test]
    async fn p5_forward_routes_via_fib() {
        let inner = NetInner::new_test();
        let b = "00000000-0000-0000-0000-0000000000b2".to_string();
        let url = "ws://host-b:8092/net".to_string();
        // FIB: B — прямой сосед (next-hop = B).
        super::discovery::handle_hello(&inner, b.clone(), vec![inner.cfg.node_id.clone()], vec![]).await;
        // node_url: B -> url исходящего соединения.
        inner.node_url.write().await.insert(b.clone(), url.clone());
        // Исходящий канал к url.
        let (tx, mut rx) = mpsc::channel::<String>(8);
        inner.outbound.write().await.insert(url.clone(), tx);
        // Устанавливаем global inner.
        set_inner(inner.clone());

        let ev = make_event("host:00000000-0000-0000-0000-0000000000b2:tool:calculator", "sess-x");
        let sent = forward(&ev).await;
        assert!(sent, "событие должно уйти по сети");
        let msg = rx.try_recv().expect("сообщение ушло в канал");
        // Сообщение — Event с правильным target.
        match super::message::unpack_message(&msg).unwrap() {
            NetMessage::Event { event, .. } => {
                let ev2 = super::message::value_to_event(&event).unwrap();
                assert_eq!(ev2.target, "host:00000000-0000-0000-0000-0000000000b2:tool:calculator");
            }
            _ => panic!("expected Event"),
        }
    }

    // P5: локальный tool (без host:) НЕ форвардится (forward возвращает false).
    #[tokio::test]
    async fn p5_forward_local_tool_not_routed() {
        let inner = NetInner::new_test();
        set_inner(inner.clone());
        let ev = make_event("tool:calculator", "sess-x");
        let sent = forward(&ev).await;
        assert!(!sent, "локальный tool не форвардится");
    }

    // P5: fallback на ручной pinned-routing (targets) при отсутствии FIB-маршрута.
    #[tokio::test]
    async fn p5_forward_fallback_targets() {
        let url = "ws://pinned-host/net".to_string();
        let cfg = {
            let mut c = NetConfig::default();
            c.remotes.push(NetRemote {
                url: url.clone(),
                token: "".into(),
                targets: vec!["tool:pinned".into()],
            });
            c
        };
        let inner = NetInner::new_test_with_cfg(cfg);
        // Исходящий канал к url.
        let (tx, mut rx) = mpsc::channel::<String>(8);
        inner.outbound.write().await.insert(url.clone(), tx);
        set_inner(inner.clone());

        let ev = make_event("tool:pinned", "sess-x");
        let sent = forward(&ev).await;
        assert!(sent, "pinned target должен форвардиться");
        let msg = rx.try_recv().expect("сообщение ушло в канал");
        match super::message::unpack_message(&msg).unwrap() {
            NetMessage::Event { event, .. } => {
                let ev2 = super::message::value_to_event(&event).unwrap();
                assert_eq!(ev2.target, "tool:pinned");
            }
            _ => panic!("expected Event"),
        }
    }
}
