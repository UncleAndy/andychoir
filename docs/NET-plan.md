# План реализации Mesh-сети (на базе `docs/NET-concept.md`)

> Статус: черновик плана. Код пока НЕ меняется — обсуждаем.
> Принцип из концепта: **децентрализованно, без root-узла, без ручной топологии**;
> пользователь настраивает только прямые линки (`remotes` в конфиге).

---

## 0. Контекст и цель

Текущий `src/host/net.rs` — это «звезда»: `forward()` выбирает remote по
ручному списку `targets` из конфига (`NetRemote.targets`). Нет автообнаружения
соседей, нет маршрутизации к произвольному хосту, нет dedup/TTL на уровне сети.

Цель по `NET-concept.md`:
- **Discovery** — каждый хост строит LSDB (Link-State DB) через `HELLO`/`BYE`.
- **Routing** — shortest-path (Dijkstra на LSDB), next-hop пересылка.
- **Dedup** — `fastbloom` (окно 60с, 512 Б, фиксированная память).
- **TTL** — 1 байт, декремент на хопе.
- **Идентификаторы** — `source_id`/`event_id`/`node_id` = **UUID v4/v7**, не строки.

Архитектурное правило (не нарушать): плагины и шина НЕ знают о сети;
вся сетевая логика живёт только в `net.rs` (мост).

---

## 1. Зависимости (Cargo.toml хоста)

```toml
[dependencies]
uuid = { version = "1", features = ["v4", "v7"] }   # уже likely есть
fastbloom = { version = "0.10", features = ["xxh3"] } # dedup (SIMD)
petgraph = "0.6"                                      # graph + dijkstra
# twox-hash не нужен напрямую — внутри fastbloom (xxh3 feature)
```

> Примечание: `petgraph`/`fastbloom` — чисто Rust-крейты, обычно НЕ требуют
> правок `flake.nix`. Если при сборке вылезет необходимость тяжёлой нативной
> зависимости — сообщу отдельно (правило: flake.nix меняю только по согласованию).

---

## 2. Фазы

### P0 — Идентификаторы: UUID вместо строк (фундамент)

**Файлы:** `src/host/net.rs`, `src/config/config.rs` (`NetConfig.node_id`),
`src/host/mod.rs` (где `node_id` генерируется/читается).

- `NetConfig.node_id: String` оставить типом `String`, но **семантически** —
  это UUID. При запуске: если `node_id` в конфиге пустой/отсутствует —
  генерировать `uuid::Uuid::new_v4()` (или v7) и логировать его.
- Заменить в тестах `\"host-a\"` → реальный UUID (или helper `fn uuid(s: &str)` для
  детерминизма тестов).
- **Проверка:** `cargo test -p andychoir --lib` проходит; `node_id` —
  валидный UUID (проверка через `Uuid::parse_str`).

### P1 — Wire-протокол: расширить `NetMessage`

**Файл:** `src/host/net.rs` (`enum NetMessage`).

Добавить/изменить варианты:
```rust
#[serde(tag = "type", rename_all = "snake_case")]
enum NetMessage {
    // Существующий EVENT, но с новыми полями:
    Event {
        source_id: String,   // UUID оригинального отправителя
        event_id: String,    // UUID события (для Bloom filter)
        ttl: u8,             // Time-To-Live
        hop: u8,             // (оставить для диагностики)
        event: serde_json::Value,
    },
    // Анонс возможностей — расширить соседями:
    Capabilities {
        source_id: String,
        tools: Vec<ToolDef>,
        neighbors: Vec<String>,  // ← НОВОЕ: кого этот хост видит напрямую
    },
    // НОВОЕ: топологический обмен
    Hello {
        source_id: String,
        neighbors: Vec<String>,
        capabilities: Vec<ToolDef>,
    },
    Bye {
        source_id: String,
    },
}
```

- `pack_event()` / `unpack_message()` — добавить `source_id`, `event_id`, `ttl`.
- `event_id` генерируется в `forward()` (или при создании события): `Uuid::new_v4()`.

### P2 — Dedup (Bloom filter) + TTL

**Файл:** `src/host/net.rs` (`NetInner` + `handle_incoming` / `run_outbound_loop`).

- В `NetInner` добавить поле:
  ```rust
  dedup: Arc<RwLock<FastBloom>>,  // 4096 бит, k=3, xxh3
  ```
  Инициализация: `FastBloom::with_num_bits_and_hasher(4096, 3, xxh3_64)`.
- При получении `Event`:
  1. Если `ttl == 0` → drop (лог: "TTL expired").
  2. `if dedup.check_and_add(event_id)` → drop (лог: "duplicate").
  3. Иначе декремент `ttl` и продолжаем обработку.
- Периодический сброс (окно 60с) — отдельная задача `tokio::spawn`:
  ```rust
  let mut interval = tokio::time::interval(Duration::from_secs(60));
  loop { interval.tick().await; dedup.write().await.clear(); }
  ```
- **Проверка:** unit-тест на `check_and_add` (дубликат детектится, новый пропускается);
  интеграционный: два одинаковых `event_id` → второй дропается.

### P3 — Discovery: LSDB + Hello protocol

**Файл:** `src/host/net.rs` (`NetInner` + новые задачи).

- В `NetInner` добавить:
  ```rust
  lsdb: Arc<RwLock<HashMap<String, LsdbNode>>,  // source_id -> узел
  ```
  ```rust
  struct LsdbNode {
      neighbors: Vec<String>,
      capabilities: Vec<ToolDef>,
      last_seen: Instant,
  }
  ```
- При установлении соединения (и каждые N сек) — отправлять `Hello` со своим
  `node_id` + списком прямых соседей + свои tools.
- При получении `Hello` от соседа:
  1. Обновить LSDB (узел + его neighbors).
  2. Flooding: переслать `Hello` другим соседям (если инфо новая) — **основа
     link-state распространения**.
  3. Пересчитать маршрутную таблицу (см. P4).
- Задача periodic hello: `tokio::time::interval(Duration::from_secs(10))`.
- **Flooding защита:** используем тот же Bloom filter (P2) — `Hello` тоже имеет
  `event_id` (или отдельный seq), чтобы не зациклиться.
- **Проверка:** 3 хоста A-B-C, A видит B, B видит C → через 2 раунда A знает о C
  (LSDB содержит все 3 узла). Тест на сходимость LSDB.

### P4 — Routing: shortest-path (Dijkstra) + FIB

**Файл:** `src/host/net.rs` (новый модуль `routing.rs` или inline).

- Построить `petgraph::UnGraph<String, u32>` из LSDB (узлы = host_id, рёбра =
  neighbors, вес = 1).
- `dijkstra(&g, target, None, |_| 1)` → карта расстояний.
- FIB (Forwarding Information Base): для каждого известного `target_id` —
  `next_hop` = сосед с минимальным расстоянием.
- Хранить FIB в `NetInner`: `fib: Arc<RwLock<HashMap<String, String>>>`.
  Пересчитывать при каждом обновлении LSDB.
- **Проверка:** unit-тест — граф A-B-C, target=C → next_hop для A = B.
  unit-тест — graph из petgraph строится корректно.

### P5 — Forward переписать под маршрутизацию

**Файл:** `src/host/net.rs` (`forward()`).

Текущий `forward()` выбирает remote по `cfg.remotes[].targets`. Заменить на:
1. Если `ev.target` — локальный плагин (как сейчас) → не форвардим.
2. Иначе смотрим FIB: `fib.get(&ev.target_host)` → `next_hop`.
3. Если `next_hop` — это прямой сосед (есть `outbound[next_hop]` или
   `incoming_senders[next_hop]`) → шлём туда (как сейчас, но по next_hop).
4. Если `next_hop` неизвестен → drop (или буфер, как сейчас `pending_outbound`).
5. **Обратная совместимость П1** (возврат ответа по входящему соединению) —
   сохранить: если target = известный `origin_host` из `request_origin` →
   шлём по `incoming_senders[origin]`.

> Важно: поле `NetRemote.targets` из конфига **больше не нужно для маршрутизации**,
> но оставить его опциональным для явного pinned-роутинга (backward-compat).

### P6 — Failure detection + BYE

**Файл:** `src/host/net.rs`.

- В `Hello` loop: если сосед не слал `Hello` за `N` сек (например, 30с) →
  удалить из LSDB, пересчитать FIB.
- Graceful shutdown: при `drop()` моста — отправить `Bye` всем соседям.
- Обработка `Bye` — удалить узел из LSDB, пересчитать FIB.
- **Проверка:** интеграционный — убить хост B → A и C через ≤30с видят
  исчезновение B из LSDB, FIB пересчитан.

### P7 — Тесты

- Unit (в `net.rs` `mod tests`):
  - UUID валидация (`node_id`).
  - Bloom filter dedup (P2).
  - Dijkstra next-hop (P4).
  - HELLO flooding сходимость LSDB (P3) — in-memory, без сети.
- Integration (против живого стенда, как раньше):
  - 3 хоста A-B-C, агент на A вызывает tool на C → маршрут A→B→C работает.
  - Перезапуск B → маршрут перестраивается.
- Все тесты через `make` (правило пользователя: «используй make для сборки»).

### P8 — Документация

- Обновить `docs/NET-concept.md`/`.ru.md` при необходимости (если реализация
  отклонится от концепта).
- Добавить примеры конфигов (только `remotes` = прямые линки, без `targets`).

---

## 3. Риски и решения

| Риск | Решение |
|------|---------|
| Flooding HELLO зацикливается | Bloom filter (P2) на `Hello.event_id` |
| petgraph/fastbloom требуют flake.nix | Сообщу отдельно, не применяю автоматом |
| Смешение старых (`targets`) и новых маршрутов | `targets` опционален, FIB приоритетнее |
| Рассинхрон FIB при быстрых изменениях топологии | TTL=16 как страховка от петель |
| `node_id` как UUID ломает старые конфиги | Backward-compat: пустой `node_id` → генерируем UUID |

---

## 4. Критерии готовности

- [ ] `node_id` — UUID (P0).
- [ ] `NetMessage` имеет `Hello`/`Bye`/`event_id`/`ttl`/`source_id` (P1).
- [ ] Bloom filter dedup + окно сброса работают (P2).
- [ ] LSDB строится автоматически через HELLO (P3).
- [ ] FIB считает shortest-path через Dijkstra (P4).
- [ ] `forward()` маршрутизирует по FIB, не по ручным `targets` (P5).
- [ ] Failure detection + BYE (P6).
- [ ] Unit + integration тесты проходят (P7).
- [ ] `make` сборка без ошибок и warnings.

---

## 5. Порядок выполнения (предложение)

P0 → P1 → P2 → P3 → P4 → P5 → P6 → P7 → P8.

Каждая фаза — отдельный шаг; после фазы — пауза на подтверждение
(правило пользователя: «работаю по фазам, после фазы спрашиваю что дальше»).

**С чего начать:** P0 (UUID для `node_id`) — маленький, безопасный фундамент.
