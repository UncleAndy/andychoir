//! Wire-протокол сетевого моста (P1): сериализация/десериализация сообщений,
//! преобразование Event <-> JSON, упаковка событий и capabilities.

use crate::ai::host::types::Event;
use crate::plugin::engine::ToolDef;

/// Сетевое сообщение: событие ИЛИ анонс возможностей (capabilities) хоста.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum NetMessage {
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
        tools: Vec<ToolDef>,
    },
    /// Приветствие (discovery): анонс node_id + соседей + инструменты.
    /// Flooding по сети для построения полной топологии (LSDB).
    /// seq — монотонный счётчик источника (защита от петель flooding в циклах).
    Hello {
        source_id: String,
        seq: u64,
        neighbors: Vec<String>,
        tools: Vec<ToolDef>,
    },
    /// Уведомление об уходе хоста (graceful shutdown).
    Bye {
        source_id: String,
    },
    /// Аутентификация входящего/исходящего соединения (B1).
    /// Первое сообщение после установки WS-соединения: клиент шлёт токен,
    /// сервер (входящая сторона) сверяет его с `NetConfig.token`.
    /// Токен НЕ хранится в логах (только факт успеха/неудачи).
    Auth {
        token: String,
    },
}

/// Event -> JSON Value (поля record Event).
pub(crate) fn event_to_value(ev: &Event) -> serde_json::Value {
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
pub(crate) fn value_to_event(v: &serde_json::Value) -> Option<Event> {
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
pub(crate) fn pack_event(origin: &str, hop: u8, ev: &Event) -> String {
    let ttl: u8 = 16; // значение по умолчанию при первой упаковке
    pack_event_with_ttl(origin, hop, ev, ttl)
}

/// Упаковать событие с явно заданным TTL (P2: для декремента при пересылке).
pub(crate) fn pack_event_with_ttl(origin: &str, hop: u8, ev: &Event, ttl: u8) -> String {
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
pub(crate) fn decrement_ttl(ttl: u8) -> Option<u8> {
    ttl.checked_sub(1)
}

/// Упаковать анонс возможностей (локальные инструменты хоста).
/// P1: добавлены source_id и neighbors (из LSDB).
pub(crate) fn pack_capabilities(origin: &str, tools: Vec<ToolDef>) -> String {
    let msg = NetMessage::Capabilities {
        origin: origin.to_string(),
        source_id: origin.to_string(),
        neighbors: Vec::new(), // P3: заполняется из LSDB
        tools,
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

/// Распаковать сетевое сообщение.
pub(crate) fn unpack_message(text: &str) -> Option<NetMessage> {
    serde_json::from_str(text).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // B1: NetMessage::Auth сериализуется с тегом type="auth" и полем token,
    // и корректно десериализуется обратно через unpack_message.
    #[test]
    fn auth_roundtrip_serialization() {
        let msg = NetMessage::Auth {
            token: "secret-token-123".to_string(),
        };
        let s = serde_json::to_string(&msg).expect("serialize Auth");
        // Тег по rename_all = snake_case → "auth".
        assert!(s.contains("\"type\":\"auth\""), "тег должен быть auth, got: {s}");
        assert!(s.contains("\"token\":\"secret-token-123\""), "поле token должно быть");

        let back = unpack_message(&s).expect("deserialize Auth");
        match back {
            NetMessage::Auth { token } => assert_eq!(token, "secret-token-123"),
            other => panic!("ожидался Auth, получили: {other:?}"),
        }
    }

    // B1: Auth не конфликтует с другими вариантами (тип уникален).
    #[test]
    fn auth_tag_distinct_from_bye() {
        let auth = serde_json::json!({"type":"auth","token":"x"}).to_string();
        let bye = serde_json::json!({"type":"bye","source_id":"node"}).to_string();
        match (unpack_message(&auth), unpack_message(&bye)) {
            (Some(NetMessage::Auth { .. }), Some(NetMessage::Bye { .. })) => {}
            other => panic!("неожиданные варианты: {other:?}"),
        }
    }
}
