//! Хостовый транспорт для MCP-клиента (плагин mcp:client).
//!
//! Плагин mcp:client реализует логику MCP (initialize, tools/list, tools/call,
//! протокол JSON-RPC), а хост предоставляет ТОЛЬКО транспорт: спавнит
//! stdio-подпроцесс MCP-сервера, гоняет JSON-RPC строки. Плагин не имеет
//! прямого сетевого/процессного доступа (песочница), поэтому транспорт — на
//! стороне хоста (как http.post_json / console / log).
//!
//! Дизайн: синхронный JSON-RPC запрос-ответ. `request(id, jsonrpc, timeout)`
//! пишет строку в stdin подпроцесса и ждёт ответ с совпадающим `id` из stdout.
//! MCP stdio = newline-delimited JSON-RPC.

use crate::{error, info, warn};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex, RwLock};

/// Один открытый stdio-транспорт MCP-сервера.
pub struct McpSession {
    /// Пишем JSON-RPC строки в stdin подпроцесса.
    stdin: Mutex<tokio::process::ChildStdin>,
    /// Карта ожидающих ответов: jsonrpc-id -> Sender(строка ответа).
    pending: Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>,
}

/// Глобальное хранилище транспортов: transport_id -> McpSession.
static SESSIONS: OnceLock<RwLock<HashMap<String, Arc<McpSession>>>> = OnceLock::new();

fn sessions() -> &'static RwLock<HashMap<String, Arc<McpSession>>> {
    SESSIONS.get_or_init(|| RwLock::new(HashMap::new()))
}

fn next_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Открыть stdio-подпроцесс MCP-сервера. Возвращает transport-id или "-" при ошибке.
pub async fn stdio_open(command: &str, args: &[String]) -> String {
    let mut cmd = tokio::process::Command::new(command);
    cmd.args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()); // stderr сервера идёт в лог хоста

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            error!("[Хост] MCP stdio-open: не удалось запустить '{}': {}", command, e);
            return "-".to_string();
        }
    };

    let stdin = match child.stdin.take() {
        Some(s) => s,
        None => {
            error!("[Хост] MCP stdio-open: нет stdin у '{}'", command);
            return "-".to_string();
        }
    };
    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => {
            error!("[Хост] MCP stdio-open: нет stdout у '{}'", command);
            return "-".to_string();
        }
    };

    let id = next_id();
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let session = Arc::new(McpSession {
        stdin: Mutex::new(stdin),
        pending: pending.clone(),
    });

    // Задача чтения stdout: построчно, сопоставляет ответы с ожидающими.
    tokio::spawn({
        let id = id.clone();
        let pending = pending.clone();
        async move {
            let mut reader = BufReader::new(stdout).lines();
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        // Извлекаем id из JSON-RPC ответа.
                        let json_id: Option<String> = serde_json::from_str::<serde_json::Value>(trimmed)
                            .ok()
                            .and_then(|v| v.get("id").cloned())
                            .and_then(|v| match v {
                                serde_json::Value::String(s) => Some(s),
                                serde_json::Value::Number(n) => Some(n.to_string()),
                                _ => None,
                            });
                        if let Some(jid) = json_id {
                            let send = pending.lock().await.remove(&jid);
                            if let Some(tx) = send {
                                let _ = tx.send(trimmed.to_string()).await;
                                continue;
                            }
                        }
                        // Ответ без ожидающего — логируем (notification/иное).
                        crate::debug!("[Хост] MCP stdio {}: ответ без ожидающего: {}", id, trimmed);
                    }
                    Ok(None) => {
                        info!("[Хост] MCP stdio {}: подпроцесс завершился", id);
                        break;
                    }
                    Err(e) => {
                        error!("[Хост] MCP stdio {}: ошибка чтения: {}", id, e);
                        break;
                    }
                }
            }
        }
    });

    sessions().write().await.insert(id.clone(), session);
    info!("[Хост] MCP stdio-open: '{}' transport={}", command, id);
    id
}

/// Отправить JSON-RPC и дождаться ответа с совпадающим id. Возвращает ответ
/// или None при таймауте/ошибке.
pub async fn request(transport_id: &str, jsonrpc: &str, timeout_ms: u64) -> Option<String> {
    let session = {
        let map = sessions().read().await;
        map.get(transport_id).cloned()?
    };

    // Извлекаем id запроса (для сопоставления ответа).
    let req_id = serde_json::from_str::<serde_json::Value>(jsonrpc)
        .ok()
        .and_then(|v| v.get("id").cloned())
        .and_then(|v| match v {
            serde_json::Value::String(s) => Some(s),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        })?;

    let (tx, mut rx) = mpsc::channel::<String>(1);
    session.pending.lock().await.insert(req_id.clone(), tx);

    // Пишем в stdin.
    {
        let mut stdin = session.stdin.lock().await;
        let mut line = jsonrpc.to_string();
        line.push('\n');
        if let Err(e) = stdin.write_all(line.as_bytes()).await {
            error!("[Хост] MCP request: ошибка записи в stdin {}: {}", transport_id, e);
            session.pending.lock().await.remove(&req_id);
            return None;
        }
        let _ = stdin.flush().await;
    }

    // Ждём ответ с таймаутом.
    let duration = std::time::Duration::from_millis(timeout_ms);
    match tokio::time::timeout(duration, rx.recv()).await {
        Ok(Some(resp)) => Some(resp),
        _ => {
            warn!("[Хост] MCP request: таймаут/ошибка ожидания ответа id={} transport={}", req_id, transport_id);
            session.pending.lock().await.remove(&req_id);
            None
        }
    }
}

/// Закрыть транспорт (убить подпроцесс).
pub async fn close(transport_id: &str) {
    let session = sessions().write().await.remove(transport_id);
    if let Some(s) = session {
        // Закрытие stdin обычно завершает подпроцесс (для stdio MCP).
        let mut stdin = s.stdin.lock().await;
        let _ = stdin.shutdown().await;
        info!("[Хост] MCP close: transport={}", transport_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // stdio-open + request с echo-подпроцессом: подпроцесс читает строку и
    // возвращает её же. Проверяем, что JSON-RPC запрос-ответ работает.
    #[tokio::test]
    async fn stdio_request_echo() {
        // bash-скрипт: читает строку из stdin и печатает её в stdout.
        let args = vec![
            "-c".to_string(),
            "while IFS= read -r line; do printf '%s\\n' \"$line\"; done".to_string(),
        ];
        let tid = stdio_open("bash", &args).await;
        assert_ne!(tid, "-", "stdio-open должен вернуть id");

        let req = r#"{"jsonrpc":"2.0","id":"1","method":"ping"}"#.to_string();
        let resp = request(&tid, &req, 5000).await;
        assert_eq!(resp, Some(req.clone()), "echo должен вернуть то же самое");

        close(&tid).await;
    }
}
