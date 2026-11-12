//! Сквозной смоук-тест хоста: HTTP -> `front:http` -> `agent` -> `tool:calculator`
//! -> mock LLM -> HTTP-ответ.
//!
//! Зачем: юнит-тесты проверяют модули по отдельности, но не проверяют, что
//! собранный хост с собранными wasm-плагинами реально обслуживает запрос.
//! Здесь проверяется весь путь, включая право `network` (B2) на http-прокси.
//!
//! Требования: собранные плагины (`make plugins`) и конфиг `ci/smoke.yaml`.
//! Внешняя сеть не нужна — мок OpenAI-совместимого LLM поднимается прямо здесь
//! (std TcpListener на 127.0.0.1:18100).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

const HOST_PORT: u16 = 18090;
const MOCK_PORT: u16 = 18100;
const CONFIG: &str = "ci/smoke.yaml";

const REQUIRED_PLUGINS: [&str; 5] = [
    "plugins/front_http_plugin/target/wasm32-wasip2/debug/front_http_plugin.wasm",
    "plugins/tool_calculator_plugin/target/wasm32-wasip2/debug/tool_calculator_plugin.wasm",
    "plugins/tool_env_plugin/target/wasm32-wasip2/debug/tool_env_plugin.wasm",
    "plugins/tool_web_plugin/target/wasm32-wasip2/debug/tool_web_plugin.wasm",
    "plugins/agent_plugin/target/wasm32-wasip2/debug/agent_plugin.wasm",
];

/// Хост-процесс, который гасится при выходе из теста (в т.ч. при панике).
struct HostProcess(std::process::Child);

impl Drop for HostProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Обработать один HTTP-запрос мока и ответить OpenAI-совместимым JSON.
///
/// Логика мока: пока в `messages` нет результата инструмента — просим
/// `calculator(2+2)`; как только результат пришёл — возвращаем его модели
/// обратно в виде `RESULT:<значение>`. Так тест доказывает, что ответ
/// инструмента дошёл до LLM и вернулся клиенту, а не был подменён.
/// Handle one HTTP request at the mock. Three routes: the OpenAI-compatible
/// completion, a Tavily-compatible search endpoint, and a plain page.
fn serve_one(mut stream: TcpStream, calls: &AtomicUsize) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 1 << 20 {
            return Ok(());
        }
    };

    let headers = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let content_len = headers
        .lines()
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            lower
                .strip_prefix("content-length:")
                .and_then(|v| v.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while buf.len() < header_end + content_len {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = &buf[header_end..(header_end + content_len).min(buf.len())];
    let request: serde_json::Value = serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);

    let request_line = headers.lines().next().unwrap_or("");
    let mut line_parts = request_line.split_whitespace();
    let method = line_parts.next().unwrap_or("");
    let path = line_parts.next().unwrap_or("");

    let (status, content_type, payload) = match (method, path) {
        ("GET", "/page") => ("200 OK".to_string(), "text/plain", "HELLO-WEB".to_string()),
        ("POST", "/search") => (
            "200 OK".to_string(),
            "application/json",
            json!({
                "results": [{
                    "title": "andychoir-result",
                    "url": "http://127.0.0.1:18100/page",
                    "content": "search snippet"
                }]
            })
            .to_string(),
        ),
        ("POST", "/v1/chat/completions") => {
            calls.fetch_add(1, Ordering::SeqCst);
            (
                "200 OK".to_string(),
                "application/json",
                completion_body(&request),
            )
        }
        _ => (
            "404 Not Found".to_string(),
            "text/plain",
            format!("unexpected {method} {path}"),
        ),
    };

    let http = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        payload.len(),
        payload
    );
    stream.write_all(http.as_bytes())?;
    stream.flush()
}

/// OpenAI-compatible reply: six tool calls on the first round, then every tool
/// result joined by "|" so the assertion proves each tool really ran.
fn completion_body(request: &serde_json::Value) -> String {
    let empty: Vec<serde_json::Value> = Vec::new();
    let messages = request
        .get("messages")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or(empty);

    let tool_results: Vec<String> = messages
        .iter()
        .filter(|m| m.get("role").and_then(|r| r.as_str()) == Some("tool"))
        .filter_map(|m| {
            m.get("content")
                .and_then(|c| c.as_str())
                .map(|s| s.to_string())
        })
        .collect();

    let message = if tool_results.is_empty() {
        json!({
            "role": "assistant",
            "content": serde_json::Value::Null,
            "tool_calls": [
                { "id": "call_calc", "type": "function",
                  "function": { "name": "calculator", "arguments": "{\"expression\":\"2+2\"}" } },
                { "id": "call_env", "type": "function",
                  "function": { "name": "env:get", "arguments": "{\"name\":\"PATH\"}" } },
                { "id": "call_time", "type": "function",
                  "function": { "name": "env:time", "arguments": "{}" } },
                { "id": "call_search", "type": "function",
                  "function": { "name": "web:search", "arguments": "{\"query\":\"andychoir\"}" } },
                { "id": "call_fetch", "type": "function",
                  "function": { "name": "web:fetch", "arguments": "{\"url\":\"http://127.0.0.1:18100/page\"}" } },
                { "id": "call_denied", "type": "function",
                  "function": { "name": "web:fetch", "arguments": "{\"url\":\"http://127.0.0.1:18101/page\"}" } }
            ]
        })
    } else {
        json!({
            "role": "assistant",
            "content": format!("RESULT:{}", tool_results.join("|"))
        })
    };

    json!({
        "id": "chatcmpl-smoke",
        "object": "chat.completion",
        "created": 0,
        "model": request.get("model").and_then(|m| m.as_str()).unwrap_or("mock"),
        "choices": [{ "index": 0, "message": message, "finish_reason": "stop" }],
    })
    .to_string()
}

fn spawn_mock(listener: TcpListener, calls: Arc<AtomicUsize>) {
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let calls = calls.clone();
            std::thread::spawn(move || {
                let _ = serve_one(stream, &calls);
            });
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smoke_http_agent_tool_through_mock_llm() {
    for path in REQUIRED_PLUGINS {
        assert!(
            std::path::Path::new(path).exists(),
            "нет собранного плагина {path} — выполните `make plugins`"
        );
    }

    let listener = TcpListener::bind(("127.0.0.1", MOCK_PORT)).unwrap_or_else(|e| {
        panic!("порт мок-LLM {MOCK_PORT} занят (остановите прошлый прогон): {e}")
    });
    let calls = Arc::new(AtomicUsize::new(0));
    spawn_mock(listener, calls.clone());

    let child = std::process::Command::new(env!("CARGO_BIN_EXE_andychoir"))
        .arg("--config")
        .arg(CONFIG)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("не удалось запустить бинарь хоста");
    let mut host = HostProcess(child);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", HOST_PORT)).is_ok() {
            break;
        }
        if let Ok(Some(status)) = host.0.try_wait() {
            panic!("хост завершился ({status}) — смотрите logs/smoke_rCURRENT.log");
        }
        if Instant::now() > deadline {
            panic!("front:http не поднялся за 30 с — смотрите logs/smoke_rCURRENT.log");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client");
    let response = client
        .post(format!("http://127.0.0.1:{HOST_PORT}/query"))
        .json(&json!({ "q": "посчитай 2+2" }))
        .send()
        .await
        .expect("HTTP-запрос к front:http");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let data: serde_json::Value = response.json().await.expect("ответ не JSON");

    let body = data
        .get("body")
        .and_then(|b| b.as_str())
        .unwrap_or_default();
    let parts: Vec<&str> = body
        .strip_prefix("RESULT:")
        .unwrap_or_default()
        .split('|')
        .collect();
    assert_eq!(parts.len(), 6, "expected six tool results, got: {body}");

    // calculator(2+2)
    assert_eq!(parts[0], "4", "calculator result: {body}");

    // env:get("PATH") - readable only because the plugin holds the right, and
    // the host must not answer with a permission error.
    assert!(!parts[1].is_empty(), "env:get returned nothing: {body}");
    assert!(
        !parts[1].contains("запрещён") && !parts[1].contains("(ошибка"),
        "env:get denied or failed: {body}"
    );

    // env:time - ISO-8601 UTC computed inside the component.
    let time = parts[2];
    assert_eq!(time.len(), 20, "env:time is not ISO-8601: {time}");
    assert_eq!(&time[0..2], "20", "env:time year looks wrong: {time}");
    assert_eq!(&time[4..5], "-", "env:time date separator: {time}");
    assert_eq!(&time[10..11], "T", "env:time date/time separator: {time}");
    assert_eq!(&time[19..20], "Z", "env:time timezone suffix: {time}");

    // web:search - POST through the proxy to the mock search endpoint.
    assert!(
        parts[3].contains("andychoir-result"),
        "web:search did not return the mock result: {body}"
    );

    // web:fetch - GET through the proxy, body returned verbatim.
    assert_eq!(parts[4], "HELLO-WEB", "web:fetch returned: {body}");

    // web:fetch to a port outside the allowlist must be denied by the host:
    // this is the end-to-end test of the network policy.
    assert!(
        parts[5].contains("network access denied"),
        "web:fetch to an unlisted port was not denied: {body}"
    );

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "ожидалось ровно два обращения к LLM: tool_call и финальный ответ"
    );
}
