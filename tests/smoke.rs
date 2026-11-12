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

const REQUIRED_PLUGINS: [&str; 3] = [
    "plugins/front_http_plugin/target/wasm32-wasip2/debug/front_http_plugin.wasm",
    "plugins/tool_calculator_plugin/target/wasm32-wasip2/debug/tool_calculator_plugin.wasm",
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

    calls.fetch_add(1, Ordering::SeqCst);

    let empty: Vec<serde_json::Value> = Vec::new();
    let messages = request
        .get("messages")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or(empty);
    let tool_result = messages
        .iter()
        .rev()
        .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("tool"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .map(|s| s.to_string());

    let message = match tool_result {
        Some(content) => json!({ "role": "assistant", "content": format!("RESULT:{}", content) }),
        None => json!({
            "role": "assistant",
            "content": serde_json::Value::Null,
            "tool_calls": [{
                "id": "call_smoke",
                "type": "function",
                "function": { "name": "calculator", "arguments": "{\"expression\":\"2+2\"}" }
            }]
        }),
    };
    let response = json!({
        "id": "chatcmpl-smoke",
        "object": "chat.completion",
        "created": 0,
        "model": request.get("model").and_then(|m| m.as_str()).unwrap_or("mock"),
        "choices": [{ "index": 0, "message": message, "finish_reason": "stop" }],
    });

    let payload = response.to_string();
    let http = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        payload.len(),
        payload
    );
    stream.write_all(http.as_bytes())?;
    stream.flush()
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

    assert_eq!(
        data.get("body").and_then(|b| b.as_str()),
        Some("RESULT:4"),
        "агент должен вернуть результат calculator через tool-calling: {data}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "ожидалось ровно два обращения к LLM: tool_call и финальный ответ"
    );
}
