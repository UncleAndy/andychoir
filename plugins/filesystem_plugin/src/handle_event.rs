use crate::ai::host::types::Event;
use crate::ai::host::event_bus::publish_event;
use crate::{TOOLS, PLUGIN_NAME};

pub async fn handle_event(ev: Event) {
    log_debug!("[WASM] FS-plugin: получен ивент: {:?}, topic: {}", ev, ev.topic);

    match ev.topic.as_str() {
        "discovery" => {
            let cfg_guard = TOOLS.lock().unwrap();
            let tools = cfg_guard.clone().unwrap_or_default();
            let defs_json = serde_json::to_string(&tools).unwrap();
            publish_event(&Event {
                request_id: ev.request_id,
                session_id: ev.session_id,
                source: PLUGIN_NAME.to_string(),
                target: ev.source,
                topic: "definition".to_string(),
                payload: defs_json,
            });
        }
        "request" => {
            let request_res: Result<serde_json::Value, serde_json::Error> = serde_json::from_str(&ev.payload);
            let args = match request_res {
                Ok(val) => val,
                Err(e) => {
                    publish_event(&Event {
                        request_id: ev.request_id,
                        session_id: ev.session_id,
                        source: PLUGIN_NAME.to_string(),
                        target: ev.source,
                        topic: "response".to_string(),
                        payload: format!("(ошибка: не удалось распарсить аргументы: {})", e),
                    });
                    return;
                }
            };

            let tool_name = args.get("tool").and_then(|n| n.as_str()).unwrap_or("");

            let res_payload = match tool_name {
                "fs-read" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::read_file(path).await {
                        Ok(c) => c,
                        Err(e) => format!("(ошибка read_file: {})", e),
                    }
                }
                "fs-write" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    let content = args.get("contents").and_then(|c| c.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::write_file(path, content).await {
                        Ok(_) => "Успешно записано".to_string(),
                        Err(e) => format!("(ошибка write_file: {})", e),
                    }
                }
                "fs-append" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    let content = args.get("contents").and_then(|c| c.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::append_file(path, content).await {
                        Ok(_) => "Успешно добавлено".to_string(),
                        Err(e) => format!("(ошибка append_file: {})", e),
                    }
                }
                "fs-remove" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::remove_file(path).await {
                        Ok(_) => "Успешно удалено".to_string(),
                        Err(e) => format!("(ошибка remove_file: {})", e),
                    }
                }
                "fs-mkdir" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::make_dir(path).await {
                        Ok(_) => "Каталог создан".to_string(),
                        Err(e) => format!("(ошибка make_dir: {})", e),
                    }
                }
                "fs-rmdir" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::remove_dir(path).await {
                        Ok(_) => "Каталог удалён".to_string(),
                        Err(e) => format!("(ошибка remove_dir: {})", e),
                    }
                }
                "fs-move" => {
                    let src = args.get("src").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    let dst = args.get("dst").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::move_path(src, dst).await {
                        Ok(_) => "Успешно перемещено".to_string(),
                        Err(e) => format!("(ошибка move_path: {})", e),
                    }
                }
                "fs-copy" => {
                    let src = args.get("src").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    let dst = args.get("dst").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::copy_file(src, dst).await {
                        Ok(_) => "Успешно скопировано".to_string(),
                        Err(e) => format!("(ошибка copy_file: {})", e),
                    }
                }
                "fs-list" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::list_dir(path).await {
                        Ok(l) => l,
                        Err(e) => format!("(ошибка list_dir: {})", e),
                    }
                }
                "fs-stat" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::stat_path(path).await {
                        Ok(s) => s,
                        Err(e) => format!("(ошибка stat_path: {})", e),
                    }
                }
                "fs-patch" => {
                    let path = args.get("path").and_then(|p| p.as_str()).unwrap_or("").to_string();
                    let old = args.get("old").and_then(|o| o.as_str()).unwrap_or("").to_string();
                    let new = args.get("new").and_then(|n| n.as_str()).unwrap_or("").to_string();
                    match crate::ai::host::host_control::patch_file(path, old, new).await {
                        Ok(_) => "Правка успешно применена".to_string(),
                        Err(e) => format!("(ошибка patch_file: {})", e),
                    }
                }
                _ => format!("(ошибка: неизвестный FS-инструмент '{}')", tool_name),
            };

            publish_event(&Event {
                request_id: ev.request_id,
                session_id: ev.session_id,
                source: PLUGIN_NAME.to_string(),
                target: ev.source,
                topic: "response".to_string(),
                payload: res_payload,
            });
        }
        _ => {
            log_debug!("[WASM] FS-plugin игнорирует топик: {}", ev.topic);
        }
    }
}
