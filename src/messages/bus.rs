use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock, Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use wasmtime::Engine;
use wasmtime::component::{Component, Linker};

use crate::{error, info, HostPlugin};
use crate::ai::host::types::Event;
use crate::exports::ai::host::plugin_lifecycle::Guest;
use crate::metrics::Metrics;
use crate::plugin::engine::{ChoirHostState, PluginInstance, new_plugin_store};

pub type PluginRegistry = Arc<RwLock<HashMap<String, PluginInstance>>>;

pub struct EventBusConfig {
    pub event_queue_size: usize,
    pub thread_pool_size: usize,
    pub max_fuel_for_call: u64,
    pub session_timeout: u64,
    pub session_check_period: u64,
    pub metrics: Arc<Metrics>,
}

pub struct EventBusHandle {
    shutdown_tx: watch::Sender<bool>,
    worker_tx: mpsc::Sender<EventJob>,
    dispatcher_handle: JoinHandle<()>,
    worker_handles: Vec<JoinHandle<()>>,
}

struct SessionSlot {
    store: Arc<Mutex<wasmtime::Store<ChoirHostState>>>,
    lifecycle: Guest,
    last_used: std::time::Instant,
}

struct EventJob {
    plugin_name: String,
    store: Arc<Mutex<wasmtime::Store<ChoirHostState>>>,
    lifecycle: Guest,
    event: Event,
}

pub fn start_event_bus(
    plugins: PluginRegistry,
    mut rx: mpsc::Receiver<Event>,
    tx: mpsc::Sender<Event>,
    engine: Engine,
    linker: Linker<ChoirHostState>,
    config: EventBusConfig,
) -> EventBusHandle {
    let stores = Arc::new(DashMap::<(String, String), SessionSlot>::new());

    let worker_count = config.thread_pool_size.max(1);
    let (worker_tx, worker_rx) = mpsc::channel::<EventJob>(config.event_queue_size);
    config.metrics.set_active_sessions(stores.len());
    config
        .metrics
        .set_incoming_queue_fill(0, config.event_queue_size);
    config
        .metrics
        .set_worker_queue_fill(0, config.event_queue_size);
    let metrics_worker = config.metrics.clone();
    let worker_queue_size = config.event_queue_size;
    let max_fuel_for_call = config.max_fuel_for_call;
    let worker_limiter = Arc::new(Semaphore::new(worker_count));
    let mut worker_handles = Vec::with_capacity(1);
    worker_handles.push(tokio::spawn(async move {
        let mut worker_rx = worker_rx;
        let mut worker_tasks = JoinSet::new();
        let mut worker_id = 0usize;

        loop {
            tokio::select! {
                Some(join_res) = worker_tasks.join_next(), if !worker_tasks.is_empty() => {
                    if let Err(err) = join_res {
                        error!("[Хост] Исполнитель событий завершился с ошибкой: {:?}", err);
                    }
                }
                job = worker_rx.recv() => {
                    let Some(job) = job else {
                        break;
                    };
                    metrics_worker.set_worker_queue_fill(worker_rx.len(), worker_queue_size);

                    let permit = match worker_limiter.clone().acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => break,
                    };
                    let metrics_worker = metrics_worker.clone();
                    let current_worker_id = worker_id;
                    worker_id = worker_id.wrapping_add(1);

                    worker_tasks.spawn(async move {
                        let _permit = permit;
                        process_event_job(current_worker_id, job, metrics_worker, max_fuel_for_call).await;
                    });
                }
            }
        }

        while let Some(join_res) = worker_tasks.join_next().await {
            if let Err(err) = join_res {
                error!("[Хост] Исполнитель событий завершился с ошибкой: {:?}", err);
            }
        }

        info!("[Хост] Исполнитель событий завершён.");
    }));

    let stores_cleanup = stores.clone();
    let metrics_cleanup = config.metrics.clone();
    tokio::spawn(async move {
        let timeout = std::time::Duration::from_secs(config.session_timeout);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(config.session_check_period)).await;

            let now = std::time::Instant::now();
            stores_cleanup.retain(|_, slot| now.duration_since(slot.last_used) < timeout);
            metrics_cleanup.set_active_sessions(stores_cleanup.len());
        }
    });

    let worker_tx_loop = worker_tx.clone();
    let metrics_loop = config.metrics.clone();
    let event_queue_size = config.event_queue_size;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let dispatcher_handle = tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                event = rx.recv() => event,
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
            };

            let Some(event) = event else {
                break;
            };
            metrics_loop.set_incoming_queue_fill(rx.len(), event_queue_size);

            dispatch_event(
                event,
                &plugins,
                &stores,
                &engine,
                &linker,
                &tx,
                &worker_tx_loop,
                &metrics_loop,
            )
            .await;
        }

        info!("[Хост] Диспетчер событий завершён.");
    });

    EventBusHandle {
        shutdown_tx,
        worker_tx,
        dispatcher_handle,
        worker_handles,
    }
}

impl EventBusHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        drop(self.worker_tx);

        if let Err(err) = self.dispatcher_handle.await {
            error!("[Хост] Диспетчер событий завершился с ошибкой: {:?}", err);
        }

        for worker_handle in self.worker_handles {
            if let Err(err) = worker_handle.await {
                error!("[Хост] Исполнитель событий завершился с ошибкой: {:?}", err);
            }
        }
    }
}

async fn process_event_job(
    worker_id: usize,
    job: EventJob,
    metrics: Arc<Metrics>,
    max_fuel_for_call: u64,
) {
    info!(
        "[Хост] Worker {} получил Job для отправки в плагин ивента: {:?}",
        worker_id, job.event
    );

    let mut store_guard = job.store.lock().await;
    let _ = store_guard.set_fuel(max_fuel_for_call);

    let started_at = std::time::Instant::now();
    let handle_event_res = store_guard
        .run_concurrent(async |accessor| job.lifecycle.call_handle_event(accessor, job.event).await)
        .await;
    metrics.observe_processing_time(&job.plugin_name, started_at.elapsed());

    if let Err(e) = handle_event_res {
        error!(
            "[Хост] Ошибка при выполнении плагина {}: {:?}",
            job.plugin_name, e
        );
    }
}

async fn dispatch_event(
    event: Event,
    plugins: &PluginRegistry,
    stores: &Arc<DashMap<(String, String), SessionSlot>>,
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    tx: &mpsc::Sender<Event>,
    worker_tx: &mpsc::Sender<EventJob>,
    metrics: &Arc<Metrics>,
) {
    let targets = parse_target_names(&event.target);
    for target_pattern in targets {
        let matched_plugins = match_plugins(plugins, &target_pattern).await;
        info!(
            "[Хост] Найдены плагины для получения сообщения: {:?}",
            matched_plugins
        );

        for plugin_name in matched_plugins {
            info!("[Хост] Исполнение плагина {} ({:?})", plugin_name, event);
            let Some((store, lifecycle)) = store_for_event(
                plugins,
                stores,
                engine,
                linker,
                tx,
                &plugin_name,
                &event.session_id,
                metrics,
            )
            .await
            else {
                let plugins_lock = plugins.read().await;
                error!(
                    "[Хост] Не удалось получить хранилище и жизненный цикл плагина {} ({:?})",
                    plugin_name,
                    plugins_lock.keys()
                );
                continue;
            };

            info!("[Хост] Подготовка Job для {}", plugin_name);

            let job = EventJob {
                plugin_name: plugin_name.clone(),
                store,
                lifecycle,
                event: event.clone(),
            };

            info!("[Хост] Отправка Job для {}", plugin_name);

            if let Err(err) = worker_tx.try_send(job) {
                metrics.inc_rejected_event(&plugin_name);
                error!(
                    "[Хост] Очередь пула исполнителей переполнена или закрыта, событие для плагина {} отклонено: {}",
                    plugin_name, err
                );
            }
            metrics.set_worker_queue_fill(
                worker_tx.max_capacity() - worker_tx.capacity(),
                worker_tx.max_capacity(),
            );
        }
    }
}

/// Чистая (без блокировок) логика матчинга имени плагина по шаблону target.
/// Выделена из `match_plugins` для юнит-тестирования.
///
/// Правила:
/// - `*`            -> все зарегистрированные плагины
/// - `"agent:*"`    -> плагины, чьё имя начинается на префикс до `*` (здесь "agent:")
/// - точное имя    -> только если присутствует в `known`
pub(crate) fn match_plugin_names(known: &[String], target_pattern: &str) -> Vec<String> {
    if target_pattern == "*" {
        known.to_vec()
    } else if target_pattern.contains('*') {
        let prefix = target_pattern.replace('*', "");
        known.iter()
            .filter(|name| name.starts_with(&prefix))
            .cloned()
            .collect()
    } else if known.iter().any(|name| name == target_pattern) {
        vec![target_pattern.to_string()]
    } else {
        vec![]
    }
}

/// Распарс поля `target` события: несколько имён/масок разделены пробелами.
/// Выделено для юнит-тестирования.
pub(crate) fn parse_target_names(target: &str) -> Vec<String> {
    target
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

async fn match_plugins(plugins: &PluginRegistry, target_pattern: &str) -> Vec<String> {
    let lock = plugins.read().await;
    let known: Vec<String> = lock.keys().cloned().collect();
    drop(lock);
    match_plugin_names(&known, target_pattern)
}

async fn store_for_event(
    plugins: &PluginRegistry,
    stores: &Arc<DashMap<(String, String), SessionSlot>>,
    engine: &Engine,
    linker: &Linker<ChoirHostState>,
    tx: &mpsc::Sender<Event>,
    plugin_name: &str,
    session_id: &str,
    metrics: &Arc<Metrics>,
) -> Option<(Arc<Mutex<wasmtime::Store<ChoirHostState>>>, Guest)> {
    let plugin_instance_opt = {
        let lock = plugins.read().await;
        lock.get(plugin_name)
            .map(|p| (p.config.clone(), p.store.clone(), p.lifecycle.clone()))
    };

    let Some((config, plugin_store, plugin_lifecycle)) = plugin_instance_opt else {
        error!("[Хост] Палин не найден: {}", plugin_name);
        return None;
    };

    if config.allow_background {
        return Some((plugin_store, plugin_lifecycle));
    }

    let key = (plugin_name.to_string(), session_id.to_string());
    if let Some(mut slot) = stores.get_mut(&key) {
        slot.last_used = std::time::Instant::now();
        return Some((slot.store.clone(), slot.lifecycle.clone()));
    }

    match new_plugin_store(engine, &config, tx.clone()).await {
        Ok(mut store) => {
            let component = match Component::from_file(engine, config.file.clone()) {
                Ok(component) => component,
                Err(err) => {
                    error!(
                        "[Хост] Ошибка загрузки файла плагина: {} (плагин: {}).",
                        err, plugin_name
                    );
                    return None;
                }
            };

            let plugin = match HostPlugin::instantiate_async(&mut store, &component, linker).await {
                Ok(plugin) => plugin,
                Err(err) => {
                    error!(
                        "[Хост] Ошибка инициализации плагина: {} (плагин: {}).",
                        err, plugin_name
                    );
                    return None;
                }
            };

            let lifecycle = plugin.ai_host_plugin_lifecycle().clone();

            let store = Arc::new(Mutex::new(store));
            stores.insert(
                key,
                SessionSlot {
                    store: store.clone(),
                    lifecycle: lifecycle.clone(),
                    last_used: std::time::Instant::now(),
                },
            );
            metrics.set_active_sessions(stores.len());
            Some((store, lifecycle))
        }
        Err(err) => {
            error!(
                "[Хост] Ошибка создания хранилища плагина: {} (плагин: {}).",
                err, plugin_name
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- match_plugin_names -------------------------------------------------
    #[test]
    fn match_wildcard_returns_all() {
        let known = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let got = match_plugin_names(&known, "*");
        assert_eq!(got.len(), 3);
        assert!(got.contains(&"a".to_string()));
        assert!(got.contains(&"b".to_string()));
        assert!(got.contains(&"c".to_string()));
    }

    #[test]
    fn match_prefix_star_returns_matching() {
        let known = vec![
            "agent:one".to_string(),
            "agent:two".to_string(),
            "tool:calc".to_string(),
        ];
        // "agent:*" -> префикс "agent:"
        let got = match_plugin_names(&known, "agent:*");
        assert_eq!(got.len(), 2);
        assert!(got.contains(&"agent:one".to_string()));
        assert!(got.contains(&"agent:two".to_string()));
        assert!(!got.contains(&"tool:calc".to_string()));
    }

    #[test]
    fn match_exact_name_present() {
        let known = vec!["front:console".to_string(), "agent:coder".to_string()];
        let got = match_plugin_names(&known, "front:console");
        assert_eq!(got, vec!["front:console".to_string()]);
    }

    #[test]
    fn match_exact_name_absent_returns_empty() {
        let known = vec!["front:console".to_string()];
        // Точное имя, которого нет -> пусто (не искать по подстроке)
        let got = match_plugin_names(&known, "agent:missing");
        assert!(got.is_empty());
    }

    #[test]
    fn match_empty_pattern_returns_empty() {
        let known = vec!["a".to_string()];
        let got = match_plugin_names(&known, "");
        assert!(got.is_empty());
    }

    #[test]
    fn match_prefix_with_no_match_returns_empty() {
        let known = vec!["tool:calc".to_string()];
        let got = match_plugin_names(&known, "agent:*");
        assert!(got.is_empty());
    }

    // --- parse_target_names -------------------------------------------------
    #[test]
    fn parse_single_target() {
        assert_eq!(parse_target_names("front:console"), vec!["front:console".to_string()]);
    }

    #[test]
    fn parse_multiple_targets_split_by_whitespace() {
        let got = parse_target_names("agent:one agent:two tool:calc");
        assert_eq!(
            got,
            vec![
                "agent:one".to_string(),
                "agent:two".to_string(),
                "tool:calc".to_string()
            ]
        );
    }

    #[test]
    fn parse_empty_target_returns_empty() {
        assert!(parse_target_names("").is_empty());
        assert!(parse_target_names("   ").is_empty());
    }

    #[test]
    fn parse_extra_spaces_deduplicated_to_tokens() {
        // Несколько пробелов между именами -> всё равно 2 токена
        let got = parse_target_names("a   b");
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }
}
