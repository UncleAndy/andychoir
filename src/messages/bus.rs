use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use wasmtime::Engine;

use crate::ai::host::types::Event;
use crate::exports::ai::host::plugin_lifecycle::Guest;
use crate::plugin::engine::{ChoirHostState, PluginInstance, new_plugin_store};

pub type PluginRegistry = Arc<RwLock<HashMap<String, PluginInstance>>>;

pub struct EventBusConfig {
    pub event_queue_size: usize,
    pub thread_pool_size: usize,
    pub max_fuel_for_call: u64,
    pub session_timeout: u64,
    pub session_check_period: u64,
}

pub struct EventBusHandle {
    shutdown_tx: watch::Sender<bool>,
    worker_tx: mpsc::Sender<EventJob>,
    dispatcher_handle: JoinHandle<()>,
    worker_handles: Vec<JoinHandle<()>>,
}

struct SessionSlot {
    store: Arc<Mutex<wasmtime::Store<ChoirHostState>>>,
    last_used: std::time::Instant,
}

struct EventJob {
    plugin_name: String,
    store_key: (String, String),
    lifecycle: Guest,
    event: Event,
}

pub fn start_event_bus(
    plugins: PluginRegistry,
    mut rx: mpsc::Receiver<Event>,
    tx: mpsc::Sender<Event>,
    engine: Engine,
    config: EventBusConfig,
) -> EventBusHandle {
    let stores = Arc::new(DashMap::<(String, String), SessionSlot>::new());

    let worker_count = config.thread_pool_size.max(1);
    let (worker_tx, worker_rx) = mpsc::channel::<EventJob>(config.event_queue_size);
    let worker_rx = Arc::new(Mutex::new(worker_rx));
    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let worker_rx = worker_rx.clone();
        let stores_worker = stores.clone();
        let max_fuel_for_call = config.max_fuel_for_call;
        worker_handles.push(tokio::spawn(async move {
            loop {
                let job = {
                    let mut rx = worker_rx.lock().await;
                    rx.recv().await
                };

                let Some(job) = job else {
                    break;
                };

                if let Some(mut slot) = stores_worker.get_mut(&job.store_key) {
                    slot.last_used = std::time::Instant::now();
                    let store_arc = slot.store.clone();
                    drop(slot);

                    let mut store_guard = store_arc.lock().await;
                    let _ = store_guard.set_fuel(max_fuel_for_call);

                    let handle_event_res = store_guard
                        .run_concurrent(async |accessor| {
                            job.lifecycle.call_handle_event(accessor, job.event).await
                        })
                        .await;

                    if let Err(e) = handle_event_res {
                        println!(
                            "[Хост] Ошибка при выполнении плагина {}: {:?}",
                            job.plugin_name, e
                        );
                    }
                } else {
                    println!("[Хост] Сессия не найдена для ключа {:?}", job.store_key);
                }
            }

            println!("[Хост] Исполнитель событий {} завершён.", worker_id);
        }));
    }

    let stores_cleanup = stores.clone();
    tokio::spawn(async move {
        let timeout = std::time::Duration::from_secs(config.session_timeout);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(config.session_check_period)).await;

            let now = std::time::Instant::now();
            stores_cleanup.retain(|_, slot| now.duration_since(slot.last_used) < timeout);
        }
    });

    let worker_tx_loop = worker_tx.clone();
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

            dispatch_event(event, &plugins, &stores, &engine, &tx, &worker_tx_loop).await;
        }

        println!("[Хост] Диспетчер событий завершён.");
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
            println!("[Хост] Диспетчер событий завершился с ошибкой: {:?}", err);
        }

        for worker_handle in self.worker_handles {
            if let Err(err) = worker_handle.await {
                println!("[Хост] Исполнитель событий завершился с ошибкой: {:?}", err);
            }
        }
    }
}

async fn dispatch_event(
    event: Event,
    plugins: &PluginRegistry,
    stores: &Arc<DashMap<(String, String), SessionSlot>>,
    engine: &Engine,
    tx: &mpsc::Sender<Event>,
    worker_tx: &mpsc::Sender<EventJob>,
) {
    let targets = event.target.split_whitespace().collect::<Vec<_>>();
    for target_pattern in targets {
        let matched_plugins = match_plugins(plugins, target_pattern).await;

        for plugin_name in matched_plugins {
            let current_store_key = match store_key_for_event(
                plugins,
                stores,
                engine,
                tx,
                &plugin_name,
                &event.session_id,
            )
            .await
            {
                Some(key) => key,
                None => continue,
            };

            let plugin_res = {
                let lock = plugins.read().await;
                lock.get(&plugin_name).map(|p| p.lifecycle.clone())
            };

            if let Some(lifecycle) = plugin_res {
                let job = EventJob {
                    plugin_name: plugin_name.clone(),
                    store_key: current_store_key,
                    lifecycle,
                    event: event.clone(),
                };

                if let Err(err) = worker_tx.try_send(job) {
                    println!(
                        "[Хост] Очередь пула исполнителей переполнена или закрыта, событие для плагина {} отклонено: {}",
                        plugin_name, err
                    );
                }
            } else {
                println!("[Хост] Не найден lifecycle плагина {}", plugin_name);
            }
        }
    }
}

async fn match_plugins(plugins: &PluginRegistry, target_pattern: &str) -> Vec<String> {
    if target_pattern.contains('*') {
        let pattern = target_pattern.replace("*", "");
        let lock = plugins.read().await;
        lock.keys()
            .filter(|name| name.starts_with(&pattern))
            .cloned()
            .collect()
    } else {
        let lock = plugins.read().await;
        if lock.contains_key(target_pattern) {
            vec![target_pattern.to_string()]
        } else {
            vec![]
        }
    }
}

async fn store_key_for_event(
    plugins: &PluginRegistry,
    stores: &Arc<DashMap<(String, String), SessionSlot>>,
    engine: &Engine,
    tx: &mpsc::Sender<Event>,
    plugin_name: &str,
    session_id: &str,
) -> Option<(String, String)> {
    let key = (plugin_name.to_string(), session_id.to_string());
    if stores.contains_key(&key) {
        return Some(key);
    }

    let plugin_config_opt = {
        let lock = plugins.read().await;
        lock.get(plugin_name).map(|p| p.config.clone())
    };

    let Some(config) = plugin_config_opt else {
        println!("Cannot find plugin: {}", plugin_name);
        return None;
    };

    match new_plugin_store(engine, &config, tx.clone()).await {
        Ok(store) => {
            stores.insert(
                key.clone(),
                SessionSlot {
                    store: Arc::new(Mutex::new(store)),
                    last_used: std::time::Instant::now(),
                },
            );
            Some(key)
        }
        Err(err) => {
            println!(
                "Error creating plugin store: {} (plugin: {}).",
                err, plugin_name
            );
            None
        }
    }
}
