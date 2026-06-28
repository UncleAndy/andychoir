use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub update_interval_secs: u64,
    pub location: String,
}

#[derive(Default)]
pub struct Metrics {
    incoming_queue_fill_percent: AtomicU64,
    worker_queue_fill_percent: AtomicU64,
    rejected_events_total: AtomicU64,
    active_sessions: AtomicUsize,
    plugin_stats: DashMap<String, Arc<PluginMetrics>>,
}

#[derive(Default)]
struct PluginMetrics {
    processed_events_total: AtomicU64,
    processing_times_micros: std::sync::Mutex<Vec<u64>>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn set_incoming_queue_fill(&self, queued: usize, capacity: usize) {
        self.incoming_queue_fill_percent
            .store(fill_percent(queued, capacity), Ordering::Relaxed);
    }

    pub fn set_worker_queue_fill(&self, queued: usize, capacity: usize) {
        self.worker_queue_fill_percent
            .store(fill_percent(queued, capacity), Ordering::Relaxed);
    }

    pub fn inc_rejected_event(&self, plugin_name: &str) {
        self.rejected_events_total.fetch_add(1, Ordering::Relaxed);
        let stats = self.plugin_stats(plugin_name);
        stats.processed_events_total.fetch_add(0, Ordering::Relaxed);
    }

    pub fn set_active_sessions(&self, active_sessions: usize) {
        self.active_sessions
            .store(active_sessions, Ordering::Relaxed);
    }

    pub fn observe_processing_time(&self, plugin_name: &str, elapsed: Duration) {
        let stats = self.plugin_stats(plugin_name);
        stats.processed_events_total.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut values) = stats.processing_times_micros.lock() {
            values.push(elapsed.as_micros().min(u128::from(u64::MAX)) as u64);
        }
    }

    fn plugin_stats(&self, plugin_name: &str) -> Arc<PluginMetrics> {
        self.plugin_stats
            .entry(plugin_name.to_string())
            .or_insert_with(|| Arc::new(PluginMetrics::default()))
            .clone()
    }

    pub fn render_prometheus(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(
            output,
            "# TYPE andychoir_incoming_event_queue_fill_percent gauge"
        );
        let _ = writeln!(
            output,
            "andychoir_incoming_event_queue_fill_percent {}",
            self.incoming_queue_fill_percent.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "# TYPE andychoir_worker_event_queue_fill_percent gauge"
        );
        let _ = writeln!(
            output,
            "andychoir_worker_event_queue_fill_percent {}",
            self.worker_queue_fill_percent.load(Ordering::Relaxed)
        );
        let _ = writeln!(output, "# TYPE andychoir_rejected_events_total counter");
        let _ = writeln!(
            output,
            "andychoir_rejected_events_total {}",
            self.rejected_events_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(output, "# TYPE andychoir_active_sessions gauge");
        let _ = writeln!(
            output,
            "andychoir_active_sessions {}",
            self.active_sessions.load(Ordering::Relaxed)
        );

        let summaries: HashMap<String, ProcessingSummary> = self
            .plugin_stats
            .iter()
            .filter_map(|entry| {
                let values = entry.value().processing_times_micros.lock().ok()?.clone();
                Some((entry.key().clone(), ProcessingSummary::from_values(values)))
            })
            .collect();

        for entry in self.plugin_stats.iter() {
            let plugin = escape_label(entry.key());
            let _ = writeln!(
                output,
                "andychoir_plugin_processed_events_total{{plugin=\"{}\"}} {}",
                plugin,
                entry.value().processed_events_total.load(Ordering::Relaxed)
            );
            if let Some(summary) = summaries.get(entry.key()) {
                let _ = writeln!(
                    output,
                    "andychoir_plugin_event_processing_time_micros_avg{{plugin=\"{}\"}} {}",
                    plugin, summary.avg
                );
                let _ = writeln!(
                    output,
                    "andychoir_plugin_event_processing_time_micros_min{{plugin=\"{}\"}} {}",
                    plugin, summary.min
                );
                let _ = writeln!(
                    output,
                    "andychoir_plugin_event_processing_time_micros_max{{plugin=\"{}\"}} {}",
                    plugin, summary.max
                );
                let _ = writeln!(
                    output,
                    "andychoir_plugin_event_processing_time_micros_median{{plugin=\"{}\"}} {}",
                    plugin, summary.median
                );
            }
        }

        output
    }

    pub fn render_console(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "Метрики AndyChoir:");
        let _ = writeln!(
            output,
            "  Заполненность входящей очереди событий: {}%",
            self.incoming_queue_fill_percent.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "  Заполненность очереди исполнителей: {}%",
            self.worker_queue_fill_percent.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "  Отклонённые события: {}",
            self.rejected_events_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "  Активные сессии: {}",
            self.active_sessions.load(Ordering::Relaxed)
        );

        if self.plugin_stats.is_empty() {
            let _ = writeln!(output, "  Метрики плагинов: нет данных");
            return output;
        }

        let _ = writeln!(output, "  Метрики плагинов:");
        for entry in self.plugin_stats.iter() {
            let values = entry
                .value()
                .processing_times_micros
                .lock()
                .map(|values| values.clone())
                .unwrap_or_default();
            let summary = ProcessingSummary::from_values(values);
            let _ = writeln!(output, "    Плагин {}:", entry.key());
            let _ = writeln!(
                output,
                "      Обработанные события: {}",
                entry.value().processed_events_total.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "      Время обработки, мкс: avg={}, min={}, max={}, median={}",
                summary.avg, summary.min, summary.max, summary.median
            );
        }

        output
    }
}

struct ProcessingSummary {
    avg: u64,
    min: u64,
    max: u64,
    median: u64,
}

impl ProcessingSummary {
    fn from_values(mut values: Vec<u64>) -> Self {
        if values.is_empty() {
            return Self {
                avg: 0,
                min: 0,
                max: 0,
                median: 0,
            };
        }

        values.sort_unstable();
        let sum = values.iter().sum::<u64>();
        let len = values.len();

        Self {
            avg: sum / len as u64,
            min: values[0],
            max: values[len - 1],
            median: values[len / 2],
        }
    }
}

pub fn start_metrics_exporter(
    metrics: Arc<Metrics>,
    config: MetricsConfig,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        return None;
    }

    Some(tokio::spawn(async move {
        let addr = format!("{}:{}", config.host, config.port);
        let listener = match TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(err) => {
                error!(
                    "[Метрики] Не удалось запустить экспорт на {}: {}",
                    addr, err
                );
                return;
            }
        };

        info!(
            "[Метрики] Экспорт запущен: http://{}{} (интервал обновления {} сек.)",
            addr, config.location, config.update_interval_secs
        );

        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                continue;
            };
            let body = metrics.render_prometheus();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    }))
}

fn fill_percent(queued: usize, capacity: usize) -> u64 {
    if capacity == 0 {
        0
    } else {
        ((queued as u128 * 100) / capacity as u128) as u64
    }
}

fn escape_label(label: &str) -> String {
    label.replace('\\', "\\\\").replace('"', "\\\"")
}
