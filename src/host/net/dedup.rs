//! P2: Дедупликация входящих сетевых событий по event_id через Bloom-фильтр.
//! Фиксированный размер (DEDUP_BITS), периодический сброс по окну времени.

use super::NetInner;
use crate::info;
use std::sync::Arc;

/// Размер битовой матрицы Bloom-фильтра (P2). 4096 бит = 512 байт, фиксированно.
/// Не растёт со временем; периодически сбрасывается (dedup_window).
pub(crate) const DEDUP_BITS: usize = 4096;
/// Окно сброса Bloom-фильтра (секунды).
pub(crate) const DEDUP_WINDOW_SECS: u64 = 60;

/// Текущее время в unix-секундах (для окна сброса dedup).
pub(crate) fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// P2: проверить event_id на дубликат и зарегистрировать его.
/// Возвращает true, если событие НОВОЕ (пропускаем), false — если дубликат (отбрасываем).
/// Каждые DEDUP_WINDOW_SECS фильтр сбрасывается (старые ID "забываются").
pub(crate) async fn check_dedup(inner: &Arc<NetInner>, event_id: &str) -> bool {
    // Сброс по окну времени.
    {
        let mut last = inner.dedup_last_reset.write().await;
        if current_unix_secs().saturating_sub(*last) >= DEDUP_WINDOW_SECS {
            inner.dedup.write().await.clear();
            *last = current_unix_secs();
            info!("[Хост] Net: Bloom-фильтр dedup сброшен (окно {})", DEDUP_WINDOW_SECS);
        }
    }
    // contains → true, если УЖЕ присутствует (дубликат). insert добавляет.
    let is_dup = inner.dedup.write().await.contains(event_id);
    if !is_dup {
        inner.dedup.write().await.insert(event_id);
    }
    !is_dup
}

/// P2: Полностью сбросить Bloom-фильтр (полезно при очистке/тестах).
#[allow(dead_code)]
pub(crate) async fn reset_dedup(inner: &Arc<NetInner>) {
    inner.dedup.write().await.clear();
    *inner.dedup_last_reset.write().await = current_unix_secs();
}
