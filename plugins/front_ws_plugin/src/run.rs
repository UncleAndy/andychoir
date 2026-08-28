pub async fn run() {
    // Ввод приходит по WS (через handle_event), фоновый цикл не нужен.
    log_debug!("[WASM] front:ws: фоновый цикл не используется (ввод через WS).");
}
