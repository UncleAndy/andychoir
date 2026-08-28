pub async fn run() {
    // Ввод приходит по HTTP (через handle_event), фоновый цикл не нужен.
    log_debug!("[WASM] front:http: фоновый цикл не используется (ввод через HTTP).");
}
