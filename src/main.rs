use wasmtime::{Config, Engine};
use wasmtime::component::{Linker, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

// Стейт хоста, который привязывается к каждому плагину
struct ChoirHostState {
    ctx: WasiCtx,
    table: ResourceTable,
}

// Реализация обязательного трейта для работы WASI Preview 2
impl WasiView for ChoirHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Конфигурируем движок
    let config = Config::new();
    let engine = Engine::new(&config)?;

    // 2. Создаем линкер для компонентной модели
    let mut linker = Linker::<ChoirHostState>::new(&engine);

    // Подключаем стандартные системные функции WASI 0.2 к линкеру
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    // Теперь хост полностью готов загружать ваши .wasm файлы, собранные через Makefile!
    println!("Модули рантайма Wasmtime успешно инициализированы.");
    Ok(())
}
