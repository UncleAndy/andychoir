wasmtime::component::bindgen!("host-plugin");

#[macro_export]
macro_rules! println {
    ($($arg:tt)*) => {
        $crate::host::console::print_line(format_args!($($arg)*))
    };
}

pub mod config;
pub mod host;
pub mod messages;
pub mod metrics;
pub mod plugin;
