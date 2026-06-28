wasmtime::component::bindgen!("host-plugin");

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        $crate::host::log::debug(format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        $crate::host::log::error(format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::host::log::warn(format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        $crate::host::log::info(format_args!($($arg)*))
    };
}

pub mod config;
pub mod host;
pub mod messages;
pub mod metrics;
pub mod plugin;
