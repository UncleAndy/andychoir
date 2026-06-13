use abi_stable::StableAbi;
use abi_stable::std_types::{RResult, RString};
use crate::host::interface::HostContextVTable;

#[derive(StableAbi)]
#[repr(C)]
pub struct PluginInitContext {
    pub host_vtable: HostContextVTable,
    pub plugin_config_json: RString, // Кастомный кусок JSON из общего конфига для этого плагина (PluginConfig.config)
}

#[derive(StableAbi, Clone)]
#[repr(C)]
pub struct PluginInterface {
    // Изменено: Хост передает структуру контекста с индивидуальным конфигом
    pub init: extern "C" fn(ctx: PluginInitContext) -> RResult<(), RString>,
}
