use libloading::{Library, Symbol};
use std::sync::Arc;
use abi_stable::RRef;

use crate::plugin::declaration::PluginDeclaration;
use crate::plugin::interface::PluginInterface;

pub struct LoadedPlugin {
    _lib: Arc<Library>, // Храним Arc, чтобы библиотека не выгрузилась из памяти раньше времени
    pub name: String,
    pub interface: PluginInterface,
}

impl LoadedPlugin {
    pub unsafe fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let lib = Arc::new(unsafe { Library::new(path)? });

        // Каждая библиотека должна экспортировать эту функцию с #[no_mangle]
        let get_decl: Symbol<unsafe extern "C" fn() -> RRef<'static, PluginDeclaration>> =
            unsafe { lib.get(b"get_plugin_declaration\0")? };

        let decl = unsafe{ get_decl().clone() };

        // Базовая валидация версии API оркестратора
        if decl.get().api_version != 1 {
            return Err("Неподдерживаемая версия API плагина".into());
        }

        Ok(LoadedPlugin {
            _lib: lib,
            name: decl.get().name.to_string(),
            interface: decl.get().interface.clone(), // Копируем VTable (это просто указатели на функции)
        })
    }
}
