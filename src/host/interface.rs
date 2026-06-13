use abi_stable::StableAbi;
use abi_stable::std_types::{RResult, RString};
use crate::messages::message::{MessageCallback, MessageFFI};

#[derive(StableAbi)]
#[repr(C)]
#[sabi(unsafe_opaque_fields)]
pub struct HostContextVTable {
    pub send_message: extern "C" fn(msg: MessageFFI) -> RResult<(), RString>,
    pub subscribe: extern "C" fn(plugin_name: RString, callback: MessageCallback) -> RResult<(), RString>,

    // НАДСТРОЙКА: Плагин вызывает это при старте, чтобы зарегистрировать свой инструмент
    pub register_tool: extern "C" fn(tool_name: RString, plugin_name: RString) -> RResult<(), RString>,
}
