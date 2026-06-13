use abi_stable::{std_types::RString, StableAbi};

#[derive(StableAbi)]
#[repr(C)]
#[derive(Clone)]
pub struct MessageFFI {
    pub id: u64,
    pub correlation_id: RString, // Сквозной ID пользовательской сессии
    pub source: RString,         // Отправитель (напр., "agent_manager")
    pub destination: RString,    // Получатель (напр., "tool_calculator" или "agent_coder")
    pub topic: RString,          // "call" или "response"
    pub payload: RString,        // JSON с аргументами или текстом
    pub is_final: bool,          // Флаг для фронтенда, что цепочка завершена
}

pub type MessageCallback = extern "C" fn(message: MessageFFI);
