use abi_stable::StableAbi;
use abi_stable::std_types::RString;
use crate::plugin::interface::PluginInterface;

#[derive(StableAbi)]
#[repr(C)]
pub struct PluginDeclaration {
    pub api_version: u32,
    pub name: RString,
    pub interface: PluginInterface,
}
