pub mod config;
pub mod loader;
pub mod interface;

use abi_stable::std_types::RString;

pub struct PluginDeclaration {
    pub api_version: u32,
    pub name: RString,
}