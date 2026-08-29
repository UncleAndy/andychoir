.PHONY: all plugins host clean

host:
	cargo build -p andychoir

plugins:
	cd plugins/front_console_plugin && cargo build --target wasm32-wasip2
	cd plugins/front_http_plugin && cargo build --target wasm32-wasip2
	cd plugins/front_ws_plugin && cargo build --target wasm32-wasip2
	cd plugins/tool_calculator_plugin && cargo build --target wasm32-wasip2
	cd plugins/agent_plugin && cargo build --target wasm32-wasip2
	cd plugins/mcp_client_plugin && cargo build --target wasm32-wasip2

all: host plugins

clean:
	cargo clean
	cd plugins/front_console_plugin && cargo clean
	cd plugins/front_http_plugin && cargo clean
	cd plugins/front_ws_plugin && cargo clean
	cd plugins/tool_calculator_plugin && cargo clean
	cd plugins/agent_plugin && cargo clean
	cd plugins/mcp_client_plugin && cargo clean
