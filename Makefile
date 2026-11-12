.PHONY: all plugins host clean test smoke ci

host:
	cargo build -p andychoir

plugins:
	cd plugins/front_console_plugin && cargo build --target wasm32-wasip2
	cd plugins/front_http_plugin && cargo build --target wasm32-wasip2
	cd plugins/front_ws_plugin && cargo build --target wasm32-wasip2
	cd plugins/tool_calculator_plugin && cargo build --target wasm32-wasip2
	cd plugins/agent_plugin && cargo build --target wasm32-wasip2
	cd plugins/mcp_client_plugin && cargo build --target wasm32-wasip2
	cd plugins/tool_env_plugin && cargo build --target wasm32-wasip2
	cd plugins/filesystem_plugin && cargo build --target wasm32-wasip2
	cd plugins/random_plugin && cargo build --target wasm32-wasip2

all: host plugins

# Юнит-тесты хоста (сеть, шина, права, mTLS).
test:
	cargo test -p andychoir --lib

# Сквозной смоук: HTTP -> front:http -> agent -> tool:calculator -> mock LLM.
# Мок-LLM поднимается внутри tests/smoke.rs; нужны собранные плагины.
# На NixOS рантайм не видит нативные библиотеки (libssl) из devshell без
# LD_LIBRARY_PATH — берём пути из NIX_LDFLAGS. В CI переменная пуста, там
# libssl уже в системном пути загрузчика.
smoke: all
	@LIBS=$$(echo "$$NIX_LDFLAGS" | tr ' ' '\n' | sed -n 's/^-L//p' | paste -sd: -); \
	LD_LIBRARY_PATH="$$LIBS$${LD_LIBRARY_PATH:+:$$LD_LIBRARY_PATH}" \
	cargo test -p andychoir --test smoke -- --nocapture

# То, что должно быть зелёным перед PR (и то же гоняет CI).
ci: test smoke

clean:
	cargo clean
	cd plugins/front_console_plugin && cargo clean
	cd plugins/front_http_plugin && cargo clean
	cd plugins/front_ws_plugin && cargo clean
	cd plugins/tool_calculator_plugin && cargo clean
	cd plugins/agent_plugin && cargo clean
	cd plugins/mcp_client_plugin && cargo clean
	cd plugins/tool_env_plugin && cargo clean
	cd plugins/filesystem_plugin && cargo clean
	cd plugins/random_plugin && cargo clean
