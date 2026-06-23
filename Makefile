.PHONY: all plugins host clean

host:
	cargo build -p andychoir

plugins:
	cargo build -p "*_plugin" --target wasm32-wasip2

all: host plugins

clean:
	cargo clean
