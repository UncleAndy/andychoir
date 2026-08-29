# Contributing to AndyChoir

> **Language:** English · [Русский](CONTRIBUTING.ru.md)

Thank you for your interest in the project! Below is how to build, test, and
make changes.

## Requirements

- **Rust** (stable) with the `wasm32-wasip2` target:
  ```sh
  rustup target add wasm32-wasip2
  ```
- **Nix** (optional): `nix develop` provides the full environment (see
  `flake.nix`). Running the host with MCP servers (`mcp-nixos` and similar) is
  done through `nix develop`.

## Building

```sh
make host      # the host (src/)
make plugins   # all wasm plugins (plugins/*/)
make all       # host + plugins
```

> Note: wasm files build into `plugins/*/target/`, not the root `target/`.
> Full clean — `make clean` (root target only) + manual clean of
> `plugins/*/target` when needed.

## Testing

```sh
cargo test -p andychoir --lib   # host tests
cd plugins/<name> && cargo test # plugin unit tests (if any)
```

Before merging a PR make sure the build passes **without warnings** and all
tests are green.

## Project structure

- `src/` — the host: message bus, wasm engine, transports (http/ws/net/mcp), console.
- `wit/plugin.wit` — WIT interfaces between the host and plugins.
- `plugins/` — wasm plugins:
  - `front_*` — frontends (console, http, ws);
  - `agent_plugin` — the LLM agent;
  - `tool_*` — tools;
  - `mcp_client_plugin` — MCP server client.

## How to make changes

1. Fork the repository and create a branch.
2. Make changes **additively**: don't break existing paths unnecessarily.
3. Add tests for new logic.
4. Run the build and tests.
5. Open a Pull Request.

## Style

- `cargo fmt` and `cargo clippy` before submitting.
- No compiler warnings.
- Comments may be in Russian or English (consistent within a file).

## License

This project is dual-licensed under **MIT OR Apache-2.0** (see `LICENSE-MIT` and
`LICENSE-APACHE`). By contributing, you agree to license your changes under the
same terms.
