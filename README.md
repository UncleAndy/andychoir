# AndyChoir
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

> **Language:** English · [Русский](README.ru.md)

Universal constructor for AI agents and orchestrators.

## Architecture

The central binary is a host application. It loads a configuration plugin
(from command-line arguments or environment) and, based on it, loads and
configures all the required plugins. In addition, the host is responsible for
the message bus between plugins.

Hosts can form a decentralized **mesh network** (link-state discovery, shortest-path
routing, bloom-filter dedup) over plain `ws://` or mutually-authenticated, encrypted
`mTLS` (`wss://`) with an internal private CA. See `docs/NET-concept.md` and
`docs/mtls-plan.md`.

## Plugin format

Plugins are loaded as wasm applications. They must export an `init` function
with the signature `init(config: string): void`. The `init` function must accept
configuration as a JSON string and initialize the plugin.

## Base plugins

- **configuration** loader plugin (from a JSON file);
- **LLM** plugin;
- **agent** plugin;
- **orchestrator** plugin;
- base **tool** plugins:
  - filesystem read-only tools;
  - filesystem write tools;
  - internet search tools;
  - calculator tool;
  - environment info tool (current time, environment variables, system info);
  - knowledge base search tool (multiple KBs identified by name);
  - plugin for adding new data to a knowledge base (by KB name);
  - system command execution plugin (with command and argument filters, optional user permission request);

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md) for build, test and contribution details.

## License

This project is dual-licensed under **MIT OR Apache-2.0** (your choice):

- [MIT License](LICENSE-MIT)
- [Apache License 2.0](LICENSE-APACHE)

You may use the project under whichever license suits you best.
