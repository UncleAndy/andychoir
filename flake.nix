{
  description = "AndyChoir - universal constructor for AI agents and orchestrators (wasm plugins, Wasmtime host)";

  # Пиннинг: зафиксировать rev nixpkgs через flake.lock (воспроизводимость).
  # Раскомментируйте inputs.nixpkgs.url, если нужен конкретный rev:
  #   url = "github:NixOS/nixpkgs/<rev>";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        fixRights = pkgs.writeShellScriptBin "fixr" ''
          mkdir -p target
          sudo chown -R andy:hermes ./
          sudo chmod -R g+wX ./
        '';

        # Rust toolchain через rustup (stable + wasm32-wasip2 target).
        rustDev = pkgs.stdenv.mkDerivation {
          name = "andychoir-rust-dev";

          nativeBuildInputs = [
            pkgs.rustup
            pkgs.gcc          # C-линковщик (cc) для нативных крейтов хоста
            pkgs.binutils     # as/ld для линковки (collect2 spawn)
            pkgs.gnumake      # make
            pkgs.pkg-config
            pkgs.openssl      # для нативных зависимостей (wasmtime, rustyline, tokio)
            pkgs.wasm-tools   # сборка/валидация wasm-компонентов
            pkgs.mcp-nixos
            pkgs.mcp-server-time
            pkgs.mcp-server-filesystem
            pkgs.github-mcp-server
            pkgs.mcp-server-sequential-thinking
            pkgs.mcp-server-fetch
            pkgs.mcp-server-git
            fixRights
          ];

          # Кастомизируем оболочку: ставим toolchain stable + target wasm32-wasip2
          # при первом входе в devshell (lazy, без пересборки флейка).
          #
          # ВАЖНО (NixOS без /usr/bin/env): rustup-тулчейн кладёт обёртку
          # gcc-ld/ld.lld с shebang "#!/usr/bin/env bash", которой здесь нет.
          # На этом хосте обёртку надо заменить настоящим ELF lld:
          #   cp $RUSTUP_HOME/toolchains/stable-*/lib/rustlib/*/bin/gcc-ld-unwrapped/ld.lld \
          #      $RUSTUP_HOME/toolchains/stable-*/lib/rustlib/*/bin/gcc-ld/ld.lld
          # (бэкап оригинала сохранить как ld.lld.bak-wrapper).
          shellHook = ''
            export RUSTUP_HOME="$HOME/.rustup"
            export CARGO_HOME="$HOME/.cargo"
            export TMPDIR="${builtins.getEnv "TMPDIR"}"
            rustup default stable >/dev/null 2>&1 || true
            rustup target add wasm32-wasip2 --toolchain stable >/dev/null 2>&1 || true
            export PATH="$CARGO_HOME/bin:$PATH"
          '';
        };
      in
      {
        devShells.default = rustDev;

        # Для запуска .wasm-компонентов и сборки плагинов.
        packages = {
          wasm-tools = pkgs.wasm-tools;
          wasmtime = pkgs.wasmtime;
        };
      });
}
