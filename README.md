# VoidB Redis Plugin (`voidb-plugin-redis`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage Redis key-value stores.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: Key pattern searching, value get/set, TTL, server info, and raw command execution.

## Usage & Operational Modes

```bash
# Connectivity test
voidb-cli redis test --profile my-redis

# Key inspection and data manipulation
voidb-cli redis keys "user:*" --profile my-redis
voidb-cli redis get "user:1" --profile my-redis
voidb-cli redis set "user:1" '{"name":"Alice"}' --profile my-redis
voidb-cli redis info --profile my-redis

# Machine / Agent stdio-jsonrpc server
voidb-cli redis serve
```

## Optional local SDK development

Release and CI builds resolve SDK/core from the immutable Git revision in Cargo.toml. To test against a sibling `../voidb` checkout, explicitly opt in with `cargo --config .cargo/config.local.example.toml test --all-targets`. Do not commit an active `.cargo/config.toml` path override.

## Authenticated releases

Starting with v0.3.4, managed installation in VoidB v0.3.5 requires a signed
`provenance.json` covering identity, target, executable, manifest and schemas.
Build jobs produce unsigned staging archives. The separate release signing job
signs all four native targets in the protected `plugin-release-signing` environment
and validates the complete payload before publishing a new immutable release.
The owner must configure environment protection and its signing secret before
publication. Existing v0.3.3 release assets are never replaced.

`VOIDB_PLUGIN_SIGNING_KEY_PEM` belongs only to the protected signing step. Do not
put private keys in this repository, packages, command arguments or logs. Core
contains the trusted public key; registry fields and editable installation records
cannot add runtime trust. Local unsigned development requires both an explicit
`VOIDB_PLUGIN_PATH` directory and `VOIDB_ALLOW_UNSIGNED_PLUGINS=1`.
