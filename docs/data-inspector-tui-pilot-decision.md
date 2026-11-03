# Redis Data-Inspector TUI Pilot Decision (Requirement 83)

- **Plugin / profile type**: `voidb-plugin-redis` / Redis TCP and TLS profiles
  (`redis://`, optional password, optional username, `db` index).
- **Selected state**: `Accepted pilot`.
- **Command**: `voidb-plugin-redis tui` (plugin-owned, CLI-launched), also
  routed as `redis tui` through `voidb-cli`.
- **Scope**: key-space incident triage. This is new plugin-owned UI work built
  on the existing `RedisService` channel contract; it adds no backend
  operations and does not depend on removed router-hosted factories, shared
  legacy data widgets, or shell-owned plugin UI state.

This record satisfies the Requirement 83 data inspector TUI acceptance gate in
`voidb/docs/data-inspector-cli-first-decisions.md`, which previously recorded
Redis as `Later candidate`, shipping as `CLI-only`, pending "a focused
key-triage pilot proposal before implementation". That pilot is implemented,
validated, and accepted here with fixture, target-error, and real PTY evidence
produced in the same requirement.

## Named human workflow

**Incident triage on a live key space.** An operator responds to a stale-cache
or runaway-session incident:

1. **Start**: the operator connects through an existing profile
   (`redis tui --profile prod-cache --purpose triage`). The TUI opens read-only
   on the keys view; the first viewport is a bounded SCAN page, not a full dump.
2. **Scan**: the operator presses `p`, enters `session:*`, and the TUI rescans
   from cursor 0 with the server-side pattern, narrowing the page to the
   suspicious subset.
3. **Inspect**: the operator moves the cursor and presses `i`; the key opens in
   a full-screen typed preview (string value, hash field table, list items,
   set members, zset member+score, stream entries) with its type, TTL, size,
   and memory line; large collections continue with `n` (FetchPreviewMore).
4. **Act**: for the suspicious key the operator presses `t` to set a short TTL
   or `d` to delete it (delete requires typing the exact key name, then `y`);
   every mutation rescans the current page.
5. **Success state**: without leaving the TUI, the next scan shows the new TTL
   on the re-TTLed key in the TTL column, or the deleted key is gone from the
   page. A failed mutation (locked key, replica) shows a redacted target error
   in a non-modal banner and the operator keeps working.

**Why the CLI is materially worse here**: the same loop requires one
`redis keys` invocation per pattern page plus separate `redis type`,
`redis get`, and `redis ttl` invocations per candidate key, each paying a fresh
process startup and none sharing cursor context. The TUI keeps the scan page,
cursor position, per-page filter, and inspection viewer in one viewport with
persistent selection state, so an operator walks a page of `session:*` keys in
one sitting instead of juggling shell history. Generic "browse data" is not the
claim; the claim is cursor-preserving triage over repeated
scan-inspect-mutate cycles on the same page set.

## Gate-by-gate evidence

| Gate | Evidence in this implementation |
|---|---|
| Named human workflow | Above; exercised end to end in fixture mode and by the PTY transcript (`typed delete` on `session:abc123`, rescan shows 7 keys and the key gone). |
| Capability-backed bounded reads | Reads are the existing service-layer operations only: `ScanCommand::ScanKeys { cursor, pattern }` (SCAN with COUNT bound in `redis_ops::scan_keys`), `PreviewCommand::FetchPreview`/`FetchPreviewMore` (per-type batches capped in `redis_ops`), `PreviewCommand::SearchKeyData`, `ServerCommand::FetchServerInfo`/`FetchSlowLog`. The TUI adds no new backend operations. |
| Pagination and large-result safety | One SCAN page is rendered at a time (`keys page N cursor=… next=…` in the list title); `n` sends the returned cursor for the next page (pages are **replaced**, not appended, so the viewport stays bounded; the previous cursor is kept in a history stack for `b`); empty state ("No keys on this page…"), partial page (8- and 3-key fixture pages), and end-of-keyspace state are all visible; preview bodies are bounded by `BoundedLines` at 2,000 lines / 2 MiB with dropped-line counters in the status panel, and `has_more` previews advertise `n load more` (truncated state visible). |
| Redaction and error shape | Preflight (`--format json`) reports auth as `"auth": "password"|"none"` with `"secret_material": "redacted"` and no connection URL; `secret_leak_markers` scan covers `redis_password_value`, `redis_secret_data_value`, `plaintext_credential_value`, `raw_connection_url` and passes (`passed: true`, 0 markers) in `build_redis_tui_evidence` output; errors render through `safe_error_summary` in the non-modal banner and keep the family `target error:` category text from the service layer (validation/auth/permission/transport/timeout/plugin/target categories come from the shared capability error contract). |
| Mutation safety | First viewport is read-only. `--readonly` launch stages plans but blocks at confirm with `readonly launch blocked <action> on <key>`. `DeleteKey` requires **typed confirmation of the exact key name** plus explicit `y` (two confirmations, `destructive` risk). `SetTtl`/`RenameKey` use input dialogs plus `y` (`side_effecting` risk). Every mutation result shows a redacted summary and rescans the current page (deleted key disappears; new TTL visible). Mutation policy is recorded in `evidence["mutation_rescan_policy"]`. |
| Fixture and target-error evidence | `fixtures/redis_tui_operations.json` covers string/hash/list/set/zset/stream keys with TTLs, a `session:*` subset, server INFO sections, a slowlog, an empty-pattern page, a truncated preview continuation, and a deliberately erroring action (`session:locked:ephemeral` → `target error: permission denied …`). Startup carries a `target error: READONLY …` banner; the PTY proves the TUI stays fully responsive across it (all 30 assertions pass with the banner present on the first frame). |
| PTY lifecycle evidence | Real PTY run (`pty.fork()` + `TIOCSWINSZ` `HHHH` at 36x130, winsize set pre-exec): 30/30 assertions across 3 consecutive runs. First frame markers (`Redis Key-Space TUI`, `1:keys`, `2:server`, `fixture-redis-ops`, fixture rows, hint footer, target-error banner); quit exits 0; resize to 24x100 keeps content visible and reports `resized redis view to 100x24`; raw-mode cleanup is delegated to `ratatui::restore()` after the loop (raw input is not used: `raw_input = false`). |
| No legacy dependency | The command lives in the plugin (`src/tui.rs`), is launched from the plugin CLI (`redis tui`), consumes only `RedisService` channel events, and contains no reference to removed router-hosted data-inspector adapters or shared legacy widgets. The agent-session/agent_live machinery is untouched. |

## Interaction surface

- Views and hotkeys: `1` keys browser, `2` server info (Tab cycles); pending
  scan/server fetches show the family `⟳` marker in the header tabs.
- Keys browser: name/type/TTL columns; `i`/`Enter` opens the typed full-screen
  preview viewer (string value; hash field table; list items; set members;
  zset member+score; stream entries) with `j/k`/PgUp/PgDn scroll, `n`
  load-more continuation, and `/` in-viewer search (`SearchKeyData` live,
  fixture filter with match count).
- `/` filters the current page client-side with a live match count; `p` edits
  the server-side SCAN pattern and rescans from cursor 0; `n` fetches the next
  scan page (replace semantics, cursor history for `b`); `r` refreshes.
- Manage mutations: `d` typed delete (type exact key name + `y`), `t` TTL
  seconds dialog + `y` (`-1` persists), `R` rename dialog + `y`.
- Server view: INFO section list with a scrollable section viewer, `s` for the
  slowlog viewer.
- Confirm-quit fires while a scan/preview/server fetch or a staged plan is
  active; the error banner is non-modal (any key acknowledges while the key
  keeps its meaning).

## Deferred inside the accepted pilot

Edit-dialog workflows (`SetString`, `HashSet`/`HashDelete`, `ListPush`/
`ListRemove`, `SetAdd`/`SetRemove`, `ZSetAdd`/`ZSetRemove`) are **deferred as
`later candidate`** within this pilot. The pilot accepts manage mutations only
(delete / TTL / rename). The `Edit` command surface already exists in the
service layer, but value-editing UX needs its own fixture, confirmation, and
redaction evidence before it can ship. Until then, docs and release copy must
keep pointing to the CLI (`redis get/set/del/ttl/exec`, and
`voidb-cli invoke run redis.<capability> …`) as the supported general path for
writes and arbitrary queries; `plugin.toml` declares `[ui] tui = true,
raw_input = false` for this bounded pilot only.

## Validation commands and evidence

All run in `voidb-plugin-redis` with `VOIDB_DISABLE_KEYRING=1` (no OS keyring
touch):

```bash
VOIDB_DISABLE_KEYRING=1 cargo test            # 45 passed (25 pre-existing + 20 TUI)
cargo clippy --all-targets -- -D warnings     # 0 warnings
cargo fmt --all && cargo fmt --all -- --check # clean (CI enforces this)
cargo run --example export_contract -- --check # manifest/contract drift green
```

TUI interaction tests (in `src/tui.rs#[cfg(test)]`): fixture evidence +
secret-leak scan, preflight redaction, non-modal startup banner, tab hotkeys,
scan paging with cursor state/history, pattern prompt rescan from cursor 0,
filter match count, typed-delete exact-name gate, delete-execute + rescan,
readonly block, erroring-action responsiveness, TTL dialog + column update,
rename dialog, per-type preview viewer + load-more, viewer search match count,
server info/slowlog viewers, empty page state, confirm-quit, glob semantics,
page-key movement.

Preflight and evidence:

```bash
target/debug/voidb-plugin-redis tui --fixture fixtures/redis_tui_operations.json --format json
target/debug/voidb-plugin-redis tui --fixture fixtures/redis_tui_operations.json --evidence evidence.json
```

Evidence JSON: `kind: redis_tui_fixture_evidence`, quality gate
`retained_tui_quality_gate("redis", ["fixture-redis-ops", "fixture-redis-local"], ["target error"], 80, 24, 10_000)`,
29 coverage entries including the new interactions
(`scan_paging_cursor_history`, `typed_delete_confirmation`, `preview_load_more`,
`viewer_search`, `mutation_rescan`, …), `secret_leak_scan.passed = true`.

PTY lifecycle record (macOS, Python 3.9 stdlib only, `struct "HHHH"`, 36 rows x
130 cols): 30/30 assertions, 3 consecutive clean runs, exit code 0 each run;
script retained out-of-repo at `/tmp/redis_tui_pty_smoke.py` (run
`python3 -I /tmp/redis_tui_pty_smoke.py`). Covered: first frame
(non-blank, marker-checked), banner acknowledgement, full-screen preview with
load-more, scan paging forward/back with cursor state, live resize with content
retention, typed-delete confirmation and rescan, normal `q` quit with clean
exit 0 and terminal restore. Local Python is 3.9 (no tomllib), so the python
provenance tests cannot run on this machine (CI runs 3.11+); this does not
affect the PTY evidence, which uses only stdlib.

## Migration note

This pilot is new plugin-owned UI work. It does not restore, wrap, or depend on
the router-hosted Redis data-inspector adapters removed in Requirement 77, and
it does not reintroduce shared legacy data widgets or shell-owned plugin UI
state. The default TUI guidance for Redis profiles remains the CLI/capability
path; `redis tui` is a bounded, profile-scoped triage pilot whose supported
mutations are delete/TTL/rename only. Release docs must not advertise the
deferred edit-dialog workflows as available in the TUI.
