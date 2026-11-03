//! Standalone Redis key-space data-inspector TUI (Requirement 83 pilot).
//!
//! The pilot follows the retained family TUI pattern (see the Kubernetes
//! plugin): overlay-first routing, a non-modal error banner, per-view
//! selection memory, a persistent hint footer, full-screen scrollable
//! viewers, confirm-quit, loading markers, Length(4) header/status panels,
//! fixture launch with `build_redis_tui_evidence`, a secret-leak scan, and
//! secret-free preflight JSON. All reads and mutations go through the
//! existing `RedisService` channel contract; the TUI adds no backend
//! operations.
//!
//! Scope is deliberately bounded to key triage: scan, preview per key type,
//! server info/slowlog inspection, and the manage mutations delete/TTL/
//! rename. Edit-dialog workflows (SET/HSET/LPUSH/SADD/ZADD) are deferred as
//! `later candidate`; the CLI remains the supported general path.

use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use voidb_core::{TabInfo, TabManager, TuiLaunchPlan, retained_tui_quality_gate};

use crate::config::RedisConfig;
use crate::redis_ops::{InfoSection, KeyInfo, PreviewCursor, RedisKeyType, SlowLogEntry};
use crate::service::{
    EditEvent, ManageCommand, ManageEvent, PreviewCommand, PreviewEvent, RedisCommand, RedisEvent,
    RedisService, ScanCommand, ScanEvent, ServerCommand, ServerEvent,
};

const KEYS_HINTS: &str = "1 keys 2 server j/k / filter p pattern n page b back i preview d del t ttl R rename r refresh ? q quit";
const SERVER_HINTS: &str = "1 keys 2 server j/k i info s slowlog r refresh ? q quit";
const PREVIEW_LINE_LIMIT: usize = 2_000;
const PREVIEW_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const INPUT_LIMIT: usize = 128;
const PAGE_STEP: isize = 10;
const SLOWLOG_COUNT: usize = 25;

/// Where the TUI launch came from. Mirrors the family `K8sTuiSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisTuiSource {
    Profile,
    Connection,
    Fixture,
}

/// Launch description for the standalone Redis key-space TUI. Mirrors the
/// family `K8sTuiLaunch` shape.
#[derive(Debug, Clone)]
pub struct RedisTuiLaunch {
    pub profile_label: String,
    pub config: Option<RedisConfig>,
    pub source: RedisTuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

// ---------------------------------------------------------------------------
// Fixture model
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RedisTuiFixture {
    profile_label: Option<String>,
    server_label: String,
    connection_kind: String,
    #[serde(default)]
    db: u8,
    status: Option<String>,
    target_error: Option<String>,
    #[serde(default)]
    error_keys: Vec<String>,
    error_message: Option<String>,
    #[serde(default)]
    scan_pages: Vec<FixtureScanPage>,
    #[serde(default)]
    previews: Vec<FixturePreviewRecord>,
    #[serde(default)]
    server_info: Vec<FixtureInfoSection>,
    #[serde(default)]
    slowlog: Vec<FixtureSlowLogEntry>,
}

#[derive(Debug, Deserialize)]
struct FixtureScanPage {
    cursor: u64,
    next_cursor: u64,
    #[serde(default)]
    keys: Vec<FixtureKey>,
}

#[derive(Debug, Deserialize)]
struct FixtureKey {
    key: String,
    key_type: String,
    ttl: i64,
}

/// Internal scan page with decoded key types.
#[derive(Debug, Clone)]
struct ScanPage {
    cursor: u64,
    next_cursor: u64,
    keys: Vec<KeyInfo>,
}

/// Fixture preview body. `lines`/`more_lines` use the same per-type rendering
/// the live service produces (value_text), so the viewer code is shared.
#[derive(Debug, Clone)]
struct FixturePreview {
    key: String,
    key_type: RedisKeyType,
    ttl: i64,
    size: Option<u64>,
    memory_bytes: Option<u64>,
    lines: Vec<String>,
    has_more: bool,
    more_lines: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FixturePreviewRecord {
    key: String,
    key_type: String,
    ttl: i64,
    size: Option<u64>,
    memory_bytes: Option<u64>,
    #[serde(default)]
    lines: Vec<String>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    more_lines: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FixtureInfoSection {
    name: String,
    #[serde(default)]
    entries: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
struct FixtureSlowLogEntry {
    id: i64,
    timestamp: i64,
    duration_us: i64,
    command: String,
    #[serde(default)]
    client_addr: String,
    #[serde(default)]
    client_name: String,
}

/// In-memory fixture state. Fixture actions mutate this state directly so the
/// "key gone from the next scan" success state is observable without a server.
struct RedisTuiData {
    profile_label: String,
    server_label: String,
    connection_kind: String,
    db: u8,
    status: String,
    target_error: Option<String>,
    error_keys: Vec<String>,
    error_message: String,
    scan_pages: Vec<ScanPage>,
    previews: Vec<FixturePreview>,
    server_info: Vec<FixtureInfoSection>,
    slowlog: Vec<FixtureSlowLogEntry>,
}

impl RedisTuiData {
    /// Resolve one SCAN page against the active pattern. Unknown cursors
    /// return an empty end-of-scan page, so the TUI always has a visible
    /// empty state to render.
    fn resolve_scan(&self, cursor: u64, pattern: &str) -> (Vec<KeyInfo>, u64) {
        let Some(page) = self.scan_pages.iter().find(|page| page.cursor == cursor) else {
            return (Vec::new(), 0);
        };
        let keys = page
            .keys
            .iter()
            .filter(|info| glob_match(pattern, &info.key))
            .cloned()
            .collect::<Vec<_>>();
        (keys, page.next_cursor)
    }

    fn preview(&self, key: &str) -> Option<&FixturePreview> {
        self.previews.iter().find(|preview| preview.key == key)
    }

    fn preview_mut(&mut self, key: &str) -> Option<&mut FixturePreview> {
        self.previews.iter_mut().find(|preview| preview.key == key)
    }

    /// Apply a manage mutation to the fixture key-space. Keys listed in
    /// `error_keys` deliberately fail so the TUI can prove a target error
    /// keeps it responsive.
    fn apply_mutation(&mut self, plan: &OperationPlanView) -> std::result::Result<String, String> {
        if self.error_keys.iter().any(|key| key == &plan.target_key) {
            return Err(self.error_message.clone());
        }
        match plan.kind {
            OperationKind::DeleteKey => {
                let target = plan.target_key.clone();
                for page in &mut self.scan_pages {
                    page.keys.retain(|info| info.key != target);
                }
                self.previews.retain(|preview| preview.key != target);
                Ok("key removed from the fixture key-space".to_string())
            }
            OperationKind::SetTtl => {
                let ttl = plan.ttl_seconds.unwrap_or(0);
                for page in &mut self.scan_pages {
                    for info in &mut page.keys {
                        if info.key == plan.target_key {
                            info.ttl = ttl;
                        }
                    }
                }
                for preview in &mut self.previews {
                    if preview.key == plan.target_key {
                        preview.ttl = ttl;
                    }
                }
                Ok(format!("ttl set to {}s", ttl))
            }
            OperationKind::RenameKey => {
                let Some(new_name) = plan.new_name.clone() else {
                    return Err("rename plan is missing the new key name".to_string());
                };
                for page in &mut self.scan_pages {
                    for info in &mut page.keys {
                        if info.key == plan.target_key {
                            info.key = new_name.clone();
                        }
                    }
                }
                for preview in &mut self.previews {
                    if preview.key == plan.target_key {
                        preview.key = new_name.clone();
                    }
                }
                Ok(format!("renamed to {new_name}"))
            }
        }
    }

    fn preview_lines(&self, preview: &FixturePreview) -> Vec<String> {
        let mut lines = preview.lines.clone();
        lines.extend(preview.more_lines.iter().cloned());
        lines
    }
}

fn load_fixture(path: &str) -> Result<RedisTuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: RedisTuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;
    Ok(RedisTuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-redis".to_string()),
        server_label: fixture.server_label,
        connection_kind: fixture.connection_kind,
        db: fixture.db,
        status: fixture
            .status
            .unwrap_or_else(|| "fixture redis operations ready".to_string()),
        target_error: fixture.target_error,
        error_keys: fixture.error_keys,
        error_message: fixture
            .error_message
            .unwrap_or_else(|| "target error: fixture action rejected".to_string()),
        scan_pages: fixture
            .scan_pages
            .into_iter()
            .map(|page| ScanPage {
                cursor: page.cursor,
                next_cursor: page.next_cursor,
                keys: page
                    .keys
                    .into_iter()
                    .map(|key| KeyInfo {
                        key: key.key,
                        key_type: RedisKeyType::from_redis_type(&key.key_type),
                        ttl: key.ttl,
                    })
                    .collect(),
            })
            .collect(),
        previews: fixture
            .previews
            .into_iter()
            .map(|record| FixturePreview {
                key: record.key,
                key_type: RedisKeyType::from_redis_type(&record.key_type),
                ttl: record.ttl,
                size: record.size,
                memory_bytes: record.memory_bytes,
                lines: record.lines,
                has_more: record.has_more,
                more_lines: record.more_lines,
            })
            .collect(),
        server_info: fixture.server_info,
        slowlog: fixture.slowlog,
    })
}

/// Minimal SCAN-style glob matcher (`*` wildcards only), used by fixture mode
/// to honor the operator's server-side pattern.
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return text == pattern;
    }
    let mut pos = 0usize;
    if let Some(first) = parts.first().copied().filter(|part| !part.is_empty()) {
        if !text.starts_with(first) {
            return false;
        }
        pos = first.len();
    }
    let last = parts[parts.len() - 1];
    let end_bound = if last.is_empty() {
        text.len()
    } else {
        text.len().saturating_sub(last.len())
    };
    for middle in &parts[1..parts.len() - 1] {
        if middle.is_empty() {
            continue;
        }
        let search_end = end_bound.max(pos);
        let Some(found) = text[pos..search_end].find(middle) else {
            return false;
        };
        pos += found + middle.len();
    }
    if !last.is_empty() && !text.ends_with(last) {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Keys,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
}

/// Full-screen or popup surfaces stacked on top of the browser. Input is
/// routed to the active overlay first, so each surface owns its key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    None,
    Help,
    PreviewViewer,
    ServerViewer,
    PatternPrompt,
    TtlPrompt,
    RenamePrompt,
    ConfirmDelete,
    ConfirmQuit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    DeleteKey,
    SetTtl,
    RenameKey,
}

impl OperationKind {
    fn label(&self) -> &'static str {
        match self {
            OperationKind::DeleteKey => "delete key",
            OperationKind::SetTtl => "set ttl",
            OperationKind::RenameKey => "rename key",
        }
    }

    fn risk(&self) -> &'static str {
        match self {
            OperationKind::DeleteKey => "destructive",
            OperationKind::SetTtl | OperationKind::RenameKey => "side_effecting",
        }
    }
}

/// Staged mutation plan rendered in the details panel before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationPlanView {
    kind: OperationKind,
    target_key: String,
    risk: &'static str,
    confirmations_required: u8,
    confirmations: u8,
    ttl_seconds: Option<i64>,
    new_name: Option<String>,
}

/// Bounded text buffer for viewer bodies (same contract as the family TUIs).
struct BoundedLines {
    lines: VecDeque<String>,
    bytes: usize,
    dropped_lines: usize,
    max_lines: usize,
    max_bytes: usize,
}

pub fn write_redis_tui_preflight(launch: &RedisTuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_redis_tui_evidence(launch: &RedisTuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("redis tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let mut evidence = json!({
        "schema_version": 1,
        "kind": "redis_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "redis",
            &["fixture-redis-ops", "fixture-redis-local"],
            &["target error"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "server_label": data.server_label,
            "connection_kind": data.connection_kind,
            "db": data.db,
            "status": data.status,
            "target_error": data.target_error,
            "scan_pages": data.scan_pages.iter().map(page_value).collect::<Vec<_>>(),
            "previews": data.previews.iter().map(preview_value).collect::<Vec<_>>(),
            "server_info": data.server_info.iter().map(info_section_value).collect::<Vec<_>>(),
            "slowlog": data.slowlog.iter().map(slowlog_value).collect::<Vec<_>>(),
            "preview_bounds": {
                "max_lines": PREVIEW_LINE_LIMIT,
                "max_bytes": PREVIEW_BYTE_LIMIT
            }
        },
        "plan_transcript": [
            {
                "action": "delete_key",
                "risk": "destructive",
                "typed_confirmation": "exact_key_name_required",
                "second_confirmation": "explicit_y",
                "readonly_behavior": "plan_visible_but_blocked",
                "post_action": "current_page_rescan"
            },
            {
                "action": "set_ttl",
                "risk": "side_effecting",
                "input_dialog": "ttl_seconds",
                "confirmation": "explicit_y",
                "post_action": "current_page_rescan"
            },
            {
                "action": "rename_key",
                "risk": "side_effecting",
                "input_dialog": "new_key_name",
                "confirmation": "explicit_y",
                "post_action": "current_page_rescan"
            }
        ],
        "coverage": [
            "startup",
            "keys_server_tabs",
            "scan_paging_cursor_history",
            "scan_pattern_edit",
            "client_filter_match_count",
            "typed_delete_confirmation",
            "ttl_dialog_confirmation",
            "rename_dialog_confirmation",
            "readonly_block",
            "mutation_rescan",
            "preview_viewer_string",
            "preview_viewer_hash",
            "preview_viewer_list",
            "preview_viewer_set",
            "preview_viewer_zset",
            "preview_viewer_stream",
            "preview_load_more",
            "viewer_search",
            "server_info_viewer",
            "slowlog_viewer",
            "non_modal_error_banner",
            "target_error_responsive",
            "empty_page_state",
            "partial_page_state",
            "truncated_preview_state",
            "quit_confirmation",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "deferred": {
            "edit_dialog_workflows": [
                "set_string",
                "hash_set",
                "hash_delete",
                "list_push",
                "list_remove",
                "set_add",
                "set_remove",
                "zset_add",
                "zset_remove"
            ],
            "outcome": "later candidate within the accepted pilot",
            "supported_mutations": ["delete_key", "set_ttl", "rename_key"],
            "cli_remains_supported_general_path": true
        },
        "mutation_rescan_policy": {
            "scan_semantics": "one bounded SCAN page at a time; the next page is fetched by sending the cursor returned with the previous page",
            "after_mutation": "the current page cursor is rescanned so the deleted key disappears and new TTLs are visible",
            "fixture_mutations": "applied to the in-memory fixture key-space so paging evidence stays deterministic"
        },
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_redis_tui(launch: RedisTuiLaunch) -> Result<()> {
    let mut app = RedisTuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &RedisTuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let connection = launch
        .config
        .as_ref()
        .map(connection_value)
        .or_else(|| {
            fixture_data.as_ref().map(|data| {
                json!({
                    "kind": data.connection_kind,
                    "server_label": data.server_label,
                    "secret_material": "redacted"
                })
            })
        })
        .unwrap_or_else(|| {
            json!({
                "kind": "fixture",
                "server_label": "fixture",
                "secret_material": "redacted"
            })
        });
    let db = launch
        .config
        .as_ref()
        .map(|config| config.db)
        .or_else(|| fixture_data.as_ref().map(|data| data.db))
        .unwrap_or(0);
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "redis tui",
        "plugin_id": "redis",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "db": db,
        "connection": connection,
        "privacy": {
            "diagnostics_include_password": false,
            "diagnostics_include_connection_url": false,
            "diagnostics_include_key_values": false
        },
        "stream_bounds": {
            "preview": { "max_lines": PREVIEW_LINE_LIMIT, "max_bytes": PREVIEW_BYTE_LIMIT }
        },
        "service_boundary": "RedisService::Channel",
        "modes": [
            "keys",
            "server",
            "filter",
            "preview_viewer",
            "server_viewer",
            "pattern_prompt",
            "ttl_prompt",
            "rename_prompt",
            "typed_delete_confirmation",
            "confirm_quit",
            "target_error"
        ],
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut RedisTuiApp) -> Result<()> {
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized redis view to {cols}x{rows}");
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

fn config_data(profile_label: String, config: &RedisConfig) -> RedisTuiData {
    RedisTuiData {
        profile_label,
        server_label: format!("{}:{}", config.host, config.port),
        connection_kind: if config.tls { "tls" } else { "tcp" }.to_string(),
        db: config.db,
        status: "connecting through RedisService channel mode".to_string(),
        target_error: None,
        error_keys: Vec::new(),
        error_message: "target error: fixture action rejected".to_string(),
        scan_pages: Vec::new(),
        previews: Vec::new(),
        server_info: Vec::new(),
        slowlog: Vec::new(),
    }
}

fn connection_value(config: &RedisConfig) -> Value {
    json!({
        "kind": if config.tls { "tls" } else { "tcp" },
        "host_label": config.host,
        "port": config.port,
        "db": config.db,
        "auth": if config.password.is_some() { "password" } else { "none" },
        "username_present": config.username.is_some(),
        "secret_material": "redacted"
    })
}

struct RedisTuiApp {
    profile_label: String,
    server_label: String,
    connection_kind: String,
    db: u8,
    source: RedisTuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    view: View,
    /// Server-side SCAN pattern (empty string scans the whole keyspace).
    scan_pattern: String,
    /// Cursor that produced the current page.
    scan_cursor: u64,
    /// Cursor returned with the current page; 0 means end of keyspace.
    next_scan_cursor: u64,
    /// Previous page cursors so `b` can walk back without losing context.
    scan_history: Vec<u64>,
    page_index: usize,
    keys: Vec<KeyInfo>,
    selection: usize,
    server_selection: usize,
    filter: String,
    mode: Mode,
    overlay: Overlay,
    error_banner: Option<String>,
    scan_pending: bool,
    preview_pending: bool,
    server_pending: bool,
    server_sections_loaded: bool,
    preview_key: Option<String>,
    preview_lines: BoundedLines,
    preview_scroll: usize,
    preview_has_more: bool,
    preview_cursor: PreviewCursor,
    viewer_search: String,
    viewer_searching: bool,
    server_viewer_title: String,
    server_viewer_lines: BoundedLines,
    server_viewer_scroll: usize,
    pattern_input: String,
    ttl_input: String,
    rename_input: String,
    typed_key_input: String,
    operation_plan: Option<OperationPlanView>,
    fixture: Option<RedisTuiData>,
    server_sections: Vec<InfoSection>,
    slowlog_entries: Vec<SlowLogEntry>,
    service: Option<RedisService>,
    status: String,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

impl RedisTuiApp {
    fn new(launch: RedisTuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("redis tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("redis tui requires config outside fixture mode")?;
            let tabs = Arc::new(StandaloneRedisTabManager::new(render_quit.clone()));
            let runtime = tokio::runtime::Handle::current();
            let service = RedisService::new(config, tabs, runtime);
            let (reply, _reply_rx) = oneshot::channel();
            service.send(RedisCommand::Connect { reply });
            Some(service)
        } else {
            None
        };

        let is_fixture = launch.fixture_path.is_some();
        let mut app = Self {
            profile_label: data.profile_label.clone(),
            server_label: data.server_label.clone(),
            connection_kind: data.connection_kind.clone(),
            db: data.db,
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            view: View::Keys,
            scan_pattern: String::new(),
            scan_cursor: 0,
            next_scan_cursor: 0,
            scan_history: Vec::new(),
            page_index: 1,
            keys: Vec::new(),
            selection: 0,
            server_selection: 0,
            filter: String::new(),
            mode: Mode::Browser,
            overlay: Overlay::None,
            error_banner: data.target_error.clone(),
            scan_pending: false,
            preview_pending: false,
            server_pending: false,
            server_sections_loaded: false,
            preview_key: None,
            preview_lines: BoundedLines::new(PREVIEW_LINE_LIMIT, PREVIEW_BYTE_LIMIT),
            preview_scroll: 0,
            preview_has_more: false,
            preview_cursor: PreviewCursor::None,
            viewer_search: String::new(),
            viewer_searching: false,
            server_viewer_title: String::new(),
            server_viewer_lines: BoundedLines::new(PREVIEW_LINE_LIMIT, PREVIEW_BYTE_LIMIT),
            server_viewer_scroll: 0,
            pattern_input: String::new(),
            ttl_input: String::new(),
            rename_input: String::new(),
            typed_key_input: String::new(),
            operation_plan: None,
            fixture: if is_fixture { Some(data) } else { None },
            server_sections: Vec::new(),
            slowlog_entries: Vec::new(),
            service,
            status: String::new(),
            should_quit: false,
            render_quit,
        };
        if is_fixture {
            // Fixture mode loads the first SCAN page eagerly so the first
            // frame already shows the key-space.
            app.request_scan(0);
        }
        if let Some(error) = app
            .fixture
            .as_ref()
            .and_then(|fixture| fixture.target_error.clone())
        {
            app.error_banner = Some(error.clone());
            app.status = format!("{} | {}", app.status, safe_error_summary(&error));
        }
        Ok(app)
    }

    fn shutdown(&mut self) {
        if let Some(service) = &self.service {
            service.send(RedisCommand::Disconnect);
        }
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        while let Some(event) = service.poll_event() {
            changed = true;
            self.handle_service_event(event);
        }
        self.service = Some(service);
        changed
    }

    fn handle_service_event(&mut self, event: RedisEvent) {
        match event {
            RedisEvent::Connected => {
                self.status = "connected; scanning keys".to_string();
                self.request_scan(0);
            }
            RedisEvent::Disconnected => {
                self.status = "disconnected".to_string();
            }
            RedisEvent::Scan(ScanEvent::KeysScanned { keys, next_cursor }) => {
                self.scan_pending = false;
                self.keys = keys;
                self.next_scan_cursor = next_cursor;
                self.trim_selection();
                self.status = format!(
                    "scanned {} keys at cursor {} (next cursor {})",
                    self.keys.len(),
                    self.scan_cursor,
                    next_cursor
                );
            }
            RedisEvent::Scan(ScanEvent::DbSizesLoaded(_)) => {}
            RedisEvent::Preview(PreviewEvent::PreviewLoaded(preview)) => {
                self.preview_pending = false;
                self.open_preview(
                    preview.key,
                    preview.key_type,
                    preview.ttl,
                    preview.size,
                    preview.memory_bytes,
                    &preview.value_text,
                    preview.cursor,
                    preview.has_more,
                );
            }
            RedisEvent::Preview(PreviewEvent::PreviewMoreLoaded(more)) => {
                self.preview_pending = false;
                self.preview_lines.push_text(&more.value_text);
                self.preview_cursor = more.cursor;
                self.preview_has_more = more.has_more;
                self.status = format!(
                    "preview continuation loaded; {} lines retained",
                    self.preview_lines.len()
                );
            }
            RedisEvent::Preview(PreviewEvent::SearchDataLoaded(data)) => {
                self.preview_pending = false;
                self.apply_search_results(&data);
            }
            RedisEvent::Preview(_) => {
                self.preview_pending = false;
                self.status = "editor data loaded outside the pilot preview viewer".to_string();
            }
            RedisEvent::Manage(ManageEvent::KeyDeleted(message)) => {
                self.operation_plan = None;
                self.status = format!("deleted: {}", safe_error_summary(&message));
                self.rescan_current_page();
            }
            RedisEvent::Manage(ManageEvent::TtlSet(message)) => {
                self.operation_plan = None;
                self.status = format!("ttl updated: {}", safe_error_summary(&message));
                self.rescan_current_page();
            }
            RedisEvent::Manage(ManageEvent::KeyRenamed { old_key, new_key }) => {
                self.operation_plan = None;
                self.status = format!("renamed: {} -> {}", old_key, new_key);
                self.rescan_current_page();
            }
            RedisEvent::Edit(EditEvent::WriteSuccess(message)) => {
                self.status = safe_error_summary(&message);
            }
            RedisEvent::Server(ServerEvent::ServerInfoLoaded(sections)) => {
                self.server_pending = false;
                self.server_sections_loaded = true;
                self.server_sections = sections;
                self.open_server_info_viewer();
            }
            RedisEvent::Server(ServerEvent::SlowLogLoaded(entries)) => {
                self.server_pending = false;
                self.slowlog_entries = entries;
                self.open_slowlog_viewer();
            }
            RedisEvent::Server(ServerEvent::ClientListLoaded(_)) => {
                self.server_pending = false;
            }
            RedisEvent::CommandResult(_) => {}
            RedisEvent::Error(message) => {
                // Background and request failures surface as a non-modal
                // banner so a failing scan or preview never hijacks the
                // browser.
                self.scan_pending = false;
                self.preview_pending = false;
                self.server_pending = false;
                self.error_banner = Some(message);
                self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
            }
        }
    }

    // === Input routing ===

    fn handle_key(&mut self, key: KeyEvent) {
        // The error banner is informational: any key acknowledges it while
        // the key itself keeps its normal meaning, so background failures
        // never trap the operator in a modal.
        self.error_banner = None;
        match self.overlay {
            Overlay::None => {}
            Overlay::Help => {
                self.overlay = Overlay::None;
                self.status = "help closed".to_string();
                return;
            }
            Overlay::PreviewViewer => {
                self.handle_preview_viewer_key(key);
                return;
            }
            Overlay::ServerViewer => {
                self.handle_server_viewer_key(key);
                return;
            }
            Overlay::PatternPrompt => {
                self.handle_pattern_prompt_key(key);
                return;
            }
            Overlay::TtlPrompt => {
                self.handle_ttl_prompt_key(key);
                return;
            }
            Overlay::RenamePrompt => {
                self.handle_rename_prompt_key(key);
                return;
            }
            Overlay::ConfirmDelete => {
                self.handle_confirm_delete_key(key);
                return;
            }
            Overlay::ConfirmQuit => {
                self.handle_confirm_quit_key(key);
                return;
            }
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Char('1') => self.set_view(View::Keys),
            KeyCode::Char('2') => self.set_view(View::Server),
            KeyCode::Tab => {
                let next = match self.view {
                    View::Keys => View::Server,
                    View::Server => View::Keys,
                };
                self.set_view(next);
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_selected(0),
            KeyCode::End | KeyCode::Char('G') => {
                let len = self.filtered_keys().len();
                self.set_selected(len.saturating_sub(1));
            }
            KeyCode::Enter | KeyCode::Char('i') => self.inspect_selected(),
            KeyCode::Char('/') => {
                self.mode = Mode::Filter;
                self.status = "filter: type to narrow live, Enter apply, Esc clear".to_string();
            }
            KeyCode::Char('p') => self.open_pattern_prompt(),
            KeyCode::Char('n') => self.next_scan_page(),
            KeyCode::Char('b') => self.previous_scan_page(),
            KeyCode::Char('r') => self.refresh_view(),
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('t') => self.open_ttl_prompt(),
            KeyCode::Char('R') => self.open_rename_prompt(),
            KeyCode::Char('s') => self.fetch_slowlog(),
            KeyCode::Char('?') => self.show_help(),
            KeyCode::Char('y') => self.confirm_plan(),
            KeyCode::Esc => self.cancel_plan(),
            _ => {
                self.status = format!(
                    "redis: {}",
                    match self.view {
                        View::Keys => KEYS_HINTS,
                        View::Server => SERVER_HINTS,
                    }
                );
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = format!(
                    "filter applied: {} ({} matches)",
                    self.filter_label(),
                    self.filtered_keys().len()
                );
            }
            KeyCode::Esc => {
                self.filter.clear();
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.filter.pop();
                self.set_selected(0);
            }
            KeyCode::Char(ch) => {
                if self.filter.len() < INPUT_LIMIT {
                    self.filter.push(ch);
                }
                self.set_selected(0);
            }
            _ => {}
        }
    }

    fn handle_preview_viewer_key(&mut self, key: KeyEvent) {
        // While the viewer search input is open every character belongs to
        // the search, mirroring the family filter-mode routing.
        if self.viewer_searching {
            match key.code {
                KeyCode::Esc => {
                    self.viewer_searching = false;
                    self.viewer_search.clear();
                    self.status = "viewer search cancelled".to_string();
                }
                KeyCode::Backspace => {
                    self.viewer_search.pop();
                }
                KeyCode::Enter => self.run_viewer_search(),
                KeyCode::Char(ch) if self.viewer_search.len() < INPUT_LIMIT => {
                    self.viewer_search.push(ch);
                }
                _ => {}
            }
            return;
        }
        let max_scroll = self.preview_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.status = "preview viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.preview_scroll = self.preview_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.preview_scroll = (self.preview_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.preview_scroll = self.preview_scroll.saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.preview_scroll = (self.preview_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.preview_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.preview_scroll = max_scroll,
            KeyCode::Char('n') => self.load_preview_more(),
            KeyCode::Char('/') => {
                self.viewer_searching = true;
                self.viewer_search.clear();
                self.status =
                    "viewer search: type a substring, Enter searches the collection".to_string();
            }
            _ => {}
        }
    }

    fn handle_server_viewer_key(&mut self, key: KeyEvent) {
        let max_scroll = self.server_viewer_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.status = "server viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.server_viewer_scroll = self.server_viewer_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.server_viewer_scroll = (self.server_viewer_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.server_viewer_scroll =
                    self.server_viewer_scroll.saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.server_viewer_scroll =
                    (self.server_viewer_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.server_viewer_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.server_viewer_scroll = max_scroll,
            _ => {}
        }
    }

    fn handle_pattern_prompt_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.pattern_input.clear();
                self.overlay = Overlay::None;
                self.status = "pattern edit cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.pattern_input.pop();
            }
            KeyCode::Enter => {
                let pattern = std::mem::take(&mut self.pattern_input);
                self.overlay = Overlay::None;
                self.scan_pattern = pattern.clone();
                self.scan_history.clear();
                self.page_index = 1;
                self.request_scan(0);
                self.status = format!(
                    "scan pattern set to '{}'; rescanning from cursor 0",
                    if pattern.is_empty() { "*" } else { &pattern }
                );
            }
            KeyCode::Char(ch) if self.pattern_input.len() < INPUT_LIMIT => {
                self.pattern_input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_ttl_prompt_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.ttl_input.clear();
                self.overlay = Overlay::None;
                self.status = "ttl edit cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.ttl_input.pop();
            }
            KeyCode::Enter => {
                let Ok(seconds) = self.ttl_input.parse::<i64>() else {
                    self.status =
                        "ttl: enter an integer number of seconds (-1 persists)".to_string();
                    return;
                };
                let Some(target) = self.selected_key() else {
                    self.overlay = Overlay::None;
                    return;
                };
                self.ttl_input.clear();
                self.overlay = Overlay::None;
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::SetTtl,
                    target_key: target.clone(),
                    risk: OperationKind::SetTtl.risk(),
                    confirmations_required: 1,
                    confirmations: 0,
                    ttl_seconds: Some(seconds),
                    new_name: None,
                });
                let action = if seconds < 0 {
                    "persist key".to_string()
                } else {
                    format!("ttl {seconds}s")
                };
                self.status =
                    format!("ttl plan staged for {target} ({action}); press y to execute");
            }
            KeyCode::Char(ch)
                if (ch.is_ascii_digit() || ch == '-') && self.ttl_input.len() < 12 =>
            {
                self.ttl_input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_rename_prompt_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.rename_input.clear();
                self.overlay = Overlay::None;
                self.status = "rename cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.rename_input.pop();
            }
            KeyCode::Enter => {
                let new_name = self.rename_input.trim().to_string();
                if new_name.is_empty() {
                    self.status = "rename: enter a non-empty new key name".to_string();
                    return;
                }
                let Some(target) = self.selected_key() else {
                    self.overlay = Overlay::None;
                    return;
                };
                if new_name == target {
                    self.status = "rename: new name equals the current name".to_string();
                    return;
                }
                self.rename_input.clear();
                self.overlay = Overlay::None;
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::RenameKey,
                    target_key: target.clone(),
                    risk: OperationKind::RenameKey.risk(),
                    confirmations_required: 1,
                    confirmations: 0,
                    ttl_seconds: None,
                    new_name: Some(new_name.clone()),
                });
                self.status =
                    format!("rename plan staged for {target} -> {new_name}; press y to execute");
            }
            KeyCode::Char(ch) if self.rename_input.len() < INPUT_LIMIT => {
                self.rename_input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_confirm_delete_key(&mut self, key: KeyEvent) {
        let Some(target) = self.selected_key() else {
            self.overlay = Overlay::None;
            return;
        };
        match key.code {
            KeyCode::Esc => {
                self.typed_key_input.clear();
                self.overlay = Overlay::None;
                self.status = "delete cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.typed_key_input.pop();
            }
            KeyCode::Enter => {
                if self.typed_key_input != target {
                    self.status =
                        format!("typed name does not match {}; delete not staged", target);
                    self.typed_key_input.clear();
                    return;
                }
                self.typed_key_input.clear();
                self.overlay = Overlay::None;
                // The typed key name is the first of two confirmations; `y`
                // remains the explicit non-default acknowledgement.
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::DeleteKey,
                    target_key: target.clone(),
                    risk: OperationKind::DeleteKey.risk(),
                    confirmations_required: 2,
                    confirmations: 1,
                    ttl_seconds: None,
                    new_name: None,
                });
                self.status = format!(
                    "delete plan staged for {target} (typed confirmation recorded); press y to execute"
                );
            }
            KeyCode::Char(ch) if self.typed_key_input.len() < INPUT_LIMIT => {
                self.typed_key_input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_confirm_quit_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.should_quit = true;
            }
            _ => {
                self.overlay = Overlay::None;
                self.status = "quit cancelled".to_string();
            }
        }
    }

    // === Selection and views ===

    fn selected(&self) -> usize {
        self.selection
    }

    fn set_selected(&mut self, value: usize) {
        let len = self.filtered_keys().len();
        self.selection = value.min(len.saturating_sub(1));
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.filtered_keys().len();
        if len == 0 {
            self.set_selected(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.selected() as isize + delta).clamp(0, last) as usize;
        self.set_selected(next);
    }

    fn trim_selection(&mut self) {
        self.set_selected(self.selected());
    }

    fn set_view(&mut self, view: View) {
        if self.view == view {
            return;
        }
        self.view = view;
        self.status = format!(
            "view: {}",
            match view {
                View::Keys => "keys",
                View::Server => "server",
            }
        );
        if view == View::Server
            && self.fixture.is_none()
            && !self.server_sections_loaded
            && !self.server_pending
            && let Some(service) = &self.service
        {
            self.server_pending = true;
            service.send(RedisCommand::Server(ServerCommand::FetchServerInfo));
        }
    }

    fn filtered_keys(&self) -> Vec<KeyInfo> {
        let needle = self.filter.to_lowercase();
        self.keys
            .iter()
            .filter(|info| needle.is_empty() || info.key.to_lowercase().contains(&needle))
            .cloned()
            .collect()
    }

    fn selected_key(&self) -> Option<String> {
        if self.view != View::Keys {
            return None;
        }
        self.filtered_keys()
            .get(self.selected())
            .map(|info| info.key.clone())
    }

    fn selected_server_section(&self) -> Option<usize> {
        if self.view != View::Server {
            return None;
        }
        let sections = self.server_section_count();
        if sections == 0 {
            return None;
        }
        Some(self.server_selection.min(sections - 1))
    }

    fn server_section_count(&self) -> usize {
        match &self.fixture {
            Some(fixture) => fixture.server_info.len(),
            None => self.server_sections.len(),
        }
    }

    fn filter_label(&self) -> String {
        if self.filter.is_empty() {
            "none".to_string()
        } else {
            self.filter.clone()
        }
    }

    // === Scanning ===

    /// Send one bounded SCAN page request. Fixture mode resolves the page
    /// from the fixture key-space synchronously; live mode sends
    /// `ScanCommand::ScanKeys` with the requested cursor.
    fn request_scan(&mut self, cursor: u64) {
        self.scan_cursor = cursor;
        self.scan_pending = true;
        if let Some(fixture) = &self.fixture {
            let (keys, next_cursor) = fixture.resolve_scan(cursor, &self.scan_pattern);
            self.keys = keys;
            self.next_scan_cursor = next_cursor;
            self.scan_pending = false;
            self.trim_selection();
            self.status = format!(
                "fixture scanned {} keys at cursor {} (next cursor {})",
                self.keys.len(),
                cursor,
                next_cursor
            );
        } else if let Some(service) = &self.service {
            let pattern = self.scan_pattern.clone();
            service.send(RedisCommand::Scan(ScanCommand::ScanKeys {
                cursor,
                pattern: (!pattern.is_empty()).then_some(pattern),
            }));
        } else {
            self.scan_pending = false;
        }
    }

    /// `n`: replace the current page with the page at `next_scan_cursor`.
    /// Pages are replaced (not appended) so the viewport stays bounded; the
    /// previous cursor is pushed onto a history stack for `b`.
    fn next_scan_page(&mut self) {
        if self.next_scan_cursor == 0 {
            self.status = "end of keyspace reached; press b to go back or p to change the pattern"
                .to_string();
            return;
        }
        self.scan_history.push(self.scan_cursor);
        self.page_index = self.page_index.saturating_add(1);
        self.set_selected(0);
        self.request_scan(self.next_scan_cursor);
    }

    fn previous_scan_page(&mut self) {
        let Some(previous) = self.scan_history.pop() else {
            self.status = "no previous scan page".to_string();
            return;
        };
        self.page_index = self.page_index.saturating_sub(1).max(1);
        self.set_selected(0);
        self.request_scan(previous);
    }

    fn rescan_current_page(&mut self) {
        // After a mutation the current page cursor is rescanned so the
        // deleted key disappears and new TTL values are visible without
        // leaving the TUI.
        self.request_scan(self.scan_cursor);
    }

    fn refresh_view(&mut self) {
        match self.view {
            View::Keys => {
                self.rescan_current_page();
                self.status = format!("refreshing keys at cursor {}", self.scan_cursor);
            }
            View::Server => {
                self.request_server_info();
            }
        }
    }

    fn request_server_info(&mut self) {
        if let Some(fixture) = &self.fixture {
            self.server_sections_loaded = true;
            self.status = format!(
                "fixture server info loaded ({} sections)",
                fixture.server_info.len()
            );
            return;
        }
        if let Some(service) = &self.service {
            self.server_pending = true;
            service.send(RedisCommand::Server(ServerCommand::FetchServerInfo));
            self.status = "loading server info".to_string();
        }
    }

    fn fetch_slowlog(&mut self) {
        if self.view != View::Server {
            self.status = "switch to the server view (2) for slowlog".to_string();
            return;
        }
        if let Some(fixture) = &self.fixture {
            self.slowlog_entries = fixture
                .slowlog
                .iter()
                .map(|entry| crate::redis_ops::SlowLogEntry {
                    id: entry.id,
                    timestamp: entry.timestamp,
                    duration_us: entry.duration_us,
                    command: entry.command.clone(),
                    client_addr: entry.client_addr.clone(),
                    client_name: entry.client_name.clone(),
                })
                .collect();
            self.open_slowlog_viewer();
            return;
        }
        if let Some(service) = &self.service {
            self.server_pending = true;
            service.send(RedisCommand::Server(ServerCommand::FetchSlowLog {
                count: SLOWLOG_COUNT,
            }));
            self.status = "loading slowlog".to_string();
        }
    }

    // === Preview viewer ===

    fn inspect_selected(&mut self) {
        match self.view {
            View::Server => {
                let Some(index) = self.selected_server_section() else {
                    self.status = "no server section loaded yet; press r to refresh".to_string();
                    return;
                };
                self.open_section_viewer(index);
            }
            View::Keys => {
                let Some(key) = self.selected_key() else {
                    self.status = "nothing selected".to_string();
                    return;
                };
                if let Some(fixture) = &self.fixture {
                    let Some(preview) = fixture.preview(&key) else {
                        self.error_banner =
                            Some(format!("target error: no fixture preview for key {key}"));
                        self.status =
                            safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                        return;
                    };
                    self.open_preview(
                        preview.key.clone(),
                        preview.key_type,
                        preview.ttl,
                        preview.size,
                        preview.memory_bytes,
                        &fixture.preview_lines(preview).join("\n"),
                        PreviewCursor::None,
                        preview.has_more,
                    );
                    self.status = format!("fixture preview: {key}");
                } else if let Some(service) = &self.service {
                    self.preview_pending = true;
                    service.send(RedisCommand::Preview(PreviewCommand::FetchPreview {
                        key: key.clone(),
                    }));
                    self.status =
                        format!("loading preview for {key} (full-screen viewer opens when loaded)");
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn open_preview(
        &mut self,
        key: String,
        key_type: RedisKeyType,
        ttl: i64,
        size: Option<u64>,
        memory_bytes: Option<u64>,
        value_text: &str,
        cursor: PreviewCursor,
        has_more: bool,
    ) {
        self.preview_lines.clear();
        self.preview_lines.push_line(format!(
            "key: {key}  type: {}  ttl: {}  size: {}  memory: {}",
            key_type.label(),
            crate::redis_ops::format_ttl(ttl),
            size.map(crate::redis_ops::format_bytes)
                .unwrap_or_else(|| "-".to_string()),
            memory_bytes
                .map(crate::redis_ops::format_bytes)
                .unwrap_or_else(|| "-".to_string()),
        ));
        self.preview_lines.push_line(String::new());
        self.preview_lines.push_text(value_text);
        self.preview_key = Some(key);
        self.preview_cursor = cursor;
        self.preview_has_more = has_more;
        self.preview_scroll = 0;
        self.viewer_search.clear();
        self.viewer_searching = false;
        self.overlay = Overlay::PreviewViewer;
    }

    fn load_preview_more(&mut self) {
        if !self.preview_has_more {
            self.status = "no further preview data offered".to_string();
            return;
        }
        let Some(key) = self.preview_key.clone() else {
            return;
        };
        if let Some(fixture) = self.fixture.as_mut() {
            let Some(preview) = fixture.preview_mut(&key) else {
                self.preview_has_more = false;
                return;
            };
            let more = std::mem::take(&mut preview.more_lines);
            for line in more {
                self.preview_lines.push_line(line);
            }
            preview.has_more = false;
            self.preview_has_more = false;
            self.status = format!(
                "fixture preview continuation loaded; {} lines retained",
                self.preview_lines.len()
            );
            return;
        }
        if matches!(self.preview_cursor, PreviewCursor::None) {
            self.preview_has_more = false;
            self.status = "no further preview data offered".to_string();
            return;
        }
        if let Some(service) = &self.service {
            self.preview_pending = true;
            service.send(RedisCommand::Preview(PreviewCommand::FetchPreviewMore {
                key,
                cursor: self.preview_cursor.clone(),
            }));
            self.status = "loading preview continuation".to_string();
        }
    }

    fn run_viewer_search(&mut self) {
        let pattern = self.viewer_search.trim().to_string();
        self.viewer_searching = false;
        if pattern.is_empty() {
            self.status = "viewer search cleared".to_string();
            return;
        }
        let Some(key) = self.preview_key.clone() else {
            return;
        };
        if let Some(fixture) = &self.fixture {
            let Some(preview) = fixture.preview(&key) else {
                return;
            };
            let lines = fixture.preview_lines(preview);
            let matches: Vec<String> = lines
                .into_iter()
                .filter(|line| line.contains(&pattern))
                .collect();
            let count = matches.len();
            self.preview_lines.clear();
            for line in matches {
                self.preview_lines.push_line(line);
            }
            self.preview_scroll = 0;
            self.status =
                format!("search '{pattern}' in {key}: {count} match(es) (fixture filter)");
            return;
        }
        if let Some(service) = &self.service {
            self.preview_pending = true;
            service.send(RedisCommand::Preview(PreviewCommand::SearchKeyData {
                key,
                key_type: self.preview_type_hint(),
                pattern,
            }));
        }
    }

    fn preview_type_hint(&self) -> RedisKeyType {
        self.filtered_keys()
            .iter()
            .find(|info| Some(&info.key) == self.preview_key.as_ref())
            .map(|info| info.key_type)
            .unwrap_or(RedisKeyType::Unknown)
    }

    fn apply_search_results(&mut self, data: &crate::redis_ops::KeyEditData) {
        let mut lines = Vec::new();
        if let Some(value) = &data.string_value {
            lines.push(value.clone());
        }
        if let Some(fields) = &data.hash_fields {
            for (field, value) in fields {
                lines.push(format!("{field}: {value}"));
            }
        }
        if let Some(items) = &data.list_items {
            for (index, item) in items.iter().enumerate() {
                lines.push(format!("[{index}] {item}"));
            }
        }
        if let Some(members) = &data.set_members {
            for member in members {
                lines.push(format!("- {member}"));
            }
        }
        if let Some(members) = &data.zset_members {
            for (member, score) in members {
                lines.push(format!("{score:.2}: {member}"));
            }
        }
        if let Some(messages) = &data.stream_messages {
            for (id, fields) in messages {
                lines.push(id.clone());
                for (field, value) in fields {
                    lines.push(format!("  {field}: {value}"));
                }
            }
        }
        let count = lines.len();
        self.preview_lines.clear();
        for line in lines {
            self.preview_lines.push_line(line);
        }
        self.preview_scroll = 0;
        self.status = format!("search results for {}: {count} match(es)", data.key);
    }

    // === Server viewers ===

    fn open_section_viewer(&mut self, index: usize) {
        let mut lines = Vec::new();
        match &self.fixture {
            Some(fixture) => {
                if let Some(section) = fixture.server_info.get(index) {
                    lines.push(format!("# {}", section.name));
                    for (key, value) in &section.entries {
                        lines.push(format!("{key}: {value}"));
                    }
                    self.server_viewer_title = format!("server info · {}", section.name);
                }
            }
            None => {
                if let Some(section) = self.server_sections.get(index) {
                    lines.push(format!("# {}", section.name));
                    for (key, value) in &section.entries {
                        lines.push(format!("{key}: {value}"));
                    }
                    self.server_viewer_title = format!("server info · {}", section.name);
                }
            }
        }
        if lines.is_empty() {
            self.status = "server section unavailable".to_string();
            return;
        }
        self.server_viewer_lines.clear();
        for line in lines {
            self.server_viewer_lines.push_line(line);
        }
        self.server_viewer_scroll = 0;
        self.overlay = Overlay::ServerViewer;
        self.status = format!("server info viewer: {}", self.server_viewer_title);
    }

    fn open_server_info_viewer(&mut self) {
        let mut lines = Vec::new();
        let sections = match &self.fixture {
            Some(fixture) => fixture
                .server_info
                .iter()
                .map(|section| (section.name.clone(), section.entries.clone()))
                .collect::<Vec<_>>(),
            None => self
                .server_sections
                .iter()
                .map(|section| (section.name.clone(), section.entries.clone()))
                .collect::<Vec<_>>(),
        };
        for (name, entries) in &sections {
            lines.push(format!("# {name}"));
            for (key, value) in entries {
                lines.push(format!("{key}: {value}"));
            }
            lines.push(String::new());
        }
        self.server_viewer_title = format!("server info · {} sections", sections.len());
        self.server_viewer_lines.clear();
        for line in lines {
            self.server_viewer_lines.push_line(line);
        }
        self.server_viewer_scroll = 0;
        self.overlay = Overlay::ServerViewer;
    }

    fn open_slowlog_viewer(&mut self) {
        let mut lines = vec![
            format!(
                "{:<6} {:<12} {:<10} {:<26} {:<22} {}",
                "id", "duration", "time", "command", "client", "addr"
            ),
            String::new(),
        ];
        for entry in &self.slowlog_entries {
            lines.push(format!(
                "{:<6} {:<12} {:<10} {:<26} {:<22} {}",
                entry.id,
                format!("{}ms", entry.duration_us / 1000),
                crate::redis_ops::format_ttl(entry.timestamp),
                crate::redis_ops::sanitize_for_display(&entry.command),
                entry.client_name,
                entry.client_addr,
            ));
        }
        if self.slowlog_entries.is_empty() {
            lines.push("(no slowlog entries)".to_string());
        }
        self.server_viewer_title = format!(
            "slowlog · {} entries (SLOWLOG GET {SLOWLOG_COUNT})",
            self.slowlog_entries.len()
        );
        self.server_viewer_lines.clear();
        for line in lines {
            self.server_viewer_lines.push_line(line);
        }
        self.server_viewer_scroll = 0;
        self.overlay = Overlay::ServerViewer;
        self.status = "slowlog viewer opened".to_string();
    }

    // === Mutations ===

    fn open_pattern_prompt(&mut self) {
        self.pattern_input.clear();
        self.overlay = Overlay::PatternPrompt;
        self.status =
            "scan pattern: glob with * (empty scans everything), Enter rescans from cursor 0"
                .to_string();
    }

    fn open_delete_prompt(&mut self) {
        let Some(key) = self.selected_key() else {
            self.status = "select a key to delete".to_string();
            return;
        };
        self.typed_key_input.clear();
        self.overlay = Overlay::ConfirmDelete;
        self.status = format!("delete {key}: type the exact key name to confirm, Esc cancels");
    }

    fn open_ttl_prompt(&mut self) {
        let Some(key) = self.selected_key() else {
            self.status = "select a key to set a TTL".to_string();
            return;
        };
        self.ttl_input.clear();
        self.overlay = Overlay::TtlPrompt;
        self.status = format!("ttl for {key}: seconds (-1 persists), Enter stages the plan");
    }

    fn open_rename_prompt(&mut self) {
        let Some(key) = self.selected_key() else {
            self.status = "select a key to rename".to_string();
            return;
        };
        self.rename_input.clear();
        self.overlay = Overlay::RenamePrompt;
        self.status = format!("rename {key}: type the new key name, Enter stages the plan");
    }

    fn confirm_plan(&mut self) {
        let Some(mut plan) = self.operation_plan.take() else {
            self.status = "no operation plan to confirm".to_string();
            return;
        };
        plan.confirmations = plan.confirmations.saturating_add(1);
        if plan.confirmations < plan.confirmations_required {
            self.status = format!(
                "{} on {} is {}; press y again to execute",
                plan.kind.label(),
                plan.target_key,
                plan.risk
            );
            self.operation_plan = Some(plan);
            return;
        }

        if self.readonly {
            self.status = format!(
                "readonly launch blocked {} on {}",
                plan.kind.label(),
                plan.target_key
            );
            self.operation_plan = Some(plan);
            return;
        }

        if let Some(fixture) = self.fixture.as_mut() {
            match fixture.apply_mutation(&plan) {
                Ok(summary) => {
                    // Rescan first so the mutation summary is the final
                    // status the operator sees on the refreshed page.
                    self.rescan_current_page();
                    self.status = format!(
                        "fixture executed {} on {}: {}",
                        plan.kind.label(),
                        plan.target_key,
                        summary
                    );
                }
                Err(message) => {
                    self.error_banner = Some(message);
                    self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                }
            }
        } else if let Some(service) = &self.service {
            match plan.kind {
                OperationKind::DeleteKey => {
                    service.send(RedisCommand::Manage(ManageCommand::DeleteKey {
                        key: plan.target_key.clone(),
                    }))
                }
                OperationKind::SetTtl => {
                    service.send(RedisCommand::Manage(ManageCommand::SetTtl {
                        key: plan.target_key.clone(),
                        ttl_seconds: plan.ttl_seconds.unwrap_or(0),
                    }))
                }
                OperationKind::RenameKey => {
                    service.send(RedisCommand::Manage(ManageCommand::RenameKey {
                        old_key: plan.target_key.clone(),
                        new_key: plan.new_name.clone().unwrap_or_default(),
                    }))
                }
            }
            self.status = format!("{} sent to RedisService", plan.kind.label());
        } else {
            self.status = "no service connection; plan dropped".to_string();
        }
    }

    fn cancel_plan(&mut self) {
        if self.operation_plan.take().is_some() {
            self.status = "operation plan cancelled".to_string();
        } else {
            self.mode = Mode::Browser;
            self.status = "browser mode".to_string();
        }
    }

    fn request_quit(&mut self) {
        let work_active = self.scan_pending
            || self.preview_pending
            || self.server_pending
            || self.operation_plan.is_some();
        if work_active {
            self.overlay = Overlay::ConfirmQuit;
            self.status =
                "quit? a scan/preview fetch or staged plan is active: y quits, any other key stays"
                    .to_string();
            return;
        }
        self.should_quit = true;
    }

    fn show_help(&mut self) {
        self.overlay = Overlay::Help;
        self.status = "help open; any key closes".to_string();
    }

    // === Drawing ===

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                // Header and status each carry two content rows (title+tabs,
                // status+hints) on top of their borders.
                Constraint::Length(4),
                Constraint::Min(12),
                Constraint::Length(4),
            ])
            .split(area);

        frame.render_widget(self.header(), vertical[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(vertical[1]);
        match self.view {
            View::Keys => {
                frame.render_widget(self.key_list(body[0]), body[0]);
                frame.render_widget(self.details_panel(), body[1]);
            }
            View::Server => {
                frame.render_widget(self.server_list(body[0]), body[0]);
                frame.render_widget(self.server_details_panel(), body[1]);
            }
        }
        frame.render_widget(self.status_panel(), vertical[2]);

        match self.overlay {
            Overlay::None => {}
            Overlay::Help => self.draw_help(frame, area),
            Overlay::PreviewViewer => self.draw_preview_viewer(frame, area),
            Overlay::ServerViewer => self.draw_server_viewer(frame, area),
            Overlay::PatternPrompt => self.draw_pattern_prompt(frame, area),
            Overlay::TtlPrompt => self.draw_ttl_prompt(frame, area),
            Overlay::RenamePrompt => self.draw_rename_prompt(frame, area),
            Overlay::ConfirmDelete => self.draw_confirm_delete(frame, area),
            Overlay::ConfirmQuit => self.draw_confirm_quit(frame, area),
        }
        if let Some(error) = &self.error_banner {
            self.draw_error_banner(frame, area, error);
        }
    }

    fn header(&self) -> Paragraph<'_> {
        let source = format!("{:?}", self.source).to_lowercase();
        let keys_mark = if self.scan_pending { "\u{27f3}" } else { "" };
        let server_mark = if self.server_pending { "\u{27f3}" } else { "" };
        let keys_tab = format!("1:keys{keys_mark}");
        let server_tab = format!("2:server{server_mark}");
        let tabs = match self.view {
            View::Keys => format!("[{keys_tab}] {server_tab}"),
            View::Server => format!("{keys_tab} [{server_tab}]"),
        };
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    "Redis Key-Space TUI (pilot)",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  profile={} server={} db={} pattern={}",
                    self.profile_label,
                    self.server_label,
                    self.db,
                    self.pattern_label()
                )),
            ]),
            Line::from(format!(
                "{}  source={} kind={} purpose={} readonly={} restore={} filter={}",
                tabs,
                source,
                self.connection_kind,
                self.purpose,
                self.readonly,
                self.restore,
                self.filter_label()
            )),
        ])
        .block(Block::default().borders(Borders::ALL))
    }

    fn pattern_label(&self) -> String {
        if self.scan_pattern.is_empty() {
            "*".to_string()
        } else {
            self.scan_pattern.clone()
        }
    }

    fn key_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_keys();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.selected(), visible, items.len());
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, info)| {
                let marker = if idx == self.selected() { ">" } else { " " };
                let style = if idx == self.selected() {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<40} {:<8} {}",
                        crate::redis_ops::sanitize_for_display(&info.key),
                        info.key_type.label(),
                        crate::redis_ops::format_ttl(info.ttl)
                    ),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if !lines.is_empty() {
            lines
        } else if self.scan_pending {
            vec![Line::from("loading\u{2026}")]
        } else {
            vec![Line::from(
                "No keys on this page. Press n for the next page, p to change the scan pattern, / to filter.",
            )]
        })
        .block(
            Block::default()
                .title(format!(
                    " keys page {} ({}/{}) cursor={} next={} ",
                    self.page_index,
                    self.selected().saturating_add(1).min(items.len().max(1)),
                    items.len(),
                    self.scan_cursor,
                    self.next_scan_cursor
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn details_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.filtered_keys().get(self.selected()) {
            Some(info) => vec![
                Line::from(format!(
                    "key: {}",
                    crate::redis_ops::sanitize_for_display(&info.key)
                )),
                Line::from(format!(
                    "type: {}  ttl: {} ({})",
                    info.key_type.label(),
                    info.ttl,
                    crate::redis_ops::format_ttl(info.ttl)
                )),
                Line::from(format!(
                    "page {} of keyspace: cursor={} next={}",
                    self.page_index, self.scan_cursor, self.next_scan_cursor
                )),
                Line::from("i/Enter preview  d delete  t ttl  R rename"),
            ],
            None => vec![Line::from(
                "Select a key to inspect type/TTL. Press p to set the scan pattern.",
            )],
        };

        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "operation plan",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!("action: {}", plan.kind.label())));
            lines.push(Line::from(format!(
                "target: {}",
                crate::redis_ops::sanitize_for_display(&plan.target_key)
            )));
            if let Some(ttl) = plan.ttl_seconds {
                lines.push(Line::from(format!("ttl_seconds: {ttl}")));
            }
            if let Some(new_name) = &plan.new_name {
                lines.push(Line::from(format!(
                    "new_name: {}",
                    crate::redis_ops::sanitize_for_display(new_name)
                )));
            }
            lines.push(Line::from(format!("risk: {}", plan.risk)));
            lines.push(Line::from(format!(
                "confirmations: {}/{}",
                plan.confirmations, plan.confirmations_required
            )));
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn server_list(&self, area: Rect) -> Paragraph<'_> {
        let count = self.server_section_count();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.server_selection, visible, count);
        let mut lines = Vec::new();
        for index in start..count.min(start + visible) {
            let (name, entry_count) = match &self.fixture {
                Some(fixture) => fixture
                    .server_info
                    .get(index)
                    .map(|section| (section.name.clone(), section.entries.len()))
                    .unwrap_or_default(),
                None => self
                    .server_sections
                    .get(index)
                    .map(|section| (section.name.clone(), section.entries.len()))
                    .unwrap_or_default(),
            };
            let marker = if index == self.server_selection {
                ">"
            } else {
                " "
            };
            let style = if index == self.server_selection {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(
                format!("{marker} {:<14} {} entries", name, entry_count),
                style,
            )));
        }
        if lines.is_empty() {
            lines.push(if self.server_pending {
                Line::from("loading\u{2026}")
            } else {
                Line::from(
                    "No server info loaded. Press r to fetch INFO sections, s for the slowlog.",
                )
            });
        }
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(format!(
                        " server info ({}/{}) ",
                        (self.server_selection + 1).min(count.max(1)),
                        count
                    ))
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false })
    }

    fn server_details_panel(&self) -> Paragraph<'_> {
        let mut lines = Vec::new();
        match &self.fixture {
            Some(fixture) => {
                if let Some(section) = fixture.server_info.get(
                    self.server_selection
                        .min(fixture.server_info.len().saturating_sub(1)),
                ) {
                    lines.push(Line::from(format!("# {}", section.name)));
                    for (key, value) in section.entries.iter().take(10) {
                        lines.push(Line::from(format!("{key}: {value}")));
                    }
                }
            }
            None => {
                if let Some(section) = self.server_sections.get(
                    self.server_selection
                        .min(self.server_sections.len().saturating_sub(1)),
                ) {
                    lines.push(Line::from(format!("# {}", section.name)));
                    for (key, value) in section.entries.iter().take(10) {
                        lines.push(Line::from(format!("{key}: {value}")));
                    }
                }
            }
        }
        if lines.is_empty() {
            lines.push(Line::from(
                "Server INFO loads here. Press r to refresh, s for the slowlog.",
            ));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("i/Enter section viewer  s slowlog"));
        Paragraph::new(lines)
            .block(Block::default().title(" Server ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn status_panel(&self) -> Paragraph<'_> {
        let hints = if self.mode == Mode::Filter {
            format!(
                "filter: {}\u{258f} Enter apply \u{b7} Esc clear \u{b7} {} match(es)",
                self.filter,
                self.filtered_keys().len()
            )
        } else {
            match self.view {
                View::Keys => KEYS_HINTS.to_string(),
                View::Server => SERVER_HINTS.to_string(),
            }
        };
        Paragraph::new(vec![
            Line::from(format!(
                "{} | mode={} | keys {} match={} | page {} cursor={} next={} | preview {}/{} dropped {} | pending scan={} preview={} server={}",
                self.status,
                mode_label(self.mode),
                self.keys.len(),
                self.filtered_keys().len(),
                self.page_index,
                self.scan_cursor,
                self.next_scan_cursor,
                self.preview_lines.len(),
                self.preview_lines.max_lines,
                self.preview_lines.dropped_lines,
                self.scan_pending,
                self.preview_pending,
                self.server_pending,
            )),
            Line::from(Span::styled(hints, Style::default().fg(Color::DarkGray))),
        ])
        .block(Block::default().title(" Status ").borders(Borders::ALL))
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(78, 84, area);
        frame.render_widget(Clear, popup);
        let help = vec![
            Line::from(Span::styled(
                "Navigation",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("1 keys browser \u{b7} 2 server info (Tab cycles)"),
            Line::from("j/k or arrows move \u{b7} PgUp/PgDn page \u{b7} g/G home/end"),
            Line::from("/ client-side filter on the current page (live, Esc clears)"),
            Line::from(Span::styled(
                "Keys & paging",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("p scan pattern (server-side narrowing, rescans from cursor 0)"),
            Line::from("n next SCAN page (page is replaced; cursor history kept) \u{b7} b back"),
            Line::from("r refresh current view"),
            Line::from(Span::styled(
                "Inspect",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("i/Enter: typed full-screen preview (string/hash/list/set/zset/stream)"),
            Line::from(
                "viewer: j/k PgUp/PgDn scroll \u{b7} n load more \u{b7} / search collection \u{b7} Esc close",
            ),
            Line::from("server view: i/Enter INFO section viewer \u{b7} s slowlog"),
            Line::from(Span::styled(
                "Manage mutations (staged plans, y confirms)",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(
                "d delete (type the exact key name, then y) \u{b7} t ttl seconds \u{b7} R rename",
            ),
            Line::from(
                "every mutation rescans the current page \u{b7} readonly launches block at confirm",
            ),
            Line::from(Span::styled(
                "Sessions",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(
                "q: quit (asks for confirmation while a scan/preview fetch or plan is active)",
            ),
            Line::from("errors appear in a non-modal banner; any key acknowledges"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().title(" Help ").borders(Borders::ALL))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_preview_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.preview_lines.len();
        let mut body: Vec<Line> = if total == 0 {
            vec![Line::from("nothing inspected yet; press i/Enter on a key")]
        } else {
            let start = self.preview_scroll.min(total.saturating_sub(1));
            self.preview_lines
                .range(start, start + visible)
                .into_iter()
                .map(Line::from)
                .collect()
        };
        if self.viewer_searching {
            body.push(Line::from(Span::styled(
                format!("search: {}\u{258f}", self.viewer_search),
                Style::default().fg(Color::Cyan),
            )));
        }
        let load_more = if self.preview_has_more {
            " \u{b7} n load more"
        } else {
            ""
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " preview {} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll{} \u{b7} / search \u{b7} Esc close ",
                            self.preview_key.as_deref().unwrap_or("(none)"),
                            (self.preview_scroll + 1).min(total.max(1)),
                            total,
                            load_more
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_server_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.server_viewer_lines.len();
        let body: Vec<Line> = if total == 0 {
            vec![Line::from("no server data loaded")]
        } else {
            let start = self.server_viewer_scroll.min(total.saturating_sub(1));
            self.server_viewer_lines
                .range(start, start + visible)
                .into_iter()
                .map(Line::from)
                .collect()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " {} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll \u{b7} Esc close ",
                            self.server_viewer_title,
                            (self.server_viewer_scroll + 1).min(total.max(1)),
                            total
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_pattern_prompt(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 22, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from("scan pattern (server-side narrowing):"),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.pattern_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from("empty scans everything"),
                Line::from("Enter rescans from cursor 0 \u{b7} Esc cancels"),
            ])
            .block(
                Block::default()
                    .title(" Scan Pattern ")
                    .borders(Borders::ALL),
            ),
            popup,
        );
    }

    fn draw_ttl_prompt(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 22, area);
        frame.render_widget(Clear, popup);
        let target = self
            .selected_key()
            .unwrap_or_else(|| "no key selected".to_string());
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(format!("ttl seconds for {target}:")),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.ttl_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from("-1 persists the key"),
                Line::from("Enter stages the plan \u{b7} y confirms \u{b7} Esc cancels"),
            ])
            .block(Block::default().title(" Set TTL ").borders(Borders::ALL)),
            popup,
        );
    }

    fn draw_rename_prompt(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(60, 22, area);
        frame.render_widget(Clear, popup);
        let target = self
            .selected_key()
            .unwrap_or_else(|| "no key selected".to_string());
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(format!("new name for {target}:")),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.rename_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from("Enter stages the plan \u{b7} y confirms \u{b7} Esc cancels"),
            ])
            .block(Block::default().title(" Rename Key ").borders(Borders::ALL)),
            popup,
        );
    }

    fn draw_confirm_delete(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(64, 26, area);
        frame.render_widget(Clear, popup);
        let target = self
            .selected_key()
            .unwrap_or_else(|| "no key selected".to_string());
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Delete key (destructive)",
                    Style::default()
                        .fg(Color::Red)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(format!(
                    "target: {}",
                    crate::redis_ops::sanitize_for_display(&target)
                )),
                Line::from("Deleting a key cannot be undone."),
                Line::from("Type the exact key name to confirm:"),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.typed_key_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(
                    "Enter stages the plan (typed confirmation) \u{b7} y executes \u{b7} Esc cancels",
                ),
            ])
            .block(Block::default().title(" Confirm Delete ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_confirm_quit(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 22, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Quit with active work?",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(self.active_work_summary()),
                Line::from("y/Enter: quit \u{b7} any other key: stay"),
            ])
            .block(
                Block::default()
                    .title(" Confirm Quit ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn active_work_summary(&self) -> String {
        let mut parts = Vec::new();
        if self.scan_pending {
            parts.push("key scan in flight".to_string());
        }
        if self.preview_pending {
            parts.push("preview fetch in flight".to_string());
        }
        if self.server_pending {
            parts.push("server fetch in flight".to_string());
        }
        if let Some(plan) = &self.operation_plan {
            parts.push(format!("{} plan on {}", plan.kind.label(), plan.target_key));
        }
        if parts.is_empty() {
            "no active fetches or plans".to_string()
        } else {
            parts.join(", ")
        }
    }

    fn draw_error_banner(&self, frame: &mut Frame, area: Rect, error: &str) {
        let banner = Rect {
            x: area.x + 1,
            y: area.height.saturating_sub(6),
            width: area.width.saturating_sub(2),
            height: 3,
        };
        frame.render_widget(Clear, banner);
        frame.render_widget(
            Paragraph::new(vec![Line::from(Span::styled(
                safe_error_summary(error),
                Style::default().fg(Color::Red),
            ))])
            .block(
                Block::default()
                    .title(" Error (any key acknowledges) ")
                    .borders(Borders::ALL)
                    .style(Style::default().fg(Color::Red)),
            )
            .wrap(Wrap { trim: false }),
            banner,
        );
    }
}

impl BoundedLines {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            dropped_lines: 0,
            max_lines,
            max_bytes,
        }
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn clear(&mut self) {
        self.lines.clear();
        self.bytes = 0;
        self.dropped_lines = 0;
    }

    fn push_text(&mut self, text: &str) {
        for line in text.lines() {
            self.push_line(line.to_string());
        }
    }

    fn push_line(&mut self, mut line: String) {
        if line.len() > self.max_bytes {
            line.truncate(self.max_bytes);
        }
        self.bytes = self.bytes.saturating_add(line.len());
        self.lines.push_back(line);
        while self.lines.len() > self.max_lines || self.bytes > self.max_bytes {
            if let Some(removed) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(removed.len());
                self.dropped_lines = self.dropped_lines.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn range(&self, start: usize, end: usize) -> Vec<String> {
        self.lines
            .iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .cloned()
            .collect()
    }
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = message.replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "redis_password_value",
        "redis_secret_data_value",
        "plaintext_credential_value",
        "raw_connection_url",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Browser => "browser",
        Mode::Filter => "filter",
    }
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn page_value(page: &ScanPage) -> Value {
    json!({
        "cursor": page.cursor,
        "next_cursor": page.next_cursor,
        "keys": page
            .keys
            .iter()
            .map(|info| {
                json!({
                    "key": info.key,
                    "key_type": info.key_type.label(),
                    "ttl": info.ttl
                })
            })
            .collect::<Vec<_>>()
    })
}

fn preview_value(preview: &FixturePreview) -> Value {
    json!({
        "key": preview.key,
        "key_type": preview.key_type.label(),
        "ttl": preview.ttl,
        "size": preview.size,
        "memory_bytes": preview.memory_bytes,
        "lines": preview.lines,
        "has_more": preview.has_more,
        "more_lines": preview.more_lines
    })
}

fn info_section_value(section: &FixtureInfoSection) -> Value {
    json!({
        "name": section.name,
        "entries": section.entries
    })
}

fn slowlog_value(entry: &FixtureSlowLogEntry) -> Value {
    json!({
        "id": entry.id,
        "timestamp": entry.timestamp,
        "duration_us": entry.duration_us,
        "command": entry.command,
        "client_addr": entry.client_addr,
        "client_name": entry.client_name
    })
}

struct StandaloneRedisTabManager {
    render_tx: tokio::sync::mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandaloneRedisTabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow::anyhow!(
            "Redis TUI does not host plugin tabs; use plugin-owned CLI commands instead"
        )
    }
}

impl TabManager for StandaloneRedisTabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "Redis".to_string(),
            plugin_id: "redis".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow::anyhow!("Redis TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Redis TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    fn fixture_launch(readonly: bool) -> RedisTuiLaunch {
        RedisTuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: RedisTuiSource::Fixture,
            fixture_path: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/redis_tui_operations.json")
                    .display()
                    .to_string(),
            ),
            purpose: "operations".to_string(),
            readonly,
            restore: true,
            launch_plan: None,
        }
    }

    fn app() -> RedisTuiApp {
        RedisTuiApp::new(fixture_launch(false)).unwrap()
    }

    fn press(app: &mut RedisTuiApp, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn type_text(app: &mut RedisTuiApp, text: &str) {
        for ch in text.chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
        }
    }

    #[test]
    fn fixture_evidence_has_coverage_and_no_secret_markers() {
        let evidence = build_redis_tui_evidence(&fixture_launch(false)).unwrap();
        let rendered = serde_json::to_string(&evidence).unwrap();
        assert!(rendered.contains("redis_tui_fixture_evidence"));
        assert!(rendered.contains("typed_delete_confirmation"));
        assert!(rendered.contains("scan_paging_cursor_history"));
        assert!(rendered.contains("preview_load_more"));
        assert!(rendered.contains("later candidate"));
        assert_eq!(evidence["secret_leak_scan"]["passed"], json!(true));
        assert_eq!(evidence["quality_gate"]["plugin_id"], json!("redis"));
        assert_eq!(
            evidence["quality_gate"]["layout"]["minimum_viewport"]["width"],
            json!(80)
        );
    }

    #[test]
    fn preflight_redacts_password_material() {
        let launch = RedisTuiLaunch {
            profile_label: "live-redis".to_string(),
            config: Some(RedisConfig {
                host: "redis.internal".to_string(),
                port: 6379,
                password: Some("redis_password_value".to_string()),
                username: Some("default".to_string()),
                db: 2,
                tls: false,
            }),
            source: RedisTuiSource::Profile,
            fixture_path: None,
            purpose: "triage".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("redacted"));
        assert!(!rendered.contains("redis_password_value"));
        assert_eq!(preflight_value(&launch)["ok"], json!(true));
        assert_eq!(preflight_value(&launch)["command"], json!("redis tui"));
        assert_eq!(preflight_value(&launch)["db"], json!(2));
    }

    #[test]
    fn startup_target_error_banner_is_non_modal() {
        let mut app = app();
        assert!(app.error_banner.is_some());
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("target error")
        );
        assert_eq!(app.mode, Mode::Browser, "errors must not hijack the mode");
        assert_eq!(app.keys.len(), 8, "fixture page loads despite the banner");
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "any key acknowledges");
        assert_eq!(app.selection, 1, "the key keeps its normal meaning");
    }

    #[test]
    fn hotkeys_switch_views_and_tab_cycles() {
        let mut app = app();
        assert_eq!(app.view, View::Keys);
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Server);
        assert!(app.status.contains("view: server"));
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.view, View::Keys);
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Keys);
    }

    #[test]
    fn scan_paging_tracks_cursor_state_and_history() {
        let mut app = app();
        assert_eq!(app.scan_cursor, 0);
        assert_eq!(app.next_scan_cursor, 42);
        assert_eq!(app.keys.len(), 8);
        press(&mut app, KeyCode::Char('n'));
        assert_eq!(app.scan_cursor, 42);
        assert_eq!(app.next_scan_cursor, 0);
        assert_eq!(app.keys.len(), 3);
        assert_eq!(app.page_index, 2);
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("end of keyspace"));
        press(&mut app, KeyCode::Char('b'));
        assert_eq!(app.scan_cursor, 0);
        assert_eq!(app.keys.len(), 8);
        assert_eq!(app.page_index, 1);
    }

    #[test]
    fn pattern_prompt_rescans_from_cursor_zero() {
        let mut app = app();
        press(&mut app, KeyCode::Char('n'));
        assert_eq!(app.scan_cursor, 42);
        press(&mut app, KeyCode::Char('p'));
        assert_eq!(app.overlay, Overlay::PatternPrompt);
        type_text(&mut app, "session:*");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::None);
        assert_eq!(app.scan_cursor, 0);
        assert_eq!(app.keys.len(), 3, "session:* narrows the fixture page");
        assert!(app.status.contains("scan pattern set to 'session:*'"));
    }

    #[test]
    fn filter_reports_match_count() {
        let mut app = app();
        press(&mut app, KeyCode::Char('/'));
        assert_eq!(app.mode, Mode::Filter);
        type_text(&mut app, "session");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.mode, Mode::Browser);
        assert_eq!(app.filtered_keys().len(), 3);
        assert!(app.status.contains("(3 matches)"));
        press(&mut app, KeyCode::Char('/'));
        press(&mut app, KeyCode::Esc);
        assert!(app.filter.is_empty());
        assert!(app.status.contains("filter cleared"));
    }

    #[test]
    fn typed_delete_requires_exact_key_name() {
        let mut app = app();
        press(&mut app, KeyCode::Char('d'));
        assert_eq!(app.overlay, Overlay::ConfirmDelete);
        type_text(&mut app, "session:abc124");
        press(&mut app, KeyCode::Enter);
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("typed name does not match"));
        assert_eq!(app.overlay, Overlay::ConfirmDelete);
        type_text(&mut app, "session:abc123");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::None);
        let plan = app.operation_plan.clone().expect("delete plan staged");
        assert_eq!(plan.kind, OperationKind::DeleteKey);
        assert_eq!(plan.target_key, "session:abc123");
        assert_eq!(plan.confirmations_required, 2);
        assert_eq!(
            plan.confirmations, 1,
            "typed entry is the first confirmation"
        );
        assert!(app.status.contains("typed confirmation recorded"));
    }

    #[test]
    fn delete_executes_after_y_and_rescans_page() {
        let mut app = app();
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "session:abc123");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed delete key"));
        assert!(
            !app.keys.iter().any(|info| info.key == "session:abc123"),
            "the deleted key is gone from the rescanned page"
        );
        assert_eq!(app.keys.len(), 7);
    }

    #[test]
    fn readonly_launch_blocks_confirmed_plan() {
        let mut app = RedisTuiApp::new(fixture_launch(true)).unwrap();
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "session:abc123");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("readonly launch blocked"));
        assert!(
            app.keys.iter().any(|info| info.key == "session:abc123"),
            "readonly launches must not mutate the key-space"
        );
    }

    #[test]
    fn erroring_action_shows_banner_and_stays_responsive() {
        let mut app = app();
        app.set_selected(2); // session:locked:ephemeral is the error key
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "session:locked:ephemeral");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.error_banner.is_some());
        assert!(app.status.contains("target error"));
        assert_eq!(app.mode, Mode::Browser);
        assert!(
            app.keys
                .iter()
                .any(|info| info.key == "session:locked:ephemeral"),
            "a failed mutation must not remove the key"
        );
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "banner stays non-modal");
        assert!(app.operation_plan.is_none());
    }

    #[test]
    fn ttl_dialog_stages_plan_and_updates_column() {
        let mut app = app();
        press(&mut app, KeyCode::Char('t'));
        assert_eq!(app.overlay, Overlay::TtlPrompt);
        type_text(&mut app, "120");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::None);
        let plan = app.operation_plan.clone().expect("ttl plan staged");
        assert_eq!(plan.kind, OperationKind::SetTtl);
        assert_eq!(plan.ttl_seconds, Some(120));
        assert!(app.status.contains("ttl plan staged for session:abc123"));
        press(&mut app, KeyCode::Char('y'));
        assert!(app.status.contains("fixture executed set ttl"));
        let updated = app
            .keys
            .iter()
            .find(|info| info.key == "session:abc123")
            .expect("key still present");
        assert_eq!(updated.ttl, 120, "the new TTL is visible after rescan");
    }

    #[test]
    fn rename_dialog_stages_plan_and_rescans() {
        let mut app = app();
        press(&mut app, KeyCode::Char('R'));
        assert_eq!(app.overlay, Overlay::RenamePrompt);
        type_text(&mut app, "session:abc999");
        press(&mut app, KeyCode::Enter);
        let plan = app.operation_plan.clone().expect("rename plan staged");
        assert_eq!(plan.kind, OperationKind::RenameKey);
        assert_eq!(plan.new_name.as_deref(), Some("session:abc999"));
        press(&mut app, KeyCode::Char('y'));
        assert!(app.status.contains("fixture executed rename key"));
        assert!(!app.keys.iter().any(|info| info.key == "session:abc123"));
        assert!(app.keys.iter().any(|info| info.key == "session:abc999"));
    }

    #[test]
    fn preview_viewer_renders_per_type_and_loads_more() {
        let mut app = app();
        app.set_selected(3); // cache:products:42 (hash)
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::PreviewViewer);
        assert!(app.preview_key.as_deref() == Some("cache:products:42"));
        let rendered = app
            .preview_lines
            .range(0, app.preview_lines.len())
            .join("\n");
        assert!(
            rendered.contains("Fields: 64"),
            "hash preview renders fields"
        );
        assert!(app.preview_has_more, "truncated state is visible");
        // string preview
        press(&mut app, KeyCode::Esc);
        app.set_selected(0); // session:abc123 (string)
        press(&mut app, KeyCode::Char('i'));
        assert_eq!(app.overlay, Overlay::PreviewViewer);
        let rendered = app
            .preview_lines
            .range(0, app.preview_lines.len())
            .join("\n");
        assert!(rendered.contains("type: String"));
        assert!(rendered.contains("subject"));
        assert!(!app.preview_has_more);
        press(&mut app, KeyCode::Esc);
        // load more on the truncated hash
        app.set_selected(3);
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('n'));
        assert!(!app.preview_has_more);
        let rendered = app
            .preview_lines
            .range(0, app.preview_lines.len())
            .join("\n");
        assert!(rendered.contains("continuation loaded"));
        assert!(app.status.contains("fixture preview continuation loaded"));
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("no further preview data"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn viewer_search_reports_match_count() {
        let mut app = app();
        app.set_selected(3); // cache:products:42
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('/'));
        assert!(app.viewer_searching);
        type_text(&mut app, "sku");
        press(&mut app, KeyCode::Enter);
        assert!(!app.viewer_searching);
        assert!(app.status.contains("search 'sku'"));
        assert!(app.status.contains("3 match(es)"));
        let rendered = app
            .preview_lines
            .range(0, app.preview_lines.len())
            .join("\n");
        assert!(
            rendered.contains("sku: WIDG-0044"),
            "search spans the continuation"
        );
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn server_view_shows_info_and_slowlog() {
        let mut app = app();
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Server);
        assert_eq!(app.server_section_count(), 4);
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::ServerViewer);
        let rendered = app
            .server_viewer_lines
            .range(0, app.server_viewer_lines.len())
            .join("\n");
        assert!(rendered.contains("# server"));
        assert!(rendered.contains("redis_version: 7.2.5"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        press(&mut app, KeyCode::Char('s'));
        assert_eq!(app.overlay, Overlay::ServerViewer);
        let rendered = app
            .server_viewer_lines
            .range(0, app.server_viewer_lines.len())
            .join("\n");
        assert!(rendered.contains("KEYS *"), "slowlog viewer shows entries");
        assert!(rendered.contains("triage-operator"));
        press(&mut app, KeyCode::Esc);
    }

    #[test]
    fn empty_pattern_page_shows_empty_state() {
        let mut app = app();
        press(&mut app, KeyCode::Char('p'));
        type_text(&mut app, "missing:*");
        press(&mut app, KeyCode::Enter);
        assert!(app.keys.is_empty());
        assert_eq!(app.scan_cursor, 0);
        // An empty first page still continues the SCAN at the page cursor;
        // the TUI shows the empty state and the operator can keep paging.
        assert_eq!(app.next_scan_cursor, 42);
        press(&mut app, KeyCode::Char('n'));
        assert!(app.keys.is_empty());
        assert_eq!(app.next_scan_cursor, 0, "the final page ends the scan");
        press(&mut app, KeyCode::Char('d'));
        assert!(
            app.status.contains("select a key to delete"),
            "mutations refuse to stage with nothing selected"
        );
        assert!(app.operation_plan.is_none());
    }

    #[test]
    fn quit_with_active_plan_requires_confirmation() {
        let mut app = app();
        press(&mut app, KeyCode::Char('t'));
        type_text(&mut app, "60");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('q'));
        assert_eq!(app.overlay, Overlay::ConfirmQuit);
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Char('q'));
        press(&mut app, KeyCode::Char('y'));
        assert!(app.should_quit);
    }

    #[test]
    fn glob_match_supports_scan_patterns() {
        assert!(glob_match("session:*", "session:abc123"));
        assert!(!glob_match("session:*", "cache:products:42"));
        assert!(glob_match("*:42", "cache:products:42"));
        assert!(glob_match("", "anything"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exact-ish"));
        assert!(glob_match("a*b*c", "a-x-b-y-c"));
        assert!(!glob_match("a*b*c", "a-x-y-c"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn page_keys_move_selection() {
        let mut app = app();
        press(&mut app, KeyCode::PageDown);
        assert_eq!(app.selected(), 7.min(app.filtered_keys().len() - 1));
        press(&mut app, KeyCode::PageUp);
        assert_eq!(app.selected(), 0);
    }
}
