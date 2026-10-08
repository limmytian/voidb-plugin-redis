//! Redis service event types.
//!
//! Events are sent from the background service task back to the TUI plugin
//! via an unbounded mpsc channel. The TUI polls events via
//! `RedisService::poll_event()` in its synchronous `Plugin::update()`.

use crate::redis_ops::{
    ClientEntry, CommandResult, InfoSection, KeyEditData, KeyEditMore, KeyInfo, KeyPreview,
    PreviewMore, SlowLogEntry,
};

/// Top-level event enum for the Redis service.
///
/// Connection lifecycle events are at the top level.
/// Domain events are grouped into sub-enums matching the command domains.
#[derive(Debug)]
pub enum RedisEvent {
    // --- Connection lifecycle ---
    /// Connection established successfully.
    Connected,

    /// Connection closed.
    Disconnected,

    // --- Domain events ---
    /// Key scanning results.
    Scan(ScanEvent),

    /// Key preview results.
    Preview(PreviewEvent),

    /// Key edit/write results.
    Edit(EditEvent),

    /// Key management results.
    Manage(ManageEvent),

    /// Server information results.
    Server(ServerEvent),

    /// Raw command execution result.
    CommandResult(CommandResult),

    /// An error occurred during an operation.
    Error(String),
}

/// Key scanning events.
#[derive(Debug)]
pub enum ScanEvent {
    /// Keys scanned with next cursor position.
    KeysScanned {
        keys: Vec<KeyInfo>,
        next_cursor: u64,
    },

    /// Database sizes loaded.
    DbSizesLoaded(Vec<(u8, u64)>),
}

/// Key preview events.
#[derive(Debug)]
pub enum PreviewEvent {
    /// Full key preview loaded.
    PreviewLoaded(KeyPreview),

    /// More preview data loaded.
    PreviewMoreLoaded(PreviewMore),

    /// Full key data loaded for editor.
    KeyDataLoaded(KeyEditData),

    /// More editor data loaded.
    KeyDataMoreLoaded(KeyEditMore),

    /// Search results loaded for editor.
    SearchDataLoaded(KeyEditData),
}

/// Key edit/write events.
#[derive(Debug)]
pub enum EditEvent {
    /// Write operation completed successfully.
    WriteSuccess(String),
}

/// Key management events.
#[derive(Debug)]
pub enum ManageEvent {
    /// Key deleted.
    KeyDeleted(String),

    /// Key renamed.
    KeyRenamed { old_key: String, new_key: String },

    /// TTL set/removed.
    TtlSet(String),
}

/// Server information events.
#[derive(Debug)]
pub enum ServerEvent {
    /// Server INFO loaded.
    ServerInfoLoaded(Vec<InfoSection>),

    /// Slow log entries loaded.
    SlowLogLoaded(Vec<SlowLogEntry>),

    /// Client list loaded.
    ClientListLoaded(Vec<ClientEntry>),
}
