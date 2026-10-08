//! Redis service command types.
//!
//! Commands are sent from the TUI plugin to the background service task
//! via an unbounded mpsc channel. Each domain (scan, preview, edit, manage,
//! server, command execution) has its own sub-enum for organization.

use tokio::sync::oneshot;

use crate::redis_ops::{EditorCursor, PreviewCursor, RedisKeyType};

/// Top-level command enum for the Redis service.
///
/// Connection lifecycle commands are at the top level.
/// Domain-specific commands are grouped into sub-enums.
#[derive(Debug)]
pub enum RedisCommand {
    // --- Connection lifecycle ---
    /// Establish a connection to the Redis server.
    /// The oneshot reply signals success or failure.
    Connect {
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Disconnect and shut down the background task.
    Disconnect,

    // --- Domain sub-commands ---
    /// Key scanning and discovery operations.
    Scan(ScanCommand),

    /// Key value preview operations (read-only).
    Preview(PreviewCommand),

    /// Key editing / write operations.
    Edit(EditCommand),

    /// Key management operations (delete, rename, TTL).
    Manage(ManageCommand),

    /// Server information and monitoring operations.
    Server(ServerCommand),

    /// Raw command execution.
    ExecuteCommand { command: String },
}

/// Key scanning and discovery commands.
#[derive(Debug)]
pub enum ScanCommand {
    /// Scan keys matching an optional pattern.
    ScanKeys {
        cursor: u64,
        pattern: Option<String>,
    },

    /// Fetch database sizes (key counts per DB).
    FetchDbSizes,
}

/// Key value preview commands (read-only).
#[derive(Debug)]
pub enum PreviewCommand {
    /// Fetch a full preview of a key's value.
    FetchPreview { key: String },

    /// Fetch more preview data using a cursor.
    FetchPreviewMore { key: String, cursor: PreviewCursor },

    /// Fetch full key data for the editor.
    FetchKeyData { key: String },

    /// Fetch more editor data using a cursor.
    FetchKeyDataMore { key: String, cursor: EditorCursor },

    /// Search within a key's collection data.
    SearchKeyData {
        key: String,
        key_type: RedisKeyType,
        pattern: String,
    },
}

/// Key editing / write commands.
#[derive(Debug)]
pub enum EditCommand {
    /// Set a string value.
    SetString { key: String, value: String },

    /// Set a hash field.
    HashSet {
        key: String,
        field: String,
        value: String,
    },

    /// Delete a hash field.
    HashDelete { key: String, field: String },

    /// Push a value to a list (left or right).
    ListPush {
        key: String,
        value: String,
        left: bool,
    },

    /// Remove a value from a list.
    ListRemove { key: String, value: String },

    /// Add a member to a set.
    SetAdd { key: String, member: String },

    /// Remove a member from a set.
    SetRemove { key: String, member: String },

    /// Add a member to a sorted set with score.
    ZSetAdd {
        key: String,
        member: String,
        score: f64,
    },

    /// Remove a member from a sorted set.
    ZSetRemove { key: String, member: String },
}

/// Key management commands (delete, rename, TTL).
#[derive(Debug)]
pub enum ManageCommand {
    /// Delete a key.
    DeleteKey { key: String },

    /// Rename a key.
    RenameKey { old_key: String, new_key: String },

    /// Set or remove TTL on a key.
    SetTtl { key: String, ttl_seconds: i64 },
}

/// Server information and monitoring commands.
#[derive(Debug)]
pub enum ServerCommand {
    /// Fetch full server INFO.
    FetchServerInfo,

    /// Fetch slow log entries.
    FetchSlowLog { count: usize },

    /// Fetch client list.
    FetchClientList,
}
