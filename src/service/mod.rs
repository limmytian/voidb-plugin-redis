//! Redis service layer.
//!
//! This module provides `RedisService`, the service facade for the Redis
//! plugin. It follows the MySqlService convention:
//!
//! - Background tokio task processes commands asynchronously
//! - `send()` dispatches commands via unbounded mpsc channel (non-blocking)
//! - `poll_event()` drains events via `try_recv()` (non-blocking)
//! - Render notifications fire after every event emission (per D-08/D-11)
//!
//! # Connection Lifecycle
//!
//! The service starts without a connection. A `Connect` command creates a
//! persistent `MultiplexedConnection` that is reused for all subsequent
//! operations. On `Disconnect` (or sender drop), the connection is dropped.

pub mod agent_live;
pub mod commands;
pub mod events;

pub use commands::{
    EditCommand, ManageCommand, PreviewCommand, RedisCommand, ScanCommand, ServerCommand,
};
pub use events::{
    EditEvent, ManageEvent, PreviewEvent, RedisEvent, ScanEvent, ServerEvent,
};

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::config::RedisConfig;
use crate::redis_ops;
use crate::redis_ops::{
    CommandExecResult, KeyEditDataResult, ScanResult, ServerInfoResult, WriteResult,
};
use voidb_core::TabManager;

/// Internal mode: channel-based (TUI) or direct (CLI).
enum ServiceMode {
    Channel {
        cmd_tx: mpsc::UnboundedSender<RedisCommand>,
        event_rx: mpsc::UnboundedReceiver<RedisEvent>,
        _task: tokio::task::JoinHandle<()>,
    },
    Direct {
        url: String,
        db: u8,
    },
}

/// Redis service facade.
///
/// Owns the command sender and event receiver channels. The background
/// task runs on the shared tokio runtime via `runtime.spawn()`.
///
/// # Send + Sync
///
/// `RedisService` is `Send` but NOT `Sync` (because `UnboundedReceiver`
/// is `!Sync`). Plugin structs must wrap it in `std::sync::Mutex` to
/// satisfy `Plugin: Send + Sync`. Since `Plugin::update(&mut self)` has
/// exclusive access, the Mutex is never contended.
pub struct RedisService {
    mode: ServiceMode,
}

/// One Redis transport retained for WATCH/MULTI, selected DB, blocking, and
/// subscription state across serialized agent calls.
pub struct PersistentRedisConnection {
    connection: Option<redis::aio::MultiplexedConnection>,
}

impl PersistentRedisConnection {
    pub async fn open(config: &RedisConfig) -> Result<Self, String> {
        Ok(Self {
            connection: Some(RedisService::open_connection(&config.to_url(), config.db).await?),
        })
    }

    pub async fn execute(&mut self, command: &str) -> Result<String, String> {
        let parts = redis_ops::parse_redis_command(command)?;
        let mut command = redis::cmd(&parts[0]);
        for argument in &parts[1..] {
            command.arg(argument);
        }
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| "Redis session is closed".to_string())?;
        let value: redis::Value = command
            .query_async(connection)
            .await
            .map_err(|_| "Redis session command failed".to_string())?;
        Ok(redis_ops::format_redis_value(&value, 0))
    }

    pub async fn close(mut self) {
        if let Some(mut connection) = self.connection.take() {
            let _: Result<redis::Value, _> = redis::cmd("DISCARD").query_async(&mut connection).await;
            let _: Result<redis::Value, _> = redis::cmd("UNWATCH").query_async(&mut connection).await;
        }
    }
}

impl RedisService {
    /// Create a new RedisService with a background processing task.
    ///
    /// The service starts without an active connection. Send a `Connect`
    /// command to establish the connection.
    pub fn new(
        config: RedisConfig,
        tabs: Arc<dyn TabManager>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RedisCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<RedisEvent>();

        let task = runtime.spawn(Self::background_task(cmd_rx, event_tx, tabs, config));

        Self {
            mode: ServiceMode::Channel {
                cmd_tx,
                event_rx,
                _task: task,
            },
        }
    }

    /// Create a direct service for CLI use (no background task, no channel overhead).
    pub fn new_direct(config: RedisConfig) -> Self {
        Self {
            mode: ServiceMode::Direct {
                url: config.to_url(),
                db: config.db,
            },
        }
    }

    /// Send a command to the background service task.
    ///
    /// This is non-blocking -- safe to call from the synchronous
    /// `Plugin::update()` context.
    pub fn send(&self, cmd: RedisCommand) {
        if let ServiceMode::Channel { cmd_tx, .. } = &self.mode {
            let _ = cmd_tx.send(cmd);
        }
    }

    /// Poll for the next event from the service.
    ///
    /// Returns `Some(event)` if available, `None` otherwise.
    /// Non-blocking, suitable for calling from `Plugin::update()`.
    pub fn poll_event(&mut self) -> Option<RedisEvent> {
        if let ServiceMode::Channel { event_rx, .. } = &mut self.mode {
            event_rx.try_recv().ok()
        } else {
            None
        }
    }

    // === Direct async methods for CLI use ===

    /// Scan keys matching a pattern. Direct mode only.
    pub async fn scan_keys(&self, cursor: u64, pattern: Option<&str>) -> ScanResult {
        match &self.mode {
            ServiceMode::Direct { url, db } => redis_ops::scan_keys(url, *db, cursor, pattern).await,
            ServiceMode::Channel { .. } => Err("scan_keys requires Direct mode".to_string()),
        }
    }

    /// Fetch a lightweight key preview. Direct mode only.
    pub async fn fetch_key_preview(&self, key: &str) -> redis_ops::PreviewResult {
        match &self.mode {
            ServiceMode::Direct { url, db } => redis_ops::fetch_key_preview(url, *db, key).await,
            ServiceMode::Channel { .. } => Err("fetch_key_preview requires Direct mode".to_string()),
        }
    }

    /// Fetch full key data for display/editing. Direct mode only.
    pub async fn fetch_key_data(&self, key: &str) -> KeyEditDataResult {
        match &self.mode {
            ServiceMode::Direct { url, db } => redis_ops::fetch_key_data(url, *db, key).await,
            ServiceMode::Channel { .. } => Err("fetch_key_data requires Direct mode".to_string()),
        }
    }

    /// Set a string key. Direct mode only.
    pub async fn set_string(&self, key: &str, value: &str) -> WriteResult {
        match &self.mode {
            ServiceMode::Direct { url, db } => redis_ops::set_string(url, *db, key, value).await,
            ServiceMode::Channel { .. } => Err("set_string requires Direct mode".to_string()),
        }
    }

    /// Delete a key. Direct mode only.
    pub async fn delete_key(&self, key: &str) -> WriteResult {
        match &self.mode {
            ServiceMode::Direct { url, db } => redis_ops::delete_key(url, *db, key).await,
            ServiceMode::Channel { .. } => Err("delete_key requires Direct mode".to_string()),
        }
    }

    /// Set or remove a key TTL. Direct mode only.
    pub async fn set_ttl(&self, key: &str, ttl_seconds: i64) -> WriteResult {
        match &self.mode {
            ServiceMode::Direct { url, db } => redis_ops::set_ttl(url, *db, key, ttl_seconds).await,
            ServiceMode::Channel { .. } => Err("set_ttl requires Direct mode".to_string()),
        }
    }

    /// Fetch server INFO sections. Direct mode only.
    pub async fn fetch_server_info(&self) -> ServerInfoResult {
        match &self.mode {
            ServiceMode::Direct { url, .. } => redis_ops::fetch_server_info(url).await,
            ServiceMode::Channel { .. } => Err("fetch_server_info requires Direct mode".to_string()),
        }
    }

    /// Execute a raw Redis command string. Direct mode only.
    pub async fn execute_command(&self, command: &str) -> CommandExecResult {
        match &self.mode {
            ServiceMode::Direct { url, db } => redis_ops::execute_command(url, *db, command).await,
            ServiceMode::Channel { .. } => Err("execute_command requires Direct mode".to_string()),
        }
    }

    /// Background task that processes commands using a persistent connection.
    async fn background_task(
        mut cmd_rx: mpsc::UnboundedReceiver<RedisCommand>,
        event_tx: mpsc::UnboundedSender<RedisEvent>,
        tabs: Arc<dyn TabManager>,
        config: RedisConfig,
    ) {
        let url = config.to_url();
        let db = config.db;
        let mut conn: Option<redis::aio::MultiplexedConnection> = None;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                // === Connection lifecycle ===
                RedisCommand::Connect { reply } => {
                    match Self::open_connection(&url, db).await {
                        Ok(c) => {
                            conn = Some(c);
                            let _ = event_tx.send(RedisEvent::Connected);
                            let _ = tabs.request_render();
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e.clone()));
                            let _ = event_tx.send(RedisEvent::Error(e));
                            let _ = tabs.request_render();
                        }
                    }
                }

                RedisCommand::Disconnect => {
                    drop(conn.take());
                    let _ = event_tx.send(RedisEvent::Disconnected);
                    let _ = tabs.request_render();
                    break;
                }

                // === Scan commands ===
                RedisCommand::Scan(scan_cmd) => {
                    Self::handle_scan(&url, db, scan_cmd, &event_tx, &tabs).await;
                }

                // === Preview commands ===
                RedisCommand::Preview(preview_cmd) => {
                    Self::handle_preview(&url, db, preview_cmd, &event_tx, &tabs).await;
                }

                // === Edit commands ===
                RedisCommand::Edit(edit_cmd) => {
                    Self::handle_edit(&url, db, edit_cmd, &event_tx, &tabs).await;
                }

                // === Manage commands ===
                RedisCommand::Manage(manage_cmd) => {
                    Self::handle_manage(&url, db, manage_cmd, &event_tx, &tabs).await;
                }

                // === Server commands ===
                RedisCommand::Server(server_cmd) => {
                    Self::handle_server(&url, server_cmd, &event_tx, &tabs).await;
                }

                // === Raw command execution ===
                RedisCommand::ExecuteCommand { command } => {
                    match redis_ops::execute_command(&url, db, &command).await {
                        Ok(result) => {
                            let _ = event_tx.send(RedisEvent::CommandResult(result));
                        }
                        Err(e) => {
                            let _ = event_tx.send(RedisEvent::Error(e));
                        }
                    }
                    let _ = tabs.request_render();
                }
            }
        }

        // Loop exits on sender drop or Disconnect
        drop(conn);
    }

    /// Open a multiplexed connection with optional DB select.
    async fn open_connection(url: &str, db: u8) -> Result<redis::aio::MultiplexedConnection, String> {
        let client = redis::Client::open(url).map_err(|e| format!("Client error: {}", e))?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;
        if db > 0 {
            redis::cmd("SELECT")
                .arg(db)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| format!("SELECT failed: {}", e))?;
        }
        Ok(conn)
    }

    // === Scan command dispatch ===

    async fn handle_scan(
        url: &str,
        db: u8,
        cmd: ScanCommand,
        event_tx: &mpsc::UnboundedSender<RedisEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            ScanCommand::ScanKeys { cursor, pattern } => {
                match redis_ops::scan_keys(url, db, cursor, pattern.as_deref()).await {
                    Ok((keys, next_cursor)) => {
                        let _ = event_tx.send(RedisEvent::Scan(ScanEvent::KeysScanned {
                            keys,
                            next_cursor,
                        }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            ScanCommand::FetchDbSizes => {
                match redis_ops::fetch_db_sizes(url).await {
                    Ok(sizes) => {
                        let _ = event_tx.send(RedisEvent::Scan(ScanEvent::DbSizesLoaded(sizes)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }

    // === Preview command dispatch ===

    async fn handle_preview(
        url: &str,
        db: u8,
        cmd: PreviewCommand,
        event_tx: &mpsc::UnboundedSender<RedisEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            PreviewCommand::FetchPreview { key } => {
                match redis_ops::fetch_key_preview(url, db, &key).await {
                    Ok(preview) => {
                        let _ = event_tx.send(RedisEvent::Preview(PreviewEvent::PreviewLoaded(preview)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            PreviewCommand::FetchPreviewMore { key, cursor } => {
                match redis_ops::fetch_preview_more(url, db, &key, cursor).await {
                    Ok(more) => {
                        let _ = event_tx.send(RedisEvent::Preview(PreviewEvent::PreviewMoreLoaded(more)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            PreviewCommand::FetchKeyData { key } => {
                match redis_ops::fetch_key_data(url, db, &key).await {
                    Ok(data) => {
                        let _ = event_tx.send(RedisEvent::Preview(PreviewEvent::KeyDataLoaded(data)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            PreviewCommand::FetchKeyDataMore { key, cursor } => {
                match redis_ops::fetch_key_data_more(url, db, &key, cursor).await {
                    Ok(more) => {
                        let _ = event_tx.send(RedisEvent::Preview(PreviewEvent::KeyDataMoreLoaded(more)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            PreviewCommand::SearchKeyData { key, key_type, pattern } => {
                match redis_ops::search_key_data(url, db, &key, key_type, &pattern).await {
                    Ok(data) => {
                        let _ = event_tx.send(RedisEvent::Preview(PreviewEvent::SearchDataLoaded(data)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }

    // === Edit command dispatch ===

    async fn handle_edit(
        url: &str,
        db: u8,
        cmd: EditCommand,
        event_tx: &mpsc::UnboundedSender<RedisEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        let result = match cmd {
            EditCommand::SetString { key, value } => {
                redis_ops::set_string(url, db, &key, &value).await
            }
            EditCommand::HashSet { key, field, value } => {
                redis_ops::hash_set(url, db, &key, &field, &value).await
            }
            EditCommand::HashDelete { key, field } => {
                redis_ops::hash_delete(url, db, &key, &field).await
            }
            EditCommand::ListPush { key, value, left } => {
                redis_ops::list_push(url, db, &key, &value, left).await
            }
            EditCommand::ListRemove { key, value } => {
                redis_ops::list_remove(url, db, &key, &value).await
            }
            EditCommand::SetAdd { key, member } => {
                redis_ops::set_add(url, db, &key, &member).await
            }
            EditCommand::SetRemove { key, member } => {
                redis_ops::set_remove(url, db, &key, &member).await
            }
            EditCommand::ZSetAdd { key, member, score } => {
                redis_ops::zset_add(url, db, &key, &member, score).await
            }
            EditCommand::ZSetRemove { key, member } => {
                redis_ops::zset_remove(url, db, &key, &member).await
            }
        };

        match result {
            Ok(msg) => {
                let _ = event_tx.send(RedisEvent::Edit(EditEvent::WriteSuccess(msg)));
            }
            Err(e) => {
                let _ = event_tx.send(RedisEvent::Error(e));
            }
        }
        let _ = tabs.request_render();
    }

    // === Manage command dispatch ===

    async fn handle_manage(
        url: &str,
        db: u8,
        cmd: ManageCommand,
        event_tx: &mpsc::UnboundedSender<RedisEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            ManageCommand::DeleteKey { key } => {
                match redis_ops::delete_key(url, db, &key).await {
                    Ok(msg) => {
                        let _ = event_tx.send(RedisEvent::Manage(ManageEvent::KeyDeleted(msg)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            ManageCommand::RenameKey { old_key, new_key } => {
                match redis_ops::rename_key(url, db, &old_key, &new_key).await {
                    Ok(_) => {
                        let _ = event_tx.send(RedisEvent::Manage(ManageEvent::KeyRenamed {
                            old_key,
                            new_key,
                        }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            ManageCommand::SetTtl { key, ttl_seconds } => {
                match redis_ops::set_ttl(url, db, &key, ttl_seconds).await {
                    Ok(msg) => {
                        let _ = event_tx.send(RedisEvent::Manage(ManageEvent::TtlSet(msg)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }

    // === Server command dispatch ===

    async fn handle_server(
        url: &str,
        cmd: ServerCommand,
        event_tx: &mpsc::UnboundedSender<RedisEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            ServerCommand::FetchServerInfo => {
                match redis_ops::fetch_server_info(url).await {
                    Ok(info) => {
                        let _ = event_tx.send(RedisEvent::Server(ServerEvent::ServerInfoLoaded(info)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            ServerCommand::FetchSlowLog { count } => {
                match redis_ops::fetch_slowlog(url, count).await {
                    Ok(entries) => {
                        let _ = event_tx.send(RedisEvent::Server(ServerEvent::SlowLogLoaded(entries)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
            ServerCommand::FetchClientList => {
                match redis_ops::fetch_client_list(url).await {
                    Ok(clients) => {
                        let _ = event_tx.send(RedisEvent::Server(ServerEvent::ClientListLoaded(clients)));
                    }
                    Err(e) => {
                        let _ = event_tx.send(RedisEvent::Error(e));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === Send/Sync compile-time assertions ===

    fn assert_send<T: Send>() {}

    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn service_is_send() {
        assert_send::<RedisService>();
    }

    #[test]
    fn command_is_send() {
        assert_send::<RedisCommand>();
    }

    #[test]
    fn event_is_send() {
        assert_send::<RedisEvent>();
    }

    #[test]
    fn mutex_service_is_send_sync() {
        assert_send_sync::<std::sync::Mutex<RedisService>>();
    }
}
