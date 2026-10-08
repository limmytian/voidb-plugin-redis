//! Async Redis operations — scan, preview, write, key management, command execution

use std::time::Instant;

// ── Shared types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisKeyType {
    String,
    Hash,
    List,
    Set,
    ZSet,
    Stream,
    Unknown,
}

impl RedisKeyType {
    pub fn from_redis_type(s: &str) -> Self {
        match s {
            "string" => Self::String,
            "hash" => Self::Hash,
            "list" => Self::List,
            "set" => Self::Set,
            "zset" => Self::ZSet,
            "stream" => Self::Stream,
            _ => Self::Unknown,
        }
    }

    pub fn icon(&self) -> &'static str {
        match self {
            Self::String => "S",
            Self::Hash => "H",
            Self::List => "L",
            Self::Set => "T",
            Self::ZSet => "Z",
            Self::Stream => "X",
            Self::Unknown => "?",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::String => "String",
            Self::Hash => "Hash",
            Self::List => "List",
            Self::Set => "Set",
            Self::ZSet => "ZSet",
            Self::Stream => "Stream",
            Self::Unknown => "Unknown",
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyInfo {
    pub key: String,
    pub key_type: RedisKeyType,
    pub ttl: i64,
}

#[allow(dead_code)]
/// Cursor for paginated preview loading.
#[derive(Debug, Clone)]
pub enum PreviewCursor {
    None,
    StringOffset(u64),
    HashCursor(u64),
    ListOffset(u64),
    SetCursor(u64),
    ZSetOffset(u64),
    StreamLastId(String),
}

#[derive(Debug)]
pub struct KeyPreview {
    pub key: String,
    pub key_type: RedisKeyType,
    pub ttl: i64,
    pub size: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub value_text: String,
    pub cursor: PreviewCursor,
    pub has_more: bool,
}

/// Result of loading the next batch of preview data.
#[derive(Debug)]
pub struct PreviewMore {
    pub value_text: String,
    pub cursor: PreviewCursor,
    pub has_more: bool,
}

pub type PreviewMoreResult = Result<PreviewMore, String>;

/// Cursor for paginated editor loading (collections).
#[derive(Debug, Clone)]
pub enum EditorCursor {
    None,
    HashCursor(u64),
    ListOffset(u64),
    SetCursor(u64),
    ZSetOffset(u64),
    StreamLastId(String),
}

/// Full key data for the editor
#[derive(Debug)]
pub struct KeyEditData {
    pub key: String,
    pub key_type: RedisKeyType,
    pub ttl: i64,
    pub size: u64,
    pub string_value: Option<String>,
    pub string_truncated: bool,
    pub hash_fields: Option<Vec<(String, String)>>,
    pub list_items: Option<Vec<String>>,
    pub set_members: Option<Vec<String>>,
    pub zset_members: Option<Vec<(String, f64)>>,
    #[allow(clippy::type_complexity)]
    pub stream_messages: Option<Vec<(String, Vec<(String, String)>)>>,
    pub collection_cursor: EditorCursor,
    pub collection_has_more: bool,
}

/// Result of loading more editor data.
#[derive(Debug)]
pub struct KeyEditMore {
    pub hash_fields: Option<Vec<(String, String)>>,
    pub list_items: Option<Vec<String>>,
    pub set_members: Option<Vec<String>>,
    pub zset_members: Option<Vec<(String, f64)>>,
    #[allow(clippy::type_complexity)]
    pub stream_messages: Option<Vec<(String, Vec<(String, String)>)>>,
    pub cursor: EditorCursor,
    pub has_more: bool,
}

pub type KeyEditMoreResult = Result<KeyEditMore, String>;

#[derive(Debug)]
pub struct CommandResult {
    pub command: String,
    pub output: String,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// Parsed section from INFO command
#[derive(Debug, Clone)]
pub struct InfoSection {
    pub name: String,
    pub entries: Vec<(String, String)>,
}

/// Parsed slow log entry
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SlowLogEntry {
    pub id: i64,
    pub timestamp: i64,
    pub duration_us: i64,
    pub command: String,
    pub client_addr: String,
    pub client_name: String,
}

/// Parsed CLIENT LIST entry
#[derive(Debug, Clone)]
pub struct ClientEntry {
    pub id: String,
    pub addr: String,
    pub name: String,
    pub db: String,
    pub cmd: String,
    pub age: String,
    pub idle: String,
    pub flags: String,
}

pub type ScanResult = Result<(Vec<KeyInfo>, u64), String>;
pub type PreviewResult = Result<KeyPreview, String>;
pub type DbSizeResult = Result<Vec<(u8, u64)>, String>;
pub type KeyEditDataResult = Result<KeyEditData, String>;
pub type WriteResult = Result<String, String>;
pub type CommandExecResult = Result<CommandResult, String>;
pub type ServerInfoResult = Result<Vec<InfoSection>, String>;
pub type SlowLogResult = Result<Vec<SlowLogEntry>, String>;
pub type ClientListResult = Result<Vec<ClientEntry>, String>;

// ── Helper: open connection with db select ────────────────────────────────────

async fn open_conn(url: &str, db: u8) -> Result<redis::aio::MultiplexedConnection, String> {
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

// ── Read operations (existing, extracted from browser.rs) ─────────────────────

pub async fn scan_keys(url: &str, db: u8, cursor: u64, pattern: Option<&str>) -> ScanResult {
    let mut conn = open_conn(url, db).await?;

    let mut cmd = redis::cmd("SCAN");
    cmd.arg(cursor);
    if let Some(p) = pattern {
        cmd.arg("MATCH").arg(p);
    }
    cmd.arg("COUNT").arg(200);

    let (next_cursor, keys): (u64, Vec<String>) = cmd
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("SCAN failed: {}", e))?;

    if keys.is_empty() {
        return Ok((Vec::new(), next_cursor));
    }

    let mut pipe = redis::pipe();
    for key in &keys {
        pipe.cmd("TYPE").arg(key.as_str());
        pipe.cmd("TTL").arg(key.as_str());
    }
    let results: Vec<redis::Value> = pipe
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("Pipeline failed: {}", e))?;

    let mut key_infos = Vec::with_capacity(keys.len());
    for (i, key) in keys.into_iter().enumerate() {
        let type_str = match &results.get(i * 2) {
            Some(redis::Value::SimpleString(s)) => s.clone(),
            Some(redis::Value::BulkString(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => "unknown".to_string(),
        };
        let ttl = match results.get(i * 2 + 1) {
            Some(redis::Value::Int(n)) => *n,
            _ => -1,
        };
        key_infos.push(KeyInfo {
            key,
            key_type: RedisKeyType::from_redis_type(&type_str),
            ttl,
        });
    }

    Ok((key_infos, next_cursor))
}

pub async fn fetch_key_preview(url: &str, db: u8, key: &str) -> PreviewResult {
    let mut conn = open_conn(url, db).await?;

    let type_str: String = redis::cmd("TYPE")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("TYPE failed: {}", e))?;
    let key_type = RedisKeyType::from_redis_type(&type_str);

    let ttl: i64 = redis::cmd("TTL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("TTL failed: {}", e))?;

    let memory_bytes: Option<u64> = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg(key)
        .query_async(&mut conn)
        .await
        .ok();

    const PREVIEW_BATCH: usize = 50;
    const STREAM_BATCH: usize = 20;
    const STRING_CAP: u64 = 64 * 1024;

    let (size, value_text, cursor, has_more) = match key_type {
        RedisKeyType::String => {
            let len: u64 = redis::cmd("STRLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let truncated = len > STRING_CAP;
            let val: String = if truncated {
                redis::cmd("GETRANGE")
                    .arg(key)
                    .arg(0i64)
                    .arg((STRING_CAP - 1) as i64)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or_else(|_| "(nil)".to_string())
            } else {
                redis::cmd("GET")
                    .arg(key)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or_else(|_| "(nil)".to_string())
            };
            let display = try_pretty_json(&val);
            let cursor = if truncated {
                PreviewCursor::StringOffset(STRING_CAP)
            } else {
                PreviewCursor::None
            };
            (Some(len), display, cursor, truncated)
        }
        RedisKeyType::Hash => {
            let len: u64 = redis::cmd("HLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let (next_cursor, fields): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
                .arg(key)
                .arg(0)
                .arg("COUNT")
                .arg(PREVIEW_BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, Vec::new()));
            let mut text = format!("Fields: {}\n\n", len);
            for (field, value) in &fields {
                text.push_str(&format!("{}: {}\n", field, value));
            }
            let has_more = next_cursor != 0;
            let cursor = if has_more {
                PreviewCursor::HashCursor(next_cursor)
            } else {
                PreviewCursor::None
            };
            (Some(len), text, cursor, has_more)
        }
        RedisKeyType::List => {
            let len: u64 = redis::cmd("LLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let items: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg((PREVIEW_BATCH - 1) as i64)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let fetched = items.len() as u64;
            let mut text = format!("Length: {}\n\n", len);
            for (i, item) in items.iter().enumerate() {
                text.push_str(&format!("[{}] {}\n", i, item));
            }
            let has_more = fetched < len;
            let cursor = if has_more {
                PreviewCursor::ListOffset(fetched)
            } else {
                PreviewCursor::None
            };
            (Some(len), text, cursor, has_more)
        }
        RedisKeyType::Set => {
            let len: u64 = redis::cmd("SCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let (next_cursor, members): (u64, Vec<String>) = redis::cmd("SSCAN")
                .arg(key)
                .arg(0)
                .arg("COUNT")
                .arg(PREVIEW_BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, Vec::new()));
            let mut text = format!("Members: {}\n\n", len);
            for member in &members {
                text.push_str(&format!("- {}\n", member));
            }
            let has_more = next_cursor != 0;
            let cursor = if has_more {
                PreviewCursor::SetCursor(next_cursor)
            } else {
                PreviewCursor::None
            };
            (Some(len), text, cursor, has_more)
        }
        RedisKeyType::ZSet => {
            let len: u64 = redis::cmd("ZCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg((PREVIEW_BATCH - 1) as i64)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let fetched = items.len() as u64;
            let mut text = format!("Members: {}\n\n", len);
            for (member, score) in &items {
                text.push_str(&format!("{:.2}: {}\n", score, member));
            }
            let has_more = fetched < len;
            let cursor = if has_more {
                PreviewCursor::ZSetOffset(fetched)
            } else {
                PreviewCursor::None
            };
            (Some(len), text, cursor, has_more)
        }
        RedisKeyType::Stream => {
            let len: u64 = redis::cmd("XLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let raw: Vec<redis::Value> = redis::cmd("XRANGE")
                .arg(key)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(STREAM_BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let messages = parse_stream_entries(&raw);
            let mut text = format!("Length: {}\n\n", len);
            for (id, fields) in &messages {
                text.push_str(&format!("{}\n", id));
                for (f, v) in fields {
                    text.push_str(&format!("  {}: {}\n", f, v));
                }
                text.push('\n');
            }
            if messages.is_empty() {
                text.push_str("(empty stream)");
            }
            let has_more = messages.len() == STREAM_BATCH && (messages.len() as u64) < len;
            let cursor = if has_more {
                if let Some((last_id, _)) = messages.last() {
                    PreviewCursor::StreamLastId(last_id.clone())
                } else {
                    PreviewCursor::None
                }
            } else {
                PreviewCursor::None
            };
            (Some(len), text, cursor, has_more)
        }
        RedisKeyType::Unknown => (None, "Unknown key type".to_string(), PreviewCursor::None, false),
    };

    Ok(KeyPreview {
        key: key.to_string(),
        key_type,
        ttl,
        size,
        memory_bytes,
        value_text,
        cursor,
        has_more,
    })
}

pub async fn fetch_preview_more(
    url: &str,
    db: u8,
    key: &str,
    cursor: PreviewCursor,
) -> PreviewMoreResult {
    let mut conn = open_conn(url, db).await?;

    const BATCH: usize = 50;
    const STREAM_BATCH: usize = 20;
    const STRING_CHUNK: u64 = 64 * 1024;

    match cursor {
        PreviewCursor::StringOffset(offset) => {
            let len: u64 = redis::cmd("STRLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let end = (offset + STRING_CHUNK - 1).min(len.saturating_sub(1));
            let val: String = redis::cmd("GETRANGE")
                .arg(key)
                .arg(offset as i64)
                .arg(end as i64)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let next_offset = offset + STRING_CHUNK;
            let has_more = next_offset < len;
            Ok(PreviewMore {
                value_text: val,
                cursor: if has_more {
                    PreviewCursor::StringOffset(next_offset)
                } else {
                    PreviewCursor::None
                },
                has_more,
            })
        }
        PreviewCursor::HashCursor(scan_cursor) => {
            let (next_cursor, fields): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
                .arg(key)
                .arg(scan_cursor)
                .arg("COUNT")
                .arg(BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, Vec::new()));
            let mut text = String::new();
            for (field, value) in &fields {
                text.push_str(&format!("{}: {}\n", field, value));
            }
            let has_more = next_cursor != 0;
            Ok(PreviewMore {
                value_text: text,
                cursor: if has_more {
                    PreviewCursor::HashCursor(next_cursor)
                } else {
                    PreviewCursor::None
                },
                has_more,
            })
        }
        PreviewCursor::ListOffset(offset) => {
            let len: u64 = redis::cmd("LLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let items: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(offset as i64)
                .arg((offset as i64) + (BATCH as i64) - 1)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let mut text = String::new();
            for (i, item) in items.iter().enumerate() {
                text.push_str(&format!("[{}] {}\n", offset as usize + i, item));
            }
            let next_offset = offset + items.len() as u64;
            let has_more = next_offset < len;
            Ok(PreviewMore {
                value_text: text,
                cursor: if has_more {
                    PreviewCursor::ListOffset(next_offset)
                } else {
                    PreviewCursor::None
                },
                has_more,
            })
        }
        PreviewCursor::SetCursor(scan_cursor) => {
            let (next_cursor, members): (u64, Vec<String>) = redis::cmd("SSCAN")
                .arg(key)
                .arg(scan_cursor)
                .arg("COUNT")
                .arg(BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, Vec::new()));
            let mut text = String::new();
            for member in &members {
                text.push_str(&format!("- {}\n", member));
            }
            let has_more = next_cursor != 0;
            Ok(PreviewMore {
                value_text: text,
                cursor: if has_more {
                    PreviewCursor::SetCursor(next_cursor)
                } else {
                    PreviewCursor::None
                },
                has_more,
            })
        }
        PreviewCursor::ZSetOffset(offset) => {
            let len: u64 = redis::cmd("ZCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
                .arg(key)
                .arg(offset as i64)
                .arg((offset as i64) + (BATCH as i64) - 1)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let mut text = String::new();
            for (member, score) in &items {
                text.push_str(&format!("{:.2}: {}\n", score, member));
            }
            let next_offset = offset + items.len() as u64;
            let has_more = next_offset < len;
            Ok(PreviewMore {
                value_text: text,
                cursor: if has_more {
                    PreviewCursor::ZSetOffset(next_offset)
                } else {
                    PreviewCursor::None
                },
                has_more,
            })
        }
        PreviewCursor::StreamLastId(ref last_id) => {
            // Use exclusive start: "(<id>" to skip the already-fetched entry
            let start = format!("({}", last_id);
            let raw: Vec<redis::Value> = redis::cmd("XRANGE")
                .arg(key)
                .arg(&start)
                .arg("+")
                .arg("COUNT")
                .arg(STREAM_BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let messages = parse_stream_entries(&raw);
            let mut text = String::new();
            for (id, fields) in &messages {
                text.push_str(&format!("{}\n", id));
                for (f, v) in fields {
                    text.push_str(&format!("  {}: {}\n", f, v));
                }
                text.push('\n');
            }
            let has_more = messages.len() == STREAM_BATCH;
            let next_cursor = if has_more {
                if let Some((id, _)) = messages.last() {
                    PreviewCursor::StreamLastId(id.clone())
                } else {
                    PreviewCursor::None
                }
            } else {
                PreviewCursor::None
            };
            Ok(PreviewMore {
                value_text: text,
                cursor: next_cursor,
                has_more,
            })
        }
        PreviewCursor::None => Ok(PreviewMore {
            value_text: String::new(),
            cursor: PreviewCursor::None,
            has_more: false,
        }),
    }
}

pub async fn fetch_db_sizes(url: &str) -> DbSizeResult {
    let client = redis::Client::open(url).map_err(|e| format!("Client error: {}", e))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    let info: String = redis::cmd("INFO")
        .arg("keyspace")
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("INFO failed: {}", e))?;

    let mut sizes = Vec::new();
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix("db")
            && let Some((db_str, kv)) = rest.split_once(':')
            && let Ok(db_num) = db_str.parse::<u8>()
            && let Some(keys_part) = kv.split(',').next()
            && let Some(count_str) = keys_part.strip_prefix("keys=")
            && let Ok(count) = count_str.parse::<u64>()
        {
            sizes.push((db_num, count));
        }
    }

    Ok(sizes)
}

// ── Full key data fetch for editor ────────────────────────────────────────────

pub async fn fetch_key_data(url: &str, db: u8, key: &str) -> KeyEditDataResult {
    let mut conn = open_conn(url, db).await?;

    let type_str: String = redis::cmd("TYPE")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("TYPE failed: {}", e))?;
    let key_type = RedisKeyType::from_redis_type(&type_str);

    let ttl: i64 = redis::cmd("TTL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("TTL failed: {}", e))?;

    const COLLECTION_BATCH: usize = 1000;
    const STREAM_BATCH: usize = 100;

    let mut data = KeyEditData {
        key: key.to_string(),
        key_type,
        ttl,
        size: 0,
        string_value: None,
        string_truncated: false,
        hash_fields: None,
        list_items: None,
        set_members: None,
        zset_members: None,
        stream_messages: None,
        collection_cursor: EditorCursor::None,
        collection_has_more: false,
    };

    match key_type {
        RedisKeyType::String => {
            const EDITOR_STRING_CAP: u64 = 1024 * 1024; // 1 MiB
            let len: u64 = redis::cmd("STRLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            let truncated = len > EDITOR_STRING_CAP;
            let val: String = if truncated {
                redis::cmd("GETRANGE")
                    .arg(key)
                    .arg(0i64)
                    .arg((EDITOR_STRING_CAP - 1) as i64)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or_default()
            } else {
                redis::cmd("GET")
                    .arg(key)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or_default()
            };
            data.string_value = Some(val);
            data.string_truncated = truncated;
        }
        RedisKeyType::Hash => {
            let len: u64 = redis::cmd("HLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            let mut fields = Vec::new();
            let mut cursor = 0u64;
            loop {
                let (next, batch): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
                    .arg(key)
                    .arg(cursor)
                    .arg("COUNT")
                    .arg(200)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((0, Vec::new()));
                fields.extend(batch);
                cursor = next;
                if cursor == 0 || fields.len() >= COLLECTION_BATCH {
                    break;
                }
            }
            if cursor != 0 {
                data.collection_cursor = EditorCursor::HashCursor(cursor);
                data.collection_has_more = true;
            }
            data.hash_fields = Some(fields);
        }
        RedisKeyType::List => {
            let len: u64 = redis::cmd("LLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            let items: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg((COLLECTION_BATCH - 1) as i64)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let fetched = items.len() as u64;
            if fetched < len {
                data.collection_cursor = EditorCursor::ListOffset(fetched);
                data.collection_has_more = true;
            }
            data.list_items = Some(items);
        }
        RedisKeyType::Set => {
            let len: u64 = redis::cmd("SCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            let mut members = Vec::new();
            let mut cursor = 0u64;
            loop {
                let (next, batch): (u64, Vec<String>) = redis::cmd("SSCAN")
                    .arg(key)
                    .arg(cursor)
                    .arg("COUNT")
                    .arg(200)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((0, Vec::new()));
                members.extend(batch);
                cursor = next;
                if cursor == 0 || members.len() >= COLLECTION_BATCH {
                    break;
                }
            }
            if cursor != 0 {
                data.collection_cursor = EditorCursor::SetCursor(cursor);
                data.collection_has_more = true;
            }
            data.set_members = Some(members);
        }
        RedisKeyType::ZSet => {
            let len: u64 = redis::cmd("ZCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg((COLLECTION_BATCH - 1) as i64)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let fetched = items.len() as u64;
            if fetched < len {
                data.collection_cursor = EditorCursor::ZSetOffset(fetched);
                data.collection_has_more = true;
            }
            data.zset_members = Some(items);
        }
        RedisKeyType::Stream => {
            let len: u64 = redis::cmd("XLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            let raw: Vec<redis::Value> = redis::cmd("XRANGE")
                .arg(key)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(STREAM_BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let messages = parse_stream_entries(&raw);
            let has_more = messages.len() == STREAM_BATCH && (messages.len() as u64) < len;
            if has_more
                && let Some((last_id, _)) = messages.last() {
                    data.collection_cursor = EditorCursor::StreamLastId(last_id.clone());
                    data.collection_has_more = true;
                }
            data.stream_messages = Some(messages);
        }
        RedisKeyType::Unknown => {}
    }

    Ok(data)
}

pub async fn fetch_key_data_more(
    url: &str,
    db: u8,
    key: &str,
    cursor: EditorCursor,
) -> KeyEditMoreResult {
    let mut conn = open_conn(url, db).await?;

    const COLLECTION_BATCH: usize = 1000;
    const STREAM_BATCH: usize = 100;

    let mut result = KeyEditMore {
        hash_fields: None,
        list_items: None,
        set_members: None,
        zset_members: None,
        stream_messages: None,
        cursor: EditorCursor::None,
        has_more: false,
    };

    match cursor {
        EditorCursor::HashCursor(scan_cursor) => {
            let mut fields = Vec::new();
            let mut cur = scan_cursor;
            loop {
                let (next, batch): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
                    .arg(key)
                    .arg(cur)
                    .arg("COUNT")
                    .arg(200)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((0, Vec::new()));
                fields.extend(batch);
                cur = next;
                if cur == 0 || fields.len() >= COLLECTION_BATCH {
                    break;
                }
            }
            if cur != 0 {
                result.cursor = EditorCursor::HashCursor(cur);
                result.has_more = true;
            }
            result.hash_fields = Some(fields);
        }
        EditorCursor::ListOffset(offset) => {
            let len: u64 = redis::cmd("LLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let items: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(offset as i64)
                .arg((offset as i64) + (COLLECTION_BATCH as i64) - 1)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let next_offset = offset + items.len() as u64;
            if next_offset < len {
                result.cursor = EditorCursor::ListOffset(next_offset);
                result.has_more = true;
            }
            result.list_items = Some(items);
        }
        EditorCursor::SetCursor(scan_cursor) => {
            let mut members = Vec::new();
            let mut cur = scan_cursor;
            loop {
                let (next, batch): (u64, Vec<String>) = redis::cmd("SSCAN")
                    .arg(key)
                    .arg(cur)
                    .arg("COUNT")
                    .arg(200)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((0, Vec::new()));
                members.extend(batch);
                cur = next;
                if cur == 0 || members.len() >= COLLECTION_BATCH {
                    break;
                }
            }
            if cur != 0 {
                result.cursor = EditorCursor::SetCursor(cur);
                result.has_more = true;
            }
            result.set_members = Some(members);
        }
        EditorCursor::ZSetOffset(offset) => {
            let len: u64 = redis::cmd("ZCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
                .arg(key)
                .arg(offset as i64)
                .arg((offset as i64) + (COLLECTION_BATCH as i64) - 1)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let next_offset = offset + items.len() as u64;
            if next_offset < len {
                result.cursor = EditorCursor::ZSetOffset(next_offset);
                result.has_more = true;
            }
            result.zset_members = Some(items);
        }
        EditorCursor::StreamLastId(ref last_id) => {
            let start = format!("({}", last_id);
            let raw: Vec<redis::Value> = redis::cmd("XRANGE")
                .arg(key)
                .arg(&start)
                .arg("+")
                .arg("COUNT")
                .arg(STREAM_BATCH)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let messages = parse_stream_entries(&raw);
            if messages.len() == STREAM_BATCH
                && let Some((id, _)) = messages.last() {
                    result.cursor = EditorCursor::StreamLastId(id.clone());
                    result.has_more = true;
                }
            result.stream_messages = Some(messages);
        }
        EditorCursor::None => {}
    }

    Ok(result)
}

pub async fn search_key_data(
    url: &str,
    db: u8,
    key: &str,
    key_type: RedisKeyType,
    pattern: &str,
) -> KeyEditDataResult {
    let mut conn = open_conn(url, db).await?;

    let ttl: i64 = redis::cmd("TTL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("TTL failed: {}", e))?;

    let glob = format!("*{}*", pattern);

    let mut data = KeyEditData {
        key: key.to_string(),
        key_type,
        ttl,
        size: 0,
        string_value: None,
        string_truncated: false,
        hash_fields: None,
        list_items: None,
        set_members: None,
        zset_members: None,
        stream_messages: None,
        collection_cursor: EditorCursor::None,
        collection_has_more: false,
    };

    match key_type {
        RedisKeyType::Hash => {
            let len: u64 = redis::cmd("HLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            // HSCAN MATCH searches field names
            let mut fields = Vec::new();
            let mut cursor = 0u64;
            loop {
                let (next, batch): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
                    .arg(key)
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&glob)
                    .arg("COUNT")
                    .arg(200)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((0, Vec::new()));
                fields.extend(batch);
                cursor = next;
                if cursor == 0 || fields.len() >= 1000 {
                    break;
                }
            }
            data.hash_fields = Some(fields);
        }
        RedisKeyType::Set => {
            let len: u64 = redis::cmd("SCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            // SSCAN MATCH searches member values
            let mut members = Vec::new();
            let mut cursor = 0u64;
            loop {
                let (next, batch): (u64, Vec<String>) = redis::cmd("SSCAN")
                    .arg(key)
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&glob)
                    .arg("COUNT")
                    .arg(200)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((0, Vec::new()));
                members.extend(batch);
                cursor = next;
                if cursor == 0 || members.len() >= 1000 {
                    break;
                }
            }
            data.set_members = Some(members);
        }
        RedisKeyType::List => {
            let len: u64 = redis::cmd("LLEN")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            // List has no server-side MATCH — load up to 1000 and filter locally
            let items: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg(999i64)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let filtered: Vec<String> = items
                .into_iter()
                .filter(|v| v.contains(pattern))
                .collect();
            data.list_items = Some(filtered);
        }
        RedisKeyType::ZSet => {
            let len: u64 = redis::cmd("ZCARD")
                .arg(key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            data.size = len;
            // ZSet has no server-side MATCH — load up to 1000 and filter locally
            let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg(999i64)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            let filtered: Vec<(String, f64)> = items
                .into_iter()
                .filter(|(v, _)| v.contains(pattern))
                .collect();
            data.zset_members = Some(filtered);
        }
        _ => {} // String/Stream: no collection search
    }

    Ok(data)
}

// ── Write operations ──────────────────────────────────────────────────────────

pub async fn set_string(url: &str, db: u8, key: &str, value: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query_async::<()>(&mut conn)
        .await
        .map_err(|e| format!("SET failed: {}", e))?;
    Ok("OK".to_string())
}

pub async fn hash_set(url: &str, db: u8, key: &str, field: &str, value: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    redis::cmd("HSET")
        .arg(key)
        .arg(field)
        .arg(value)
        .query_async::<()>(&mut conn)
        .await
        .map_err(|e| format!("HSET failed: {}", e))?;
    Ok("OK".to_string())
}

pub async fn hash_delete(url: &str, db: u8, key: &str, field: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let removed: i64 = redis::cmd("HDEL")
        .arg(key)
        .arg(field)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("HDEL failed: {}", e))?;
    Ok(format!("{} field(s) removed", removed))
}

pub async fn list_push(url: &str, db: u8, key: &str, value: &str, left: bool) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let cmd = if left { "LPUSH" } else { "RPUSH" };
    let len: i64 = redis::cmd(cmd)
        .arg(key)
        .arg(value)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("{} failed: {}", cmd, e))?;
    Ok(format!("OK (list length: {})", len))
}

pub async fn list_remove(url: &str, db: u8, key: &str, value: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let removed: i64 = redis::cmd("LREM")
        .arg(key)
        .arg(1)
        .arg(value)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("LREM failed: {}", e))?;
    Ok(format!("{} element(s) removed", removed))
}

pub async fn set_add(url: &str, db: u8, key: &str, member: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let added: i64 = redis::cmd("SADD")
        .arg(key)
        .arg(member)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("SADD failed: {}", e))?;
    Ok(format!("{} member(s) added", added))
}

pub async fn set_remove(url: &str, db: u8, key: &str, member: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let removed: i64 = redis::cmd("SREM")
        .arg(key)
        .arg(member)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("SREM failed: {}", e))?;
    Ok(format!("{} member(s) removed", removed))
}

pub async fn zset_add(url: &str, db: u8, key: &str, member: &str, score: f64) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let added: i64 = redis::cmd("ZADD")
        .arg(key)
        .arg(score)
        .arg(member)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("ZADD failed: {}", e))?;
    Ok(format!("{} member(s) added/updated", added))
}

pub async fn zset_remove(url: &str, db: u8, key: &str, member: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let removed: i64 = redis::cmd("ZREM")
        .arg(key)
        .arg(member)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("ZREM failed: {}", e))?;
    Ok(format!("{} member(s) removed", removed))
}

// ── Key management operations ─────────────────────────────────────────────────

pub async fn delete_key(url: &str, db: u8, key: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    let deleted: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("DEL failed: {}", e))?;
    Ok(format!("{} key(s) deleted", deleted))
}

pub async fn rename_key(url: &str, db: u8, old_key: &str, new_key: &str) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    redis::cmd("RENAME")
        .arg(old_key)
        .arg(new_key)
        .query_async::<()>(&mut conn)
        .await
        .map_err(|e| format!("RENAME failed: {}", e))?;
    Ok("OK".to_string())
}

pub async fn set_ttl(url: &str, db: u8, key: &str, ttl_seconds: i64) -> WriteResult {
    let mut conn = open_conn(url, db).await?;
    if ttl_seconds < 0 {
        // Remove TTL (persist)
        redis::cmd("PERSIST")
            .arg(key)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| format!("PERSIST failed: {}", e))?;
        Ok("TTL removed".to_string())
    } else {
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(ttl_seconds)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| format!("EXPIRE failed: {}", e))?;
        Ok(format!("TTL set to {}s", ttl_seconds))
    }
}

// ── Server info operations ───────────────────────────────────────────────────

pub async fn fetch_server_info(url: &str) -> ServerInfoResult {
    let client = redis::Client::open(url).map_err(|e| format!("Client error: {}", e))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    let info: String = redis::cmd("INFO")
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("INFO failed: {}", e))?;

    let mut sections = Vec::new();
    let mut current_section = InfoSection {
        name: "server".to_string(),
        entries: Vec::new(),
    };

    for line in info.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix("# ") {
            if !current_section.entries.is_empty() {
                sections.push(current_section);
            }
            current_section = InfoSection {
                name: name.to_string(),
                entries: Vec::new(),
            };
        } else if let Some((key, value)) = line.split_once(':') {
            current_section.entries.push((key.to_string(), value.to_string()));
        }
    }
    if !current_section.entries.is_empty() {
        sections.push(current_section);
    }

    Ok(sections)
}

pub async fn fetch_slowlog(url: &str, count: usize) -> SlowLogResult {
    let client = redis::Client::open(url).map_err(|e| format!("Client error: {}", e))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    let raw: Vec<redis::Value> = redis::cmd("SLOWLOG")
        .arg("GET")
        .arg(count)
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("SLOWLOG GET failed: {}", e))?;

    let mut entries = Vec::new();
    for entry in &raw {
        if let redis::Value::Array(parts) = entry {
            let id = match parts.first() {
                Some(redis::Value::Int(n)) => *n,
                _ => 0,
            };
            let timestamp = match parts.get(1) {
                Some(redis::Value::Int(n)) => *n,
                _ => 0,
            };
            let duration_us = match parts.get(2) {
                Some(redis::Value::Int(n)) => *n,
                _ => 0,
            };
            let command = match parts.get(3) {
                Some(redis::Value::Array(cmd_parts)) => {
                    cmd_parts
                        .iter()
                        .map(|v| match v {
                            redis::Value::BulkString(b) => {
                                String::from_utf8_lossy(b).to_string()
                            }
                            redis::Value::Int(n) => n.to_string(),
                            _ => format!("{:?}", v),
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                }
                _ => String::new(),
            };
            let client_addr = match parts.get(4) {
                Some(redis::Value::BulkString(b)) => {
                    String::from_utf8_lossy(b).to_string()
                }
                _ => String::new(),
            };
            let client_name = match parts.get(5) {
                Some(redis::Value::BulkString(b)) => {
                    String::from_utf8_lossy(b).to_string()
                }
                _ => String::new(),
            };
            entries.push(SlowLogEntry {
                id,
                timestamp,
                duration_us,
                command,
                client_addr,
                client_name,
            });
        }
    }

    Ok(entries)
}

pub async fn fetch_client_list(url: &str) -> ClientListResult {
    let client = redis::Client::open(url).map_err(|e| format!("Client error: {}", e))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    let raw: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("CLIENT LIST failed: {}", e))?;

    let mut clients = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut entry = ClientEntry {
            id: String::new(),
            addr: String::new(),
            name: String::new(),
            db: String::new(),
            cmd: String::new(),
            age: String::new(),
            idle: String::new(),
            flags: String::new(),
        };
        for pair in line.split_whitespace() {
            if let Some((key, value)) = pair.split_once('=') {
                match key {
                    "id" => entry.id = value.to_string(),
                    "addr" => entry.addr = value.to_string(),
                    "name" => entry.name = value.to_string(),
                    "db" => entry.db = value.to_string(),
                    "cmd" => entry.cmd = value.to_string(),
                    "age" => entry.age = value.to_string(),
                    "idle" => entry.idle = value.to_string(),
                    "flags" => entry.flags = value.to_string(),
                    _ => {}
                }
            }
        }
        clients.push(entry);
    }

    Ok(clients)
}

// ── Command execution ─────────────────────────────────────────────────────────

pub async fn execute_command(url: &str, db: u8, command_str: &str) -> CommandExecResult {
    let start = Instant::now();
    let parts = parse_redis_command(command_str)?;
    if parts.is_empty() {
        return Err("Empty command".to_string());
    }

    let mut conn = open_conn(url, db).await.map_err(|e| e.to_string())?;

    let mut cmd = redis::cmd(&parts[0]);
    for arg in &parts[1..] {
        cmd.arg(arg.as_str());
    }

    let result: redis::Value = cmd
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("Error: {}", e))?;

    let output = format_redis_value(&result, 0);
    let duration_ms = start.elapsed().as_millis() as u64;

    Ok(CommandResult {
        command: command_str.to_string(),
        output,
        error: None,
        duration_ms,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn try_pretty_json(s: &str) -> String {
    if s.starts_with('{') || s.starts_with('[') {
        serde_json::from_str::<serde_json::Value>(s)
            .ok()
            .and_then(|v| serde_json::to_string_pretty(&v).ok())
            .unwrap_or_else(|| s.to_string())
    } else {
        s.to_string()
    }
}

fn parse_stream_entries(raw: &[redis::Value]) -> Vec<(String, Vec<(String, String)>)> {
    let mut messages = Vec::new();
    for entry in raw {
        if let redis::Value::Array(parts) = entry
            && parts.len() >= 2 {
                let id = match &parts[0] {
                    redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                    _ => continue,
                };
                let fields = if let redis::Value::Array(fv) = &parts[1] {
                    let mut pairs = Vec::new();
                    let mut iter = fv.iter();
                    while let (Some(f), Some(v)) = (iter.next(), iter.next()) {
                        let field = match f {
                            redis::Value::BulkString(b) => {
                                String::from_utf8_lossy(b).to_string()
                            }
                            _ => continue,
                        };
                        let val = match v {
                            redis::Value::BulkString(b) => {
                                String::from_utf8_lossy(b).to_string()
                            }
                            _ => continue,
                        };
                        pairs.push((field, val));
                    }
                    pairs
                } else {
                    Vec::new()
                };
                messages.push((id, fields));
            }
    }
    messages
}

pub(crate) fn parse_redis_command(input: &str) -> Result<Vec<String>, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("Empty command".to_string());
    }

    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escape = false;

    for ch in input.chars() {
        if escape {
            current.push(ch);
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_double_quote => {
                escape = true;
            }
            '\'' if !in_double_quote => {
                in_single_quote = !in_single_quote;
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
            }
            ' ' | '\t' if !in_single_quote && !in_double_quote => {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }

    if in_single_quote || in_double_quote {
        return Err("Unclosed quote".to_string());
    }

    // Uppercase the command name
    if let Some(first) = parts.first_mut() {
        *first = first.to_uppercase();
    }

    Ok(parts)
}

pub(crate) fn format_redis_value(value: &redis::Value, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    match value {
        redis::Value::Nil => format!("{}(nil)", prefix),
        redis::Value::Int(n) => format!("{}(integer) {}", prefix, n),
        redis::Value::BulkString(bytes) => {
            match String::from_utf8(bytes.clone()) {
                Ok(s) => format!("{}\"{}\"", prefix, s),
                Err(_) => format!("{}(binary) {} bytes", prefix, bytes.len()),
            }
        }
        redis::Value::SimpleString(s) => format!("{}{}", prefix, s),
        redis::Value::Okay => format!("{}OK", prefix),
        redis::Value::Array(arr) | redis::Value::Set(arr) => {
            if arr.is_empty() {
                return format!("{}(empty array)", prefix);
            }
            let mut out = String::new();
            for (i, v) in arr.iter().enumerate() {
                let line = format_redis_value(v, indent + 1);
                out.push_str(&format!("{}{}) {}\n", prefix, i + 1, line.trim()));
            }
            out.trim_end().to_string()
        }
        redis::Value::Map(pairs) => {
            if pairs.is_empty() {
                return format!("{}(empty map)", prefix);
            }
            let mut out = String::new();
            for (i, (k, v)) in pairs.iter().enumerate() {
                let k_str = format_redis_value(k, 0);
                let v_str = format_redis_value(v, indent + 1);
                out.push_str(&format!("{}{}) {} => {}\n", prefix, i + 1, k_str.trim(), v_str.trim()));
            }
            out.trim_end().to_string()
        }
        redis::Value::Double(d) => format!("{}(double) {}", prefix, d),
        redis::Value::Boolean(b) => format!("{}(boolean) {}", prefix, b),
        redis::Value::VerbatimString { text, .. } => format!("{}\"{}\"", prefix, text),
        redis::Value::BigNumber(n) => format!("{}(big number) {}", prefix, n),
        redis::Value::ServerError(e) => format!("{}(error) {:?}", prefix, e),
        _ => format!("{}(other) {:?}", prefix, value),
    }
}

pub fn format_ttl(ttl: i64) -> String {
    match ttl {
        -1 => "no expiry".to_string(),
        -2 => "expired".to_string(),
        t if t >= 86400 => format!("{}d", t / 86400),
        t if t >= 3600 => format!("{}h", t / 3600),
        t if t >= 60 => format!("{}m", t / 60),
        t => format!("{}s", t),
    }
}

/// Replace control characters (except \n \r \t) with the Unicode replacement character.
/// Redis values are binary-safe and may contain arbitrary bytes that break terminal rendering.
pub fn sanitize_for_display(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_control() && c != '\n' && c != '\r' && c != '\t' {
                '\u{FFFD}'
            } else {
                c
            }
        })
        .collect()
}

pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;
    match bytes {
        b if b >= GIB => format!("{:.1} GiB", b as f64 / GIB as f64),
        b if b >= MIB => format!("{:.1} MiB", b as f64 / MIB as f64),
        b if b >= KIB => format!("{:.1} KiB", b as f64 / KIB as f64),
        b => format!("{} B", b),
    }
}
