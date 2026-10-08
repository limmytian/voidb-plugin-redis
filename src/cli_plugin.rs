use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::VoidbError;

use crate::config::RedisConfig;
use crate::redis_ops;
use crate::service::RedisService;

pub struct RedisCliPlugin;

pub fn create_redis_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(RedisCliPlugin)
}

#[async_trait]
impl CliPlugin for RedisCliPlugin {
    fn plugin_id(&self) -> &str {
        "redis"
    }

    fn name(&self) -> &str {
        "Redis"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .required(true)
            .help("Connection name");

        let db_arg = Arg::new("db")
            .short('d')
            .long("db")
            .value_parser(clap::value_parser!(u8))
            .help("Database number (overrides connection config)");

        vec![
            Command::new("keys")
                .about("List keys matching a pattern")
                .arg(conn_arg.clone())
                .arg(db_arg.clone())
                .arg(
                    Arg::new("pattern")
                        .default_value("*")
                        .help("Key pattern (glob-style)"),
                )
                .arg(
                    Arg::new("limit")
                        .short('n')
                        .long("limit")
                        .default_value("100")
                        .help("Max keys to return"),
                ),
            Command::new("get")
                .about("Get key value")
                .arg(conn_arg.clone())
                .arg(db_arg.clone())
                .arg(Arg::new("key").required(true).help("Key name")),
            Command::new("set")
                .about("Set string value")
                .arg(conn_arg.clone())
                .arg(db_arg.clone())
                .arg(Arg::new("key").required(true).help("Key name"))
                .arg(Arg::new("value").required(true).help("Value")),
            Command::new("del")
                .about("Delete a key")
                .arg(conn_arg.clone())
                .arg(db_arg.clone())
                .arg(Arg::new("key").required(true).help("Key name")),
            Command::new("ttl")
                .about("Get or set key TTL")
                .arg(conn_arg.clone())
                .arg(db_arg.clone())
                .arg(Arg::new("key").required(true).help("Key name"))
                .arg(
                    Arg::new("seconds")
                        .value_parser(clap::value_parser!(i64))
                        .help("TTL in seconds (omit to show current, -1 to persist)"),
                ),
            Command::new("type")
                .about("Get key type")
                .arg(conn_arg.clone())
                .arg(db_arg.clone())
                .arg(Arg::new("key").required(true).help("Key name")),
            Command::new("info")
                .about("Show server info")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("section")
                        .help("Info section (server, memory, clients, stats, etc.)"),
                ),
            Command::new("exec")
                .about("Execute a raw Redis command")
                .arg(conn_arg)
                .arg(db_arg)
                .arg(
                    Arg::new("command")
                        .required(true)
                        .trailing_var_arg(true)
                        .num_args(1..)
                        .help("Redis command"),
                ),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "keys" => self.handle_keys(matches, ctx).await,
            "get" => self.handle_get(matches, ctx).await,
            "set" => self.handle_set(matches, ctx).await,
            "del" => self.handle_del(matches, ctx).await,
            "ttl" => self.handle_ttl(matches, ctx).await,
            "type" => self.handle_type(matches, ctx).await,
            "info" => self.handle_info(matches, ctx).await,
            "exec" => self.handle_exec(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl RedisCliPlugin {
    fn parse_config(
        conn_name: &str,
        db_override: Option<u8>,
        ctx: &CliContext,
    ) -> Result<RedisConfig, VoidbError> {
        let config = ctx.find_connection(conn_name).ok_or_else(|| {
            VoidbError::Plugin(format!("Connection '{}' not found", conn_name))
        })?;

        if config.effective_plugin_id() != "redis" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not a Redis connection (plugin: {})",
                conn_name,
                config.effective_plugin_id()
            )));
        }

        let mut redis_config: RedisConfig = config
            .plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
            .and_then(|pc| {
                serde_json::from_value(pc.clone())
                    .map_err(|e| VoidbError::Connection(format!("Invalid Redis config: {}", e)))
            })?;

        if let Some(db) = db_override {
            redis_config.db = db;
        }

        Ok(redis_config)
    }

    fn get_service(
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<RedisService, VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let db_override = matches.get_one::<u8>("db").copied();
        let config = Self::parse_config(conn_name, db_override, ctx)?;
        Ok(RedisService::new_direct(config))
    }

    fn get_service_no_db(
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<RedisService, VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let config = Self::parse_config(conn_name, None, ctx)?;
        Ok(RedisService::new_direct(config))
    }

    async fn handle_keys(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::get_service(matches, ctx)?;
        let pattern = matches.get_one::<String>("pattern").unwrap();
        let limit: usize = matches
            .get_one::<String>("limit")
            .unwrap()
            .parse()
            .map_err(|_| VoidbError::Plugin("Invalid limit".to_string()))?;

        let mut all_keys = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (keys, next_cursor) = svc
                .scan_keys(cursor, Some(pattern))
                .await
                .map_err(VoidbError::Plugin)?;

            all_keys.extend(keys);
            cursor = next_cursor;

            if cursor == 0 || all_keys.len() >= limit {
                break;
            }
        }

        all_keys.truncate(limit);

        for key in &all_keys {
            let ttl_str = if key.ttl < 0 {
                String::new()
            } else {
                format!(" (TTL: {}s)", key.ttl)
            };
            println!("{:<50} {:<8}{}", key.key, key.key_type.label(), ttl_str);
        }
        eprintln!("({} keys)", all_keys.len());
        Ok(())
    }

    async fn handle_get(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::get_service(matches, ctx)?;
        let key = matches.get_one::<String>("key").unwrap();

        let data = svc
            .fetch_key_data(key)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("Key:  {}", data.key);
        println!("Type: {}", data.key_type.label());
        println!("TTL:  {}", redis_ops::format_ttl(data.ttl));
        if data.size > 0 {
            println!("Size: {} bytes", data.size);
        }
        println!();

        if let Some(val) = &data.string_value {
            println!("{}", val);
        } else if let Some(fields) = &data.hash_fields {
            for (field, value) in fields {
                println!("{}: {}", field, value);
            }
        } else if let Some(items) = &data.list_items {
            for (i, item) in items.iter().enumerate() {
                println!("[{}] {}", i, item);
            }
        } else if let Some(members) = &data.set_members {
            for member in members {
                println!("{}", member);
            }
        } else if let Some(members) = &data.zset_members {
            for (member, score) in members {
                println!("{}: {}", member, score);
            }
        } else if let Some(messages) = &data.stream_messages {
            for (id, fields) in messages {
                print!("{}", id);
                for (k, v) in fields {
                    print!(" {}={}", k, v);
                }
                println!();
            }
        }

        Ok(())
    }

    async fn handle_set(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::get_service(matches, ctx)?;
        let key = matches.get_one::<String>("key").unwrap();
        let value = matches.get_one::<String>("value").unwrap();

        svc.set_string(key, value)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("OK");
        Ok(())
    }

    async fn handle_del(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::get_service(matches, ctx)?;
        let key = matches.get_one::<String>("key").unwrap();

        svc.delete_key(key)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("Deleted: {}", key);
        Ok(())
    }

    async fn handle_ttl(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::get_service(matches, ctx)?;
        let key = matches.get_one::<String>("key").unwrap();

        if let Some(&seconds) = matches.get_one::<i64>("seconds") {
            svc.set_ttl(key, seconds)
                .await
                .map_err(VoidbError::Plugin)?;
            if seconds < 0 {
                println!("Persisted: {}", key);
            } else {
                println!("TTL set to {}s: {}", seconds, key);
            }
        } else {
            let preview = svc
                .fetch_key_preview(key)
                .await
                .map_err(VoidbError::Plugin)?;
            println!("{}", redis_ops::format_ttl(preview.ttl));
        }

        Ok(())
    }

    async fn handle_type(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::get_service(matches, ctx)?;
        let key = matches.get_one::<String>("key").unwrap();

        let preview = svc
            .fetch_key_preview(key)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{}", preview.key_type.label());
        Ok(())
    }

    async fn handle_info(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let section = matches.get_one::<String>("section");
        let svc = Self::get_service_no_db(matches, ctx)?;

        let sections = svc
            .fetch_server_info()
            .await
            .map_err(VoidbError::Plugin)?;

        for s in &sections {
            if let Some(filter) = section
                && !s.name.eq_ignore_ascii_case(filter)
            {
                continue;
            }
            println!("# {}", s.name);
            for (key, value) in &s.entries {
                println!("{}:{}", key, value);
            }
            println!();
        }
        Ok(())
    }

    async fn handle_exec(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::get_service(matches, ctx)?;
        let parts: Vec<&String> = matches.get_many::<String>("command").unwrap().collect();
        let command_str = parts.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(" ");

        let result = svc
            .execute_command(&command_str)
            .await
            .map_err(VoidbError::Plugin)?;

        if let Some(err) = &result.error {
            eprintln!("(error) {}", err);
        } else {
            print!("{}", result.output);
        }
        eprintln!("({:.2}ms)", result.duration_ms as f64);
        Ok(())
    }
}
