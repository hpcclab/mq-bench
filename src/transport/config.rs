use super::{ConnectOptions, Engine};

pub fn parse_engine(s: &str) -> Option<Engine> {
    match s.to_lowercase().as_str() {
        "zenoh" => Some(Engine::Zenoh),
        "tcp" => Some(Engine::Tcp),
        "redis" => Some(Engine::Redis),
        "mqtt" => Some(Engine::Mqtt),
    "nats" => Some(Engine::Nats),
        _ => None,
    }
}

pub fn parse_connect_kv(pairs: &[String]) -> ConnectOptions {
    let mut opts = ConnectOptions::default();
    for p in pairs {
        if let Some((k, v)) = p.split_once('=') {
            opts.params.insert(k.to_string(), v.to_string());
        }
    }
    opts
}
