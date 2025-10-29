// Tracing setup
use anyhow::Result;

pub fn init(level: &str) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(level)
        .with_ansi(false)
        .init();
    Ok(())
}
