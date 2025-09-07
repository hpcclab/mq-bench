use crate::metrics::stats::StatsSnapshot;
use anyhow::Result;
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};

pub enum OutputWriter {
    Csv(BufWriter<tokio::fs::File>),
    Stdout,
}

impl OutputWriter {
    pub async fn new_csv(path: String) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).await.ok();
            }
        }
        let file = File::create(&path).await?;
        let mut writer = BufWriter::new(file);
        
        // Write CSV header
    writer.write_all(StatsSnapshot::csv_header().as_bytes()).await?;
    writer.write_all(b"\n").await?;
        
        println!("Writing CSV output to: {}", path);
        Ok(Self::Csv(writer))
    }
    
    pub fn new_stdout() -> Self {
        println!("Writing output to stdout");
        Self::Stdout
    }
    
    pub async fn write_snapshot(&mut self, snapshot: &StatsSnapshot) -> Result<()> {
        match self {
            Self::Csv(writer) => {
                writer.write_all(snapshot.to_csv_row().as_bytes()).await?;
                writer.write_all(b"\n").await?;
                // Flush so external tail/readers see progress promptly
                writer.flush().await?;
            }
            Self::Stdout => {
                println!("{}", snapshot.to_csv_row());
            }
        }
        Ok(())
    }
}
