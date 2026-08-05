use std::process::ExitCode;

use clap::Parser;
use webhook_multiplexer::cli::Cli;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = cli.initialize_logging() {
        eprintln!("error: {error:#}");
        return ExitCode::FAILURE;
    }
    match cli.run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}
