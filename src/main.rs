use clap::Parser;
use flowlite::cli::Cli;


#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e:?}");
        std::process::exit(1);
    }
}


async fn run() -> anyhow::Result<()> {

    let cli = Cli::parse();
    cli.run().await?;

    Ok(())
}