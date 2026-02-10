use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> tracing::result::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    tracing::info!("Starting source-sink application");

    match args.name {
        Some(name) => println!("Hello, {}!", name),
        None => println!("Hello, World!"),
    }

    Ok(())
}
