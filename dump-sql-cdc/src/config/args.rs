use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Arguments {
    #[arg(long, short)]
    cdc_file: String,
}

impl Arguments {
    pub fn cdc_file(&self) -> &str {
        &self.cdc_file
    }
}

impl ToString for Arguments {
    fn to_string(&self) -> String {
        format!("cdc_file: {}", self.cdc_file)
    }
}
