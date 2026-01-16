use clap::Parser;

#[derive(Parser, Debug)]
#[command(author,version,about,long_about = None)]
pub struct Args {
    #[arg(long, short)]
    file: String,
}

impl Args {
    pub fn file(&self) -> &str {
        return self.file.as_str();
    }
}

impl ToString for Args {
    fn to_string(&self) -> String {
        return format!("flink_cdc: {}", self.file);
    }
}
