//! Pipeline configuration
//!
//! Defines configuration for data processing pipelines.

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Pipeline {
    pub name: String,

    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
}

fn default_parallelism() -> usize {
    1
}

impl Pipeline {
    pub fn new(name: &str, parallelism: usize) -> Self {
        Self {
            name: name.to_string(),
            parallelism,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn parallelism(&self) -> usize {
        self.parallelism
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_pipeline() {
        let yaml = r#"
name: Test Pipeline
parallelism: 4
"#;

        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(pipeline.name(), "Test Pipeline");
        assert_eq!(pipeline.parallelism(), 4);
    }

    #[test]
    fn test_pipeline_with_defaults() {
        let yaml = r#"
name: Simple Pipeline
"#;

        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(pipeline.name(), "Simple Pipeline");
        assert_eq!(pipeline.parallelism(), 1);
    }

    #[test]
    fn test_serialize_pipeline() {
        let pipeline = Pipeline::new("Test Pipeline", 2);
        let yaml = serde_yaml::to_string(&pipeline).unwrap();
        println!("{}", yaml);
        assert!(yaml.contains("name: Test Pipeline"));
        assert!(yaml.contains("parallelism: 2"));
    }

    #[test]
    fn test_create_pipeline() {
        let pipeline = Pipeline::new("My Pipeline", 4);
        assert_eq!(pipeline.name(), "My Pipeline");
        assert_eq!(pipeline.parallelism(), 4);
    }
}
