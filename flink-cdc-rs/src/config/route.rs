use regex::Regex;
use serde::{Deserialize, Serialize};

///
/// 增加route的路由的配置
///
#[derive(Debug, Deserialize, Serialize)]
pub struct Route {
    #[serde(rename = "source-table")]
    source: String,
    #[serde(rename = "sink-table")]
    sink: String,
    #[serde(rename = "replace-symbol")]
    replace_symbol: Option<String>,
    description: Option<String>,

    #[serde(skip)]
    source_reg: Option<Regex>,
}

impl Route {
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn sink(&self) -> &str {
        &self.sink
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn source_reg(&mut self) -> Option<&Regex> {
        if self.source_reg.is_none() {
            let source = format!("^{}$", self.source());
            if let Ok(reg) = Regex::new(source.as_str()) {
                self.source_reg = Some(reg);
                return self.source_reg.as_ref();
            } else {
                return None;
            }
        } else {
            return self.source_reg.as_ref();
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Router {
    route: Vec<Route>,
    #[serde(skip)]
    infos: Vec<String>,
}

impl Router {
    pub fn find(&mut self, source: &str) -> Option<&Route> {
        let result = self.route.iter_mut().find(|ele| ele.source().eq(source));
        if result.is_some() {
            return result.map(|ele| &*ele);
        }
        self.route.iter_mut();
        return None;
    }

    pub fn find_reg(&mut self, source: &str) -> Option<&mut Route> {
        self.route.iter_mut().find_map(|ele| {
            let matched = ele
                .source_reg()
                .map(|reg| reg.is_match(source))
                .unwrap_or(false);
            if matched { Some(ele) } else { None }
        })
    }

    pub fn test_life() {
        let mut name = String::from("测试生命中去");
        let first = &mut name;
        let second = &mut name;
    }
}

#[cfg(test)]
mod tests {
    use super::{Route, Router};

    fn route(source: &str, sink: &str) -> Route {
        Route {
            source: source.to_string(),
            sink: sink.to_string(),
            replace_symbol: None,
            description: None,
            source_reg: None,
        }
    }

    #[test]
    fn find_returns_exact_match_only() {
        let router = Router {
            route: vec![route("db.orders", "sink_orders")],
        };

        assert_eq!(
            router.find("db.orders").map(Route::sink),
            Some("sink_orders")
        );
        assert!(router.find("db.orders_001").is_none());
    }

    #[test]
    fn find_reg_prefers_exact_match_before_regex() {
        let mut router = Router {
            route: vec![
                route("db.gsms_msg_ticket", "sink_exact"),
                route("db.gsms_msg_ticket_.*", "sink_regex"),
            ],
        };

        assert_eq!(
            router.find_reg("db.gsms_msg_ticket").map(Route::sink),
            Some("sink_exact")
        );
        assert_eq!(
            router.find_reg("db.gsms_msg_ticket_sms").map(Route::sink),
            Some("sink_regex")
        );
    }

    #[test]
    fn find_reg_uses_full_match_regex() {
        let mut router = Router {
            route: vec![route("db.gsms_msg_ticket", "sink_ticket")],
        };

        assert!(router.find_reg("db.gsms_msg_ticket_sms").is_none());
    }

    #[test]
    fn find_reg_returns_none_for_invalid_regex() {
        let mut router = Router {
            route: vec![route("db.[", "sink_invalid")],
        };

        assert!(router.find_reg("db.any").is_none());
    }

    #[test]
    fn router_deserializes_from_yaml_sequence() {
        let yaml = r#"
- source-table: db.orders_.*
  sink-table: sink_orders
- source-table: db.users
  sink-table: sink_users
"#;
        let mut router: Router = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            router.find_reg("db.orders_001").map(Route::sink),
            Some("sink_orders")
        );
        assert_eq!(
            router.find_reg("db.users").map(Route::sink),
            Some("sink_users")
        );
    }
}
