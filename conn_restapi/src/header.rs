use std::collections::HashMap;

pub struct HttpHeaderBuilder {
    map : HashMap<String,String>
}

impl HttpHeaderBuilder {
    pub fn new() -> Self {
        HttpHeaderBuilder{
            map : HashMap::new()
        }
    }

    pub fn authorization(mut self, auth_type : &'_ str, token : &'_ str) -> Self {
        self.map.insert("Authorization".to_string(), format!("{} {}", auth_type, token));
        self
    }

    pub fn content_type(mut self, content_type: &'_ str) -> Self {
        self.map.insert("Content-Type".to_string(), content_type.to_string());
        self
    }

    pub fn accept(mut self, accept: &'_ str) -> Self {
        self.map.insert("Accept".to_string(), accept.to_string());
        self
    }

    pub fn user_agent(mut self, user_agent: &'_ str) -> Self {
        self.map.insert("User-Agent".to_string(), user_agent.to_string());
        self
    }

    pub fn cache_control(mut self, cache_control: &'_ str) -> Self {
        self.map.insert("Cache-Control".to_string(), cache_control.to_string());
        self
    }

    pub fn origin(mut self, origin: &'_ str) -> Self {
        self.map.insert("Origin".to_string(), origin.to_string());
        self
    }

    pub fn referer(mut self, referer: &'_ str) -> Self {
        self.map.insert("Referer".to_string(), referer.to_string());
        self
    }

    pub fn custom_header(mut self, key: &'_ str, value: &'_ str) -> Self {
        self.map.insert(key.to_string(), value.to_string());
        self
    }

    pub fn build_map(self) -> HashMap<String, String> {
        self.map
    }

    pub fn build_string(self) -> String {
        let mut s = String::new();
        for (k, v) in self.map {
            s.push_str(format!("{} :{}\n", k, v).as_str());
        }

        s
    }
}


