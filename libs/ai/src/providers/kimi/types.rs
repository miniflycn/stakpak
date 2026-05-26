#[derive(Debug, Clone)]
pub struct KimiConfig {
    pub api_key: String,
    pub base_url: String,
    pub user_agent: String,
}

impl KimiConfig {
    pub const DEFAULT_BASE_URL: &'static str = "https://api.kimi.com/coding/v1";
    pub const DEFAULT_USER_AGENT: &'static str = "KimiCLI/1.1.1";

    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: Self::DEFAULT_BASE_URL.to_string(),
            user_agent: Self::DEFAULT_USER_AGENT.to_string(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }
}

impl Default for KimiConfig {
    fn default() -> Self {
        Self::new(std::env::var("KIMI_API_KEY").unwrap_or_else(|_| String::new()))
    }
}
