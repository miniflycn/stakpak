use super::types::KimiConfig;
use crate::error::{Error, Result};
use crate::provider::Provider;
use crate::providers::openai::convert::{from_openai_response, to_openai_request};
use crate::providers::openai::stream::create_completions_stream;
use crate::providers::openai::types::{ChatCompletionRequest, ChatCompletionResponse, ChatMessage};
use crate::providers::tls::create_platform_tls_client;
use crate::types::{GenerateRequest, GenerateResponse, GenerateStream, Headers, Model};
use async_trait::async_trait;
use reqwest::Client;
use reqwest_eventsource::EventSource;

pub struct KimiProvider {
    config: KimiConfig,
    client: Client,
}

impl KimiProvider {
    pub fn new(config: KimiConfig) -> Result<Self> {
        if config.api_key.is_empty() {
            return Err(Error::MissingApiKey("kimi".to_string()));
        }
        let client = create_platform_tls_client()?;
        Ok(Self { config, client })
    }
}

fn to_kimi_request(request: &GenerateRequest, stream: bool) -> ChatCompletionRequest {
    let mut kimi_req = to_openai_request(request, stream);
    add_reasoning_content_to_assistant_tool_calls(&mut kimi_req.messages);
    kimi_req
}

fn add_reasoning_content_to_assistant_tool_calls(messages: &mut [ChatMessage]) {
    for message in messages {
        if message.role == "assistant"
            && message
                .tool_calls
                .as_ref()
                .is_some_and(|tool_calls| !tool_calls.is_empty())
            && message.reasoning_content.is_none()
        {
            message.reasoning_content = Some(String::new());
        }
    }
}

#[async_trait]
impl Provider for KimiProvider {
    fn provider_id(&self) -> &str {
        "kimi"
    }

    fn build_headers(&self, custom_headers: Option<&Headers>) -> Headers {
        let mut headers = Headers::new();

        headers.insert("Authorization", format!("Bearer {}", self.config.api_key));
        headers.insert("Content-Type", "application/json");

        if let Some(custom) = custom_headers {
            headers.merge_with(custom);
        }

        headers.insert("User-Agent", self.config.user_agent.clone());

        headers
    }

    async fn generate(&self, request: GenerateRequest) -> Result<GenerateResponse> {
        let url = format!("{}/chat/completions", self.config.base_url);
        let headers = self.build_headers(request.options.headers.as_ref());

        let openai_req = to_kimi_request(&request, false);

        let response = self
            .client
            .post(&url)
            .headers(headers.to_reqwest_headers())
            .json(&openai_req)
            .send()
            .await
            .map_err(|e| Error::provider_error(format!("Kimi request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(Error::provider_error(format!(
                "Kimi returned error {}: {}",
                status, text
            )));
        }

        let openai_resp: ChatCompletionResponse = response.json().await?;
        from_openai_response(openai_resp)
    }

    async fn stream(&self, request: GenerateRequest) -> Result<GenerateStream> {
        let url = format!("{}/chat/completions", self.config.base_url);
        let headers = self.build_headers(request.options.headers.as_ref());

        let openai_req = to_kimi_request(&request, true);

        let request_builder = self
            .client
            .post(&url)
            .headers(headers.to_reqwest_headers())
            .json(&openai_req);

        let event_source = EventSource::new(request_builder)
            .map_err(|e| Error::provider_error(format!("Failed to create event source: {}", e)))?;

        create_completions_stream(event_source).await
    }

    async fn list_models(&self) -> Result<Vec<Model>> {
        let mut models =
            crate::registry::models_dev::load_models_for_provider("kimi").unwrap_or_default();
        if models.is_empty() {
            models.push(Model::custom("K2.6", "kimi"));
        }
        Ok(models)
    }

    async fn get_model(&self, id: &str) -> Result<Option<Model>> {
        Ok(self.list_models().await?.into_iter().find(|m| m.id == id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ContentPart, Message, MessageContent, Role};
    use serde_json::json;

    #[test]
    fn build_headers_adds_required_user_agent() {
        let provider = KimiProvider::new(KimiConfig::new("test-key")).expect("provider");
        let headers = provider.build_headers(None);

        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer test-key".to_string())
        );
        assert_eq!(
            headers.get("User-Agent"),
            Some(&"KimiCLI/1.1.1".to_string())
        );
    }

    #[test]
    fn build_headers_keeps_required_user_agent_when_custom_headers_are_provided() {
        let provider = KimiProvider::new(KimiConfig::new("test-key")).expect("provider");
        let custom = Headers::from([("User-Agent", "OtherClient/0.1")]);

        let headers = provider.build_headers(Some(&custom));

        assert_eq!(
            headers.get("User-Agent"),
            Some(&"KimiCLI/1.1.1".to_string())
        );
    }

    #[test]
    fn kimi_request_adds_reasoning_content_to_assistant_tool_calls() {
        let request = GenerateRequest::new(
            Model::custom("K2.6", "kimi"),
            vec![
                Message::new(Role::User, "check the host"),
                Message {
                    role: Role::Assistant,
                    content: MessageContent::Parts(vec![ContentPart::tool_call(
                        "call_1",
                        "run_command",
                        json!({"cmd": "uptime"}),
                    )]),
                    name: None,
                    provider_options: None,
                },
            ],
        );

        let kimi_req = to_kimi_request(&request, false);
        let assistant = kimi_req
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .expect("assistant message");

        assert_eq!(assistant.reasoning_content, Some(String::new()));
        assert!(assistant.tool_calls.is_some());
    }
}
