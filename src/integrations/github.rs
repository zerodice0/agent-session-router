use reqwest::{StatusCode, header};
use serde::{Deserialize, Serialize};

use super::{
    ExternalErrorCode, ExternalIssue, ExternalMutation, IntegrationCheck, IntegrationClient,
    IntegrationClientError, SecretToken, ensure_typed_size, invalid_response, parse_url,
};
use crate::tasks::ExternalProvider;

pub(super) const HOST: &str = "api.github.com";
const API_ROOT: &str = "https://api.github.com";
const WEB_HOST: &str = "github.com";
const API_VERSION: &str = "2026-03-10";
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Repository {
    owner: String,
    name: String,
    namespace: String,
}

impl Repository {
    pub(super) fn parse(value: &str) -> Result<Self, super::IntegrationConfigurationError> {
        if value != value.to_ascii_lowercase() {
            return Err(super::IntegrationConfigurationError);
        }
        let mut segments = value.split('/');
        let owner = segments.next().unwrap_or_default();
        let name = segments.next().unwrap_or_default();
        if segments.next().is_some()
            || !valid_segment(owner, 39, |value| {
                value.is_ascii_alphanumeric() || value == b'-'
            })
            || !valid_segment(name, 100, |value| {
                value.is_ascii_alphanumeric() || matches!(value, b'.' | b'_' | b'-')
            })
            || matches!(name, "." | "..")
        {
            return Err(super::IntegrationConfigurationError);
        }
        Ok(Self {
            owner: owner.to_owned(),
            name: name.to_owned(),
            namespace: value.to_owned(),
        })
    }

    pub(super) fn as_str(&self) -> &str {
        &self.namespace
    }
}

pub(super) async fn check(
    client: &IntegrationClient,
    token: &SecretToken,
    repository: &Repository,
) -> Result<IntegrationCheck, IntegrationClientError> {
    let endpoint = format!("{API_ROOT}/repos/{}", repository.namespace);
    let bytes = client
        .send(
            request(client, token, reqwest::Method::GET, &endpoint),
            StatusCode::OK,
            false,
        )
        .await?;
    let response: RepositoryResponse = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    if !response.full_name.eq_ignore_ascii_case(repository.as_str())
        || !response.has_issues
        || !valid_repository_url(&response.html_url, repository)
    {
        return Err(invalid_response(false, ExternalErrorCode::ScopeMismatch));
    }
    ensure_typed_size([
        response.full_name.len(),
        response.html_url.len(),
        repository.as_str().len(),
    ])
    .map_err(|code| invalid_response(false, code))?;
    Ok(IntegrationCheck {
        provider: ExternalProvider::Github,
        target: repository.as_str().to_owned(),
        url: Some(response.html_url),
    })
}

pub(super) async fn get_issue(
    client: &IntegrationClient,
    token: &SecretToken,
    repository: &Repository,
    external_id: &str,
) -> Result<ExternalIssue, IntegrationClientError> {
    let number = parse_issue_number(external_id)?;
    get_issue_number(client, token, repository, number).await
}

async fn get_issue_number(
    client: &IntegrationClient,
    token: &SecretToken,
    repository: &Repository,
    number: i64,
) -> Result<ExternalIssue, IntegrationClientError> {
    let endpoint = issue_api_url(repository, number);
    let bytes = client
        .send(
            request(client, token, reqwest::Method::GET, &endpoint),
            StatusCode::OK,
            false,
        )
        .await?;
    let response: IssueResponse = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    validate_issue(response, repository, Some(number), false)
}

pub(super) async fn create_issue(
    client: &IntegrationClient,
    token: &SecretToken,
    repository: &Repository,
    title: &str,
    body: &str,
) -> Result<ExternalMutation, IntegrationClientError> {
    let endpoint = format!("{API_ROOT}/repos/{}/issues", repository.namespace);
    let payload = CreateIssueRequest { title, body };
    let bytes = client
        .send(
            request(client, token, reqwest::Method::POST, &endpoint).json(&payload),
            StatusCode::CREATED,
            true,
        )
        .await?;
    let response: IssueResponse = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(true, ExternalErrorCode::ApiError))?;
    let issue = validate_issue(response, repository, None, true)?;
    Ok(ExternalMutation {
        external_id: issue.external_id,
        url: issue.url,
    })
}

pub(super) async fn create_comment(
    client: &IntegrationClient,
    token: &SecretToken,
    repository: &Repository,
    external_id: &str,
    body: &str,
) -> Result<ExternalMutation, IntegrationClientError> {
    let number = parse_issue_number(external_id)?;
    get_issue_number(client, token, repository, number).await?;
    let endpoint = format!("{}/comments", issue_api_url(repository, number));
    let bytes = client
        .send(
            request(client, token, reqwest::Method::POST, &endpoint)
                .json(&CreateCommentRequest { body }),
            StatusCode::CREATED,
            true,
        )
        .await?;
    let response: CommentResponse = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(true, ExternalErrorCode::ApiError))?;
    if response.id <= 0
        || !valid_issue_api_url(&response.issue_url, repository, number)
        || !valid_comment_url(&response.html_url, repository, number, response.id)
    {
        return Err(invalid_response(true, ExternalErrorCode::ScopeMismatch));
    }
    ensure_typed_size([response.html_url.len(), response.issue_url.len()])
        .map_err(|code| invalid_response(true, code))?;
    Ok(ExternalMutation {
        external_id: response.id.to_string(),
        url: response.html_url,
    })
}

pub(super) async fn verify_comment_marker(
    client: &IntegrationClient,
    token: &SecretToken,
    repository: &Repository,
    issue_id: &str,
    comment_id: &str,
    marker: &str,
) -> Result<ExternalMutation, IntegrationClientError> {
    let issue_number = parse_issue_number(issue_id)?;
    let comment_number = parse_issue_number(comment_id)?;
    let endpoint = format!(
        "{API_ROOT}/repos/{}/issues/comments/{comment_number}",
        repository.namespace
    );
    let bytes = client
        .send(
            request(client, token, reqwest::Method::GET, &endpoint),
            StatusCode::OK,
            false,
        )
        .await?;
    let response: CommentResponse = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    if response.id != comment_number
        || !valid_issue_api_url(&response.issue_url, repository, issue_number)
        || !valid_comment_url(&response.html_url, repository, issue_number, comment_number)
        || !response
            .body
            .as_deref()
            .is_some_and(|body| body.contains(marker))
    {
        return Err(invalid_response(false, ExternalErrorCode::ScopeMismatch));
    }
    ensure_typed_size([
        response.html_url.len(),
        response.issue_url.len(),
        response.body.as_deref().map_or(0, str::len),
    ])
    .map_err(|code| invalid_response(false, code))?;
    Ok(ExternalMutation {
        external_id: response.id.to_string(),
        url: response.html_url,
    })
}

fn request(
    client: &IntegrationClient,
    token: &SecretToken,
    method: reqwest::Method,
    endpoint: &str,
) -> reqwest::RequestBuilder {
    client
        .http
        .request(method, endpoint)
        .header(header::AUTHORIZATION, format!("Bearer {}", token.expose()))
        .header(header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", API_VERSION)
        .header(header::USER_AGENT, "agent-session-router")
}

fn validate_issue(
    response: IssueResponse,
    repository: &Repository,
    expected_number: Option<i64>,
    mutation: bool,
) -> Result<ExternalIssue, IntegrationClientError> {
    let valid_state = matches!(response.state.as_str(), "open" | "closed");
    if response.id <= 0
        || response.number <= 0
        || response.number > MAX_SAFE_INTEGER
        || expected_number.is_some_and(|number| number != response.number)
        || response.pull_request.is_some()
        || !valid_state
        || !valid_issue_url(&response.html_url, repository, response.number)
        || !valid_issue_api_url(&response.url, repository, response.number)
    {
        return Err(invalid_response(mutation, ExternalErrorCode::ScopeMismatch));
    }
    let description = response.body.unwrap_or_default();
    ensure_typed_size([
        response.title.len(),
        description.len(),
        response.state.len(),
        response.html_url.len(),
        response.url.len(),
    ])
    .map_err(|code| invalid_response(mutation, code))?;
    Ok(ExternalIssue {
        external_id: response.number.to_string(),
        url: response.html_url,
        title: response.title,
        description,
        state: response.state,
    })
}

fn parse_issue_number(value: &str) -> Result<i64, IntegrationClientError> {
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|value| value.is_ascii_digit())
    {
        return Err(IntegrationClientError::Failed(
            ExternalErrorCode::InvalidRequest,
        ));
    }
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0 && *value <= MAX_SAFE_INTEGER)
        .ok_or(IntegrationClientError::Failed(
            ExternalErrorCode::InvalidRequest,
        ))
}

fn issue_api_url(repository: &Repository, number: i64) -> String {
    format!("{API_ROOT}/repos/{}/issues/{number}", repository.namespace)
}

fn valid_repository_url(value: &str, repository: &Repository) -> bool {
    valid_url_segments(
        value,
        WEB_HOST,
        &[repository.owner.as_str(), repository.name.as_str()],
        None,
    )
}

fn valid_issue_url(value: &str, repository: &Repository, number: i64) -> bool {
    let number = number.to_string();
    valid_url_segments(
        value,
        WEB_HOST,
        &[
            repository.owner.as_str(),
            repository.name.as_str(),
            "issues",
            &number,
        ],
        None,
    )
}

fn valid_comment_url(value: &str, repository: &Repository, number: i64, id: i64) -> bool {
    let number = number.to_string();
    let fragment = format!("issuecomment-{id}");
    valid_url_segments(
        value,
        WEB_HOST,
        &[
            repository.owner.as_str(),
            repository.name.as_str(),
            "issues",
            &number,
        ],
        Some(&fragment),
    )
}

fn valid_issue_api_url(value: &str, repository: &Repository, number: i64) -> bool {
    let number = number.to_string();
    valid_url_segments(
        value,
        HOST,
        &[
            "repos",
            repository.owner.as_str(),
            repository.name.as_str(),
            "issues",
            &number,
        ],
        None,
    )
}

fn valid_url_segments(value: &str, host: &str, expected: &[&str], fragment: Option<&str>) -> bool {
    let Ok(url) = parse_url(value) else {
        return false;
    };
    if url.scheme() != "https"
        || url.host_str() != Some(host)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment() != fragment
    {
        return false;
    }
    let Some(actual) = url.path_segments() else {
        return false;
    };
    actual
        .zip(expected.iter().copied())
        .all(|(actual, expected)| actual.eq_ignore_ascii_case(expected))
        && url
            .path_segments()
            .is_some_and(|segments| segments.count() == expected.len())
}

fn valid_segment(value: &str, maximum: usize, allowed: impl Fn(u8) -> bool) -> bool {
    !value.is_empty() && value.len() <= maximum && value.is_ascii() && value.bytes().all(allowed)
}

#[derive(Deserialize)]
struct RepositoryResponse {
    full_name: String,
    html_url: String,
    has_issues: bool,
}

#[derive(Deserialize)]
struct IssueResponse {
    id: i64,
    number: i64,
    html_url: String,
    url: String,
    title: String,
    body: Option<String>,
    state: String,
    #[serde(default)]
    pull_request: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct CommentResponse {
    id: i64,
    html_url: String,
    issue_url: String,
    body: Option<String>,
}

#[derive(Serialize)]
struct CreateIssueRequest<'a> {
    title: &'a str,
    body: &'a str,
}

#[derive(Serialize)]
struct CreateCommentRequest<'a> {
    body: &'a str,
}
