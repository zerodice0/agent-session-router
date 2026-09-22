use reqwest::{StatusCode, header};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ExternalErrorCode, ExternalIssue, ExternalMutation, IntegrationCheck, IntegrationClient,
    IntegrationClientError, SecretToken, ensure_typed_size, invalid_response, parse_url,
};
use crate::tasks::ExternalProvider;

pub(super) const HOST: &str = "api.linear.app";
const ENDPOINT: &str = "https://api.linear.app/graphql";
const CHECK_QUERY: &str =
    "query($team:String!,$project:String!){team(id:$team){id} project(id:$project){id}}";
const ISSUE_QUERY: &str = "query($id:String!){issue(id:$id){id identifier url title description team{id} project{id} state{name}}}";
const COMMENT_QUERY: &str =
    "query($id:String!){comment(id:$id){id url body issue{id team{id} project{id}}}}";
const CREATE_ISSUE_MUTATION: &str = "mutation($input:IssueCreateInput!){issueCreate(input:$input){success issue{id identifier url team{id} project{id}}}}";
const CREATE_COMMENT_MUTATION: &str = "mutation($input:CommentCreateInput!){commentCreate(input:$input){success comment{id url issue{id team{id} project{id}}}}}";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Target {
    team_id: Uuid,
    project_id: Uuid,
}

impl Target {
    pub(super) fn parse(
        team_id: &str,
        project_id: &str,
    ) -> Result<Self, super::IntegrationConfigurationError> {
        let team_id = parse_canonical_uuid(team_id)?;
        let project_id = parse_canonical_uuid(project_id)?;
        Ok(Self {
            team_id,
            project_id,
        })
    }

    pub(super) fn public_name(&self) -> String {
        format!("{}/{}", self.team_id, self.project_id)
    }
}

pub(super) async fn check(
    client: &IntegrationClient,
    token: &SecretToken,
    target: &Target,
) -> Result<IntegrationCheck, IntegrationClientError> {
    let variables = CheckVariables {
        team: target.team_id.to_string(),
        project: target.project_id.to_string(),
    };
    let bytes = graphql(client, token, CHECK_QUERY, variables, false).await?;
    let response: GraphqlResponse<CheckData> = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    require_no_errors(&response, false)?;
    let Some(data) = response.data else {
        return Err(invalid_response(false, ExternalErrorCode::ApiError));
    };
    let (Some(team), Some(project)) = (data.team, data.project) else {
        return Err(invalid_response(false, ExternalErrorCode::ScopeMismatch));
    };
    if !same_uuid(&team.id, target.team_id) || !same_uuid(&project.id, target.project_id) {
        return Err(invalid_response(false, ExternalErrorCode::ScopeMismatch));
    }
    ensure_typed_size([team.id.len(), project.id.len()])
        .map_err(|code| invalid_response(false, code))?;
    Ok(IntegrationCheck {
        provider: ExternalProvider::Linear,
        target: target.public_name(),
        url: None,
    })
}

pub(super) async fn get_issue(
    client: &IntegrationClient,
    token: &SecretToken,
    target: &Target,
    external_id: &str,
) -> Result<ExternalIssue, IntegrationClientError> {
    let lookup = IssueLookup::parse(external_id)?;
    let variables = IssueVariables {
        id: external_id.to_owned(),
    };
    let bytes = graphql(client, token, ISSUE_QUERY, variables, false).await?;
    let response: GraphqlResponse<IssueData> = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    require_no_errors(&response, false)?;
    let issue = response
        .data
        .and_then(|data| data.issue)
        .ok_or_else(|| invalid_response(false, ExternalErrorCode::NotFound))?;
    validate_issue(issue, target, &lookup, false)
}

pub(super) async fn create_issue(
    client: &IntegrationClient,
    token: &SecretToken,
    target: &Target,
    creation_id: Uuid,
    title: &str,
    description: &str,
) -> Result<ExternalMutation, IntegrationClientError> {
    let variables = IssueCreateVariables {
        input: IssueCreateInput {
            id: creation_id.to_string(),
            team_id: target.team_id.to_string(),
            project_id: target.project_id.to_string(),
            title,
            description,
        },
    };
    let bytes = graphql(client, token, CREATE_ISSUE_MUTATION, variables, true).await?;
    let response: GraphqlResponse<IssueCreateData> = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(true, ExternalErrorCode::ApiError))?;
    require_no_errors(&response, true)?;
    let payload = response
        .data
        .and_then(|data| data.issue_create)
        .ok_or_else(|| invalid_response(true, ExternalErrorCode::ApiError))?;
    if !payload.success {
        return Err(invalid_response(true, ExternalErrorCode::ApiError));
    }
    let issue = payload
        .issue
        .ok_or_else(|| invalid_response(true, ExternalErrorCode::ApiError))?;
    if !same_uuid(&issue.id, creation_id)
        || !same_scope(&issue.team, issue.project.as_ref(), target)
        || !valid_linear_url(&issue.url)
        || !valid_issue_identifier(&issue.identifier)
        || !url_has_issue_identity(&issue.url, &issue.identifier)
    {
        return Err(invalid_response(true, ExternalErrorCode::ScopeMismatch));
    }
    ensure_typed_size([issue.id.len(), issue.identifier.len(), issue.url.len()])
        .map_err(|code| invalid_response(true, code))?;
    Ok(ExternalMutation {
        external_id: issue.id,
        url: issue.url,
    })
}

pub(super) async fn create_comment(
    client: &IntegrationClient,
    token: &SecretToken,
    target: &Target,
    issue_id: &str,
    creation_id: Uuid,
    body: &str,
) -> Result<ExternalMutation, IntegrationClientError> {
    let issue = get_issue(client, token, target, issue_id).await?;
    let expected_issue_id = Uuid::parse_str(&issue.external_id)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    let variables = CommentCreateVariables {
        input: CommentCreateInput {
            id: creation_id.to_string(),
            issue_id: issue.external_id,
            body,
        },
    };
    let bytes = graphql(client, token, CREATE_COMMENT_MUTATION, variables, true).await?;
    let response: GraphqlResponse<CommentCreateData> = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(true, ExternalErrorCode::ApiError))?;
    require_no_errors(&response, true)?;
    let payload = response
        .data
        .and_then(|data| data.comment_create)
        .ok_or_else(|| invalid_response(true, ExternalErrorCode::ApiError))?;
    if !payload.success {
        return Err(invalid_response(true, ExternalErrorCode::ApiError));
    }
    let comment = payload
        .comment
        .ok_or_else(|| invalid_response(true, ExternalErrorCode::ApiError))?;
    if !same_uuid(&comment.id, creation_id)
        || !same_uuid(&comment.issue.id, expected_issue_id)
        || !same_scope(&comment.issue.team, comment.issue.project.as_ref(), target)
        || !valid_linear_url(&comment.url)
    {
        return Err(invalid_response(true, ExternalErrorCode::ScopeMismatch));
    }
    ensure_typed_size([comment.id.len(), comment.url.len(), comment.issue.id.len()])
        .map_err(|code| invalid_response(true, code))?;
    Ok(ExternalMutation {
        external_id: comment.id,
        url: comment.url,
    })
}

pub(super) async fn verify_comment_marker(
    client: &IntegrationClient,
    token: &SecretToken,
    target: &Target,
    issue_id: &str,
    comment_id: &str,
    marker: &str,
) -> Result<ExternalMutation, IntegrationClientError> {
    let issue = get_issue(client, token, target, issue_id).await?;
    let expected_issue_id = Uuid::parse_str(&issue.external_id)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    let comment_uuid = parse_canonical_uuid(comment_id)
        .map_err(|_| invalid_response(false, ExternalErrorCode::InvalidRequest))?;
    let bytes = graphql(
        client,
        token,
        COMMENT_QUERY,
        IssueVariables {
            id: comment_id.to_owned(),
        },
        false,
    )
    .await?;
    let response: GraphqlResponse<CommentData> = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_response(false, ExternalErrorCode::ApiError))?;
    require_no_errors(&response, false)?;
    let comment = response
        .data
        .and_then(|data| data.comment)
        .ok_or_else(|| invalid_response(false, ExternalErrorCode::NotFound))?;
    if !same_uuid(&comment.id, comment_uuid)
        || !same_uuid(&comment.issue.id, expected_issue_id)
        || !same_scope(&comment.issue.team, comment.issue.project.as_ref(), target)
        || !valid_linear_url(&comment.url)
        || !comment.body.contains(marker)
    {
        return Err(invalid_response(false, ExternalErrorCode::ScopeMismatch));
    }
    ensure_typed_size([
        comment.id.len(),
        comment.url.len(),
        comment.body.len(),
        comment.issue.id.len(),
    ])
    .map_err(|code| invalid_response(false, code))?;
    Ok(ExternalMutation {
        external_id: comment.id,
        url: comment.url,
    })
}

async fn graphql<V: Serialize>(
    client: &IntegrationClient,
    token: &SecretToken,
    query: &'static str,
    variables: V,
    mutation: bool,
) -> Result<Vec<u8>, IntegrationClientError> {
    client
        .send(
            client
                .http
                .post(ENDPOINT)
                .header(header::AUTHORIZATION, token.expose())
                .header(header::CONTENT_TYPE, "application/json")
                .json(&GraphqlRequest { query, variables }),
            StatusCode::OK,
            mutation,
        )
        .await
}

fn validate_issue(
    issue: IssueNode,
    target: &Target,
    lookup: &IssueLookup,
    mutation: bool,
) -> Result<ExternalIssue, IntegrationClientError> {
    let issue_id = Uuid::parse_str(&issue.id)
        .ok()
        .filter(|id| id.to_string() == issue.id)
        .ok_or_else(|| invalid_response(mutation, ExternalErrorCode::ApiError))?;
    if !lookup.matches(issue_id, &issue.identifier)
        || !same_scope(&issue.team, issue.project.as_ref(), target)
        || !valid_issue_identifier(&issue.identifier)
        || !valid_linear_url(&issue.url)
        || !url_has_issue_identity(&issue.url, &issue.identifier)
        || issue.state.name.is_empty()
    {
        return Err(invalid_response(mutation, ExternalErrorCode::ScopeMismatch));
    }
    let description = issue.description.unwrap_or_default();
    ensure_typed_size([
        issue.id.len(),
        issue.identifier.len(),
        issue.url.len(),
        issue.title.len(),
        description.len(),
        issue.state.name.len(),
    ])
    .map_err(|code| invalid_response(mutation, code))?;
    Ok(ExternalIssue {
        external_id: issue.id,
        url: issue.url,
        title: issue.title,
        description,
        state: issue.state.name,
    })
}

fn require_no_errors<T>(
    response: &GraphqlResponse<T>,
    mutation: bool,
) -> Result<(), IntegrationClientError> {
    if response
        .errors
        .as_ref()
        .is_some_and(|errors| !errors.is_empty())
    {
        Err(invalid_response(mutation, ExternalErrorCode::ApiError))
    } else {
        Ok(())
    }
}

fn same_scope(team: &Identity, project: Option<&Identity>, target: &Target) -> bool {
    same_uuid(&team.id, target.team_id)
        && project.is_some_and(|project| same_uuid(&project.id, target.project_id))
}

fn same_uuid(value: &str, expected: Uuid) -> bool {
    Uuid::parse_str(value).is_ok_and(|actual| actual == expected && actual.to_string() == value)
}

fn parse_canonical_uuid(value: &str) -> Result<Uuid, super::IntegrationConfigurationError> {
    Uuid::parse_str(value)
        .ok()
        .filter(|id| id.to_string() == value)
        .ok_or(super::IntegrationConfigurationError)
}

fn valid_linear_url(value: &str) -> bool {
    parse_url(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str() == Some("linear.app")
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && url.query().is_none()
            && url.path() != "/"
    })
}

fn url_has_issue_identity(value: &str, identifier: &str) -> bool {
    let Ok(url) = parse_url(value) else {
        return false;
    };
    let Some(segments) = url.path_segments() else {
        return false;
    };
    let segments = segments.collect::<Vec<_>>();
    segments
        .windows(2)
        .any(|pair| pair[0] == "issue" && pair[1] == identifier)
}

fn valid_issue_identifier(value: &str) -> bool {
    if value.is_empty() || value.len() > 64 || !value.is_ascii() {
        return false;
    }
    let Some((key, number)) = value.split_once('-') else {
        return false;
    };
    !key.is_empty()
        && key.len() <= 32
        && key.as_bytes()[0].is_ascii_uppercase()
        && key
            .bytes()
            .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit())
        && !number.is_empty()
        && !number.starts_with('0')
        && number.bytes().all(|value| value.is_ascii_digit())
        && number.parse::<u64>().is_ok_and(|value| value > 0)
}

enum IssueLookup {
    Id(Uuid),
    Identifier(String),
}

impl IssueLookup {
    fn parse(value: &str) -> Result<Self, IntegrationClientError> {
        if value.len() > 64 {
            return Err(IntegrationClientError::Failed(
                ExternalErrorCode::InvalidRequest,
            ));
        }
        if let Ok(id) = Uuid::parse_str(value)
            && id.to_string() == value
        {
            return Ok(Self::Id(id));
        }
        if valid_issue_identifier(value) {
            Ok(Self::Identifier(value.to_owned()))
        } else {
            Err(IntegrationClientError::Failed(
                ExternalErrorCode::InvalidRequest,
            ))
        }
    }

    fn matches(&self, id: Uuid, identifier: &str) -> bool {
        match self {
            Self::Id(expected) => *expected == id,
            Self::Identifier(expected) => expected == identifier,
        }
    }
}

#[derive(Serialize)]
struct GraphqlRequest<V> {
    query: &'static str,
    variables: V,
}

#[derive(Serialize)]
struct CheckVariables {
    team: String,
    project: String,
}

#[derive(Serialize)]
struct IssueVariables {
    id: String,
}

#[derive(Serialize)]
struct IssueCreateVariables<'a> {
    input: IssueCreateInput<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IssueCreateInput<'a> {
    id: String,
    team_id: String,
    project_id: String,
    title: &'a str,
    description: &'a str,
}

#[derive(Serialize)]
struct CommentCreateVariables<'a> {
    input: CommentCreateInput<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CommentCreateInput<'a> {
    id: String,
    issue_id: String,
    body: &'a str,
}

#[derive(Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct CheckData {
    team: Option<Identity>,
    project: Option<Identity>,
}

#[derive(Deserialize)]
struct IssueData {
    issue: Option<IssueNode>,
}

#[derive(Deserialize)]
struct Identity {
    id: String,
}

#[derive(Deserialize)]
struct StateNode {
    name: String,
}

#[derive(Deserialize)]
struct IssueNode {
    id: String,
    identifier: String,
    url: String,
    title: String,
    description: Option<String>,
    team: Identity,
    project: Option<Identity>,
    state: StateNode,
}

#[derive(Deserialize)]
struct IssueCreateData {
    #[serde(rename = "issueCreate")]
    issue_create: Option<IssueCreatePayload>,
}

#[derive(Deserialize)]
struct IssueCreatePayload {
    success: bool,
    issue: Option<CreatedIssueNode>,
}

#[derive(Deserialize)]
struct CreatedIssueNode {
    id: String,
    identifier: String,
    url: String,
    team: Identity,
    project: Option<Identity>,
}

#[derive(Deserialize)]
struct CommentCreateData {
    #[serde(rename = "commentCreate")]
    comment_create: Option<CommentCreatePayload>,
}

#[derive(Deserialize)]
struct CommentCreatePayload {
    success: bool,
    comment: Option<CommentNode>,
}

#[derive(Deserialize)]
struct CommentNode {
    id: String,
    url: String,
    issue: CommentIssueNode,
}

#[derive(Deserialize)]
struct CommentIssueNode {
    id: String,
    team: Identity,
    project: Option<Identity>,
}

#[derive(Deserialize)]
struct CommentData {
    comment: Option<CommentReadNode>,
}

#[derive(Deserialize)]
struct CommentReadNode {
    id: String,
    url: String,
    body: String,
    issue: CommentIssueNode,
}
