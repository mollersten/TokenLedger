//! Errors that are *not* cached for idempotent replay: authentication and
//! plan-limit rejections, request validation failures, missing wallets and
//! internal faults (which roll the transaction back, so a retry with the same
//! key re-executes safely).

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
    BadRequest { code: &'static str, message: String },
    Unauthorized { code: &'static str, message: String },
    Forbidden { code: &'static str, message: String },
    NotFound { code: &'static str, message: String },
    Unprocessable { code: &'static str, message: String },
    TooManyRequests { code: &'static str, message: String, retry_after_secs: Option<u64> },
    Internal(String),
}

impl ApiError {
    pub fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::BadRequest { code, message: message.into() }
    }

    pub fn unauthorized(code: &'static str, message: impl Into<String>) -> Self {
        Self::Unauthorized { code, message: message.into() }
    }

    pub fn forbidden(code: &'static str, message: impl Into<String>) -> Self {
        Self::Forbidden { code, message: message.into() }
    }

    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::NotFound { code, message: message.into() }
    }

    pub fn unprocessable(code: &'static str, message: impl Into<String>) -> Self {
        Self::Unprocessable { code, message: message.into() }
    }

    pub fn too_many_requests(
        code: &'static str,
        message: impl Into<String>,
        retry_after_secs: Option<u64>,
    ) -> Self {
        Self::TooManyRequests { code, message: message.into(), retry_after_secs }
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    pub fn wallet_not_found(tenant_id: &str) -> Self {
        Self::not_found("wallet_not_found", format!("no wallet exists for tenant '{tenant_id}'"))
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest { code, message }
            | Self::Unauthorized { code, message }
            | Self::Forbidden { code, message }
            | Self::NotFound { code, message }
            | Self::Unprocessable { code, message }
            | Self::TooManyRequests { code, message, .. } => write!(f, "{code}: {message}"),
            Self::Internal(detail) => write!(f, "internal error: {detail}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        Self::Internal(format!("database error: {err}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut retry_after = None;
        let (status, code, message) = match self {
            Self::BadRequest { code, message } => (StatusCode::BAD_REQUEST, code, message),
            Self::Unauthorized { code, message } => (StatusCode::UNAUTHORIZED, code, message),
            Self::Forbidden { code, message } => (StatusCode::FORBIDDEN, code, message),
            Self::NotFound { code, message } => (StatusCode::NOT_FOUND, code, message),
            Self::Unprocessable { code, message } => {
                (StatusCode::UNPROCESSABLE_ENTITY, code, message)
            }
            Self::TooManyRequests { code, message, retry_after_secs } => {
                retry_after = retry_after_secs;
                (StatusCode::TOO_MANY_REQUESTS, code, message)
            }
            Self::Internal(detail) => {
                tracing::error!(%detail, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "an internal error occurred; retry with the same Idempotency-Key".to_owned(),
                )
            }
        };
        let mut response = (status, Json(json!({ "error": code, "message": message }))).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        if let Some(secs) = retry_after {
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(secs));
        }
        response
    }
}
