//! Errors that are *not* cached for idempotent replay: authentication and
//! plan-limit rejections, request validation failures, missing wallets,
//! conflicts that clear by themselves (an item being charged right now),
//! overload (503: pool or lock waits timed out) and internal faults (which roll the transaction back, so a retry with the same
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
    /// Not cached, so a retry with the same key is decided afresh.
    Conflict { code: &'static str, message: String, retry_after_secs: Option<u64> },
    TooManyRequests { code: &'static str, message: String, retry_after_secs: Option<u64> },
    /// The server is too busy to take the request right now (pool or lock
    /// wait timed out). Nothing was written; retry after `retry_after_secs`.
    Unavailable { code: &'static str, message: String, retry_after_secs: u64 },
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

    pub fn conflict(code: &'static str, message: impl Into<String>, retry_after_secs: Option<u64>) -> Self {
        Self::Conflict { code, message: message.into(), retry_after_secs }
    }

    pub fn too_many_requests(
        code: &'static str,
        message: impl Into<String>,
        retry_after_secs: Option<u64>,
    ) -> Self {
        Self::TooManyRequests { code, message: message.into(), retry_after_secs }
    }

    pub fn unavailable(code: &'static str, message: impl Into<String>, retry_after_secs: u64) -> Self {
        Self::Unavailable { code, message: message.into(), retry_after_secs }
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
            | Self::Conflict { code, message, .. }
            | Self::TooManyRequests { code, message, .. }
            | Self::Unavailable { code, message, .. } => write!(f, "{code}: {message}"),
            Self::Internal(detail) => write!(f, "internal error: {detail}"),
        }
    }
}

impl std::error::Error for ApiError {}

/// Postgres: `lock_timeout` ran out waiting for a row lock.
const LOCK_NOT_AVAILABLE: &str = "55P03";
/// Postgres: `statement_timeout` (or a cancel) stopped the statement.
const QUERY_CANCELED: &str = "57014";

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        // Overload is not a bug: answer 503 so clients back off and retry,
        // instead of a 500 that looks like a fault.
        match &err {
            sqlx::Error::PoolTimedOut => {
                return Self::unavailable("service_busy", "the server is busy; retry shortly", 1);
            }
            sqlx::Error::Database(db) => match db.code().as_deref() {
                Some(LOCK_NOT_AVAILABLE) => {
                    return Self::unavailable("wallet_busy", "the wallet is busy; retry shortly", 1);
                }
                Some(QUERY_CANCELED) => {
                    return Self::unavailable("timeout", "the request took too long and was cancelled; retry", 1);
                }
                _ => {}
            },
            _ => {}
        }
        Self::Internal(format!("database error: {err}"))
    }
}

impl ApiError {
    /// The error code and message a client sees, for errors reported inside
    /// a larger response (one tenant of an import). Internal details stay in
    /// the log.
    pub fn client_parts(&self) -> (&'static str, String) {
        match self {
            Self::BadRequest { code, message }
            | Self::Unauthorized { code, message }
            | Self::Forbidden { code, message }
            | Self::NotFound { code, message }
            | Self::Unprocessable { code, message }
            | Self::Conflict { code, message, .. }
            | Self::TooManyRequests { code, message, .. }
            | Self::Unavailable { code, message, .. } => (code, message.clone()),
            Self::Internal(detail) => {
                tracing::error!(%detail, "request failed");
                ("internal_error", "an internal error occurred; retry with the same Idempotency-Key".to_owned())
            }
        }
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
            Self::Conflict { code, message, retry_after_secs } => {
                retry_after = retry_after_secs;
                (StatusCode::CONFLICT, code, message)
            }
            Self::TooManyRequests { code, message, retry_after_secs } => {
                retry_after = retry_after_secs;
                (StatusCode::TOO_MANY_REQUESTS, code, message)
            }
            Self::Unavailable { code, message, retry_after_secs } => {
                tracing::warn!(code, %message, "request refused: server busy");
                retry_after = Some(retry_after_secs);
                (StatusCode::SERVICE_UNAVAILABLE, code, message)
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
