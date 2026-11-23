use backend::{AppState, auth, config::Config, reaper};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let pool = backend::connect(&config.database_url, config.max_connections).await?;
    backend::migrate(&pool).await?;
    tracing::info!(
        database = %backend::database_label(&config.database_url),
        max_connections = config.max_connections,
        "database connected and migrations applied"
    );

    let state = AppState::with_auth_cache_ttl(pool.clone(), config.auth_cache_ttl).with_import_caps(config.import_caps);
    let reaper = reaper::spawn(pool.clone(), config.reaper_interval);
    let flusher = auth::spawn_usage_flusher(pool.clone(), state.usage.clone(), config.usage_flush_interval);
    let pruner = backend::idempotency::spawn_pruner(
        pool.clone(),
        config.idempotency_retention,
        backend::idempotency::PRUNE_INTERVAL,
    );
    let app = backend::router(state.clone());

    let listener = TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    reaper.abort();
    flusher.abort();
    pruner.abort();
    if let Err(err) = state.usage.flush(&pool).await {
        tracing::warn!(error = %err, "final usage flush failed");
    }
    Ok(())
}

/// Ctrl-C locally; SIGTERM from Docker and most hosting platforms. In-flight
/// requests finish, then the reaper stops and the last usage counts are
/// written.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutting down");
}
