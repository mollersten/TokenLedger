use backend::{AppState, auth, config::Config, reaper};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,backend=debug")),
        )
        .init();

    let config = Config::from_env()?;
    let pool = backend::connect(&config.database_url, config.max_connections).await?;
    backend::MIGRATOR.run(&pool).await?;
    tracing::info!("database connected and migrations applied");

    let state = AppState::with_auth_cache_ttl(pool.clone(), config.auth_cache_ttl);
    let reaper = reaper::spawn(pool.clone(), config.reaper_interval);
    let flusher = auth::spawn_usage_flusher(pool.clone(), state.usage.clone(), config.usage_flush_interval);
    let app = backend::router(state.clone());

    let listener = TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    reaper.abort();
    flusher.abort();
    if let Err(err) = state.usage.flush(&pool).await {
        tracing::warn!(error = %err, "final usage flush failed");
    }
    Ok(())
}
