use backend::{AppState, config::Config, reaper};
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

    let reaper = reaper::spawn(pool.clone(), config.reaper_interval);
    let app = backend::router(AppState::new(pool));

    let listener = TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    reaper.abort();
    Ok(())
}
