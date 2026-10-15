mod hub;
mod log;
mod protocol;
mod router;
mod state;
mod validate;

use tokio::net::TcpListener;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use validate::OriginPolicy;

#[tokio::main]
async fn main() {
    let host = std::env::var("HUB_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("HUB_PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");

    let ts = log::ts();
    println!("{ts} [swarm] hive-server listening on ws://{addr}");

    let policy = OriginPolicy::from_env_value(std::env::var("HUB_ALLOWED_ORIGINS").ok().as_deref());
    println!("{ts} [swarm] allowed origins: {policy:?}");

    let hub = hub::Hub::new();
    hub.start_stats_ticker();
    hub.start_time_ticker();

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };

        let ts = log::ts();
        println!("{ts} [swarm] connection from {peer_addr}");

        let hub = hub.clone();
        let policy = policy.clone();
        tokio::spawn(async move {
            let check = |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                let origin = req.headers().get("origin").and_then(|v| v.to_str().ok());
                if policy.allows(origin) {
                    Ok(resp)
                } else {
                    let mut err = ErrorResponse::new(Some("origin not allowed".to_string()));
                    *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
                    Err(err)
                }
            };
            match accept_hdr_async(stream, check).await {
                Ok(ws) => hub.handle_connection(ws).await,
                Err(e) => eprintln!("websocket handshake error: {e}"),
            }
        });
    }
}
