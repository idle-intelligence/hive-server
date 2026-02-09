mod hub;
mod log;
mod protocol;
mod router;
mod state;

use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

#[tokio::main]
async fn main() {
    let host = std::env::var("HUB_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("HUB_PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");

    let ts = log::ts();
    println!("{ts} [swarm] hive-server listening on ws://{addr}");

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
        tokio::spawn(async move {
            match accept_async(stream).await {
                Ok(ws) => hub.handle_connection(ws).await,
                Err(e) => eprintln!("websocket handshake error: {e}"),
            }
        });
    }
}
