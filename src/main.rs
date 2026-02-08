mod hub;
mod log;
mod protocol;
mod state;

use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

#[tokio::main]
async fn main() {
    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await.expect("failed to bind");

    let ts = log::ts();
    println!("{ts} [swarm] hive-server listening on ws://{addr}");

    let hub = hub::Hub::new();

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
