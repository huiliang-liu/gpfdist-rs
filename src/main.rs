mod df_engine;
mod server;
mod util;

use server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("GPFDIST_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    
    let server = Server::new(addr);
    server.run().await?;
    
    Ok(())
}
