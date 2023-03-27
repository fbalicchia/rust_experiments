fn main() {
    println!("Hello, world!");
    let metrics_addr = std::env::var("CUBESTORE_METRICS_ADDRESS").unwrap_or("127.0.0.1".to_string());
    let metrics_port = std::env::var("CUBESTORE_METRICS_PORT").unwrap_or("8125".to_string());
    let metrics_server_address = format!("{}:{}",metrics_addr,metrics_port);
    println!("server address {}",metrics_server_address);

}
