
mod tiny_kv;
mod handler;
use actix_web::{web::{self, resource}, App, HttpServer};


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db: tiny_kv::RocksDB = tiny_kv::KVStore::init("/tmp/fixrocks-db");

    HttpServer::new(move || {
        App::new()
        .app_data(web::Data::new(db.clone()))
        .service(
            web::scope("/api")
            .service(
                resource("/{key}")
                .route(web::get().to(handler::get))
                .route(web::post().to(handler::post))
                .route(web::post().to(handler::delete))
            ),
        )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}