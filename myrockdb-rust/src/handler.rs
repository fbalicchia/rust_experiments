use actix_web::{web::{Data, Path}, HttpResponse};
use bytes::Bytes;
use json::parse;

use crate::tiny_kv::{KVStore, RocksDB};

pub async fn get(key: Path<String>, db: Data<RocksDB>) -> HttpResponse {
    match &db.find(&key.into_inner()) {
        Some(v) => {
            parse(v)
                .map(|obj| HttpResponse::Ok().content_type("application/json").body(obj.dump()))
                .unwrap_or(HttpResponse::InternalServerError().content_type("application/json").finish())
        }
        None    => HttpResponse::NotFound().content_type("application/json").finish()
    }
}

pub async fn post(key:  Path<String>,
                  db:   Data<RocksDB>,
                  body: Bytes) -> HttpResponse {
    match String::from_utf8(body.to_vec()) {
        Ok(body) => match &db.save(&key.into_inner(), &body) {
            true  => {
                parse(&body)
                    .map(|obj| HttpResponse::Ok().content_type("application/json").body(obj.dump()))
                    .unwrap_or(HttpResponse::InternalServerError().content_type("application/json").finish())
            }
            false => HttpResponse::InternalServerError().content_type("application/json").finish()
        }
        Err(_) => HttpResponse::InternalServerError().content_type("application/json").finish(),
    }
}

pub async fn delete(key: Path<String>, db: Data<RocksDB>) -> HttpResponse {
    match &db.delete(&key.into_inner()) {
        true  => HttpResponse::NoContent().content_type("application/json").finish(),
        false => HttpResponse::InternalServerError().content_type("application/json").finish()
    }
}
