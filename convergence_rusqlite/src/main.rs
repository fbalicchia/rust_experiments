use convergence::engine::{Engine, Portal};
use async_trait::async_trait;
use convergence::protocol_ext::DataRowBatch;
use convergence::protocol::{DataTypeOid, ErrorResponse, FieldDescription, SqlState};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use std::sync::Arc;
use convergence::server::{self, BindOptions};


struct ReturnSingleScalarPortal;

#[async_trait]
impl Portal for ReturnSingleScalarPortal {
	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
		let mut row = batch.create_row();
		row.write_int4(1);
		Ok(())
	}
}



struct ReturnSingleScalarEngine;
#[async_trait]
impl Engine for ReturnSingleScalarEngine {
	type PortalType = ReturnSingleScalarPortal;

	async fn prepare(&mut self, statement: &Statement) -> Result<Vec<FieldDescription>, ErrorResponse> {
		if let Statement::Query(query) = &statement {
			if let SetExpr::Select(select) = &*query.body {
				if select.projection.len() == 1 {
					if let SelectItem::UnnamedExpr(Expr::Identifier(column_name)) = &select.projection[0] {
						match column_name.value.as_str() {
							"test_error" => return Err(ErrorResponse::error(SqlState::DATA_EXCEPTION, "test error")),
							"test_fatal" => return Err(ErrorResponse::fatal(SqlState::DATA_EXCEPTION, "fatal error")),
							_ => (),
						}
					}
				}
			}
		}

		Ok(vec![FieldDescription {
			name: "test".to_owned(),
			data_type: DataTypeOid::Int4,
		}])
	}

	async fn create_portal(&mut self, _: &Statement) -> Result<Self::PortalType, ErrorResponse> {
		Ok(ReturnSingleScalarPortal)
	}
}





#[tokio::main]
async fn main() {
    server::run(
        BindOptions::new().with_addr("127.0.0.1").with_port(5432),
        Arc::new(|| Box::pin(async { ReturnSingleScalarEngine })),
    )
    .await
    .unwrap();
}