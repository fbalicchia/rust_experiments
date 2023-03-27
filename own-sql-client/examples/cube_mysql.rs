extern crate mysql;
use mysql::prelude::*;
use mysql::Opts;





fn main() {
    let mut db =
        mysql::Conn::new(Opts::from_url(&format!("mysql://127.0.0.1:{}", "3306")).unwrap()).unwrap();
     assert_eq!(db.ping(), true);
    let result = db.query_iter("SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = 'dev_pre_aggregations'").unwrap().count();
    println!("result {}", result);
    drop(db);
}



