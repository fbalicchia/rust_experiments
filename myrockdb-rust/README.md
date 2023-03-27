# myrockdb: Tiny kv store build on top rocksdb

Under construction

See [rust-rocksdb](https://crates.io/crates/rocksdb)

## How to Use

```bash

#Store a value
curl -i -X POST -H "Content-Type: application/json" -d '{"bar":"baz"}' http://localhost:8080/api/learn
#Retrieve a value
curl -i -X GET -H "Content-Type: application/json" http://localhost:8080/api/learn
#Delete a value
curl -i -X DELETE -H "Content-Type: application/json" http://localhost:8080/api/learn
```

