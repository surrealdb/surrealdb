#!/bin/zsh
RUSTFLAGS="--cfg tokio_unstable" cargo build --features storage-rocksdb
echo "Starting db"
SURREAL_INSECURE_FORWARD_SCOPE_ERRORS=false RUST_LOG=tokio=trace,surrealdb=trace,tungstenite=trace target/debug/surreal start --allow-all --log trace --user root --pass root memory >  ~/le-file.txt 2>&1

