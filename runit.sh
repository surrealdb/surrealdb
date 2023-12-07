#!/bin/zsh
RUSTFLAGS="--cfg tokio_unstable" cargo build --features storage-rocksdb

# Check if the previous command (cargo build) succeeded
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi

echo "Starting db"
export SURREAL_INSECURE_FORWARD_SCOPE_ERRORS=false

export LOGARG="RUST_LOG=tokio=trace,surrealdb=trace,tungstenite=trace"
export LOGARG="RUST_LOG=debug"

rm lock.csv
target/debug/surreal start --allow-all --log trace --user root --pass root memory >  ~/le-file.txt 2>&1

