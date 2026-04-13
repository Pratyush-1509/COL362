cargo build -r
cargo run -r --bin generator -- all \
    -d scratch/datasets/tpch \
    -c scratch/compiled_datasets/tpch \
    -r scratch/runtimes/tpch \
    -b target/release \
    -s 4096s
cargo run -r --bin tests_gen -- -c scratch/compiled_datasets/tpch -r scratch/runtimes/tpch
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json