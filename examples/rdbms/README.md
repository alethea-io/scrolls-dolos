## Transpile and bundle reducer code
```bash
deno run --allow-read --allow-net --allow-env --allow-run --allow-write  build.ts
```

## Launch dolos
```bash
cd /path/to/dolos/examples/sync-mainnet
cargo run daemon
```

## Run scrolls
```bash
RUST_BACKTRACE=1 cargo run --bin scrolls -- daemon --config examples/rdbms/daemon.toml
```