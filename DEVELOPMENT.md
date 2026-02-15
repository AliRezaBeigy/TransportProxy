# Build & Development

## Build

```bash
cargo build --release
```

## Development

```bash
cargo fmt -- --check    # check formatting
cargo clippy --all-targets -- -D warnings   # lint
```

## ys-kcp

This project uses a patched **ys-kcp** via `[patch.crates-io]` in `Cargo.toml` (https://github.com/AliRezaBeigy/ys-kcp). Upstream ys4e/ys-kcp has a bug in `send()` where segment payload is never copied; the fork includes the fix.
