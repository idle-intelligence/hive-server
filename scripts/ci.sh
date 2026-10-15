#!/usr/bin/env bash
# Local CI: the steps a hosted workflow would run. Run from anywhere.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --check
cargo clippy --locked --all-targets
cargo test --locked
cargo build --locked --release
echo "hive-server: ci ok"
