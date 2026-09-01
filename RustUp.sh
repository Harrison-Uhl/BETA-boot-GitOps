#!/bin/bash
# https://github.com/Harrison-Uhl/BETA-boot-GitOps/RustUp.sh
# Entry point to load & Run RustUp bundle

# --- Usage:
# export GitOpsUrl=https://github.com/Harrison-Uhl/BETA-boot-GitOps
# git clone --depth 1 --single-branch -b main $GitOpsUrl

cd BETA-boot-GitOps
# cat RustUp.sh

set +euxo

# Add the following early to avoid warnings during RuspUP...
sudo dnf install cmake gcc make curl clang llvm lld -y

# 1. Initialize Rustup silently passing automatic defaults (-y)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# 2. Actively link environment maps onto your current active profile instance
source "$HOME/.cargo/env"
# 3. Verify total tool environment status flags
echo '--- Versions ---'
rustc --version && cargo --version

mkdir -p ./rustprojects/helloworld
cd ./rustprojects/helloworld

##nano hello.rs

cat >hello.rs <<EOF
fn main() {
    println!("Congratulations! Rust is installed, compiling and running.");
}
EOF

rustc hello.rs
./hello

