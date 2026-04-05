#!/usr/bin/env bash
set -euo pipefail

ZIG_BIN="/home/ratushniak-da/.config/Code/User/globalStorage/ziglang.vscode-zig/zig/x86_64-linux-0.16.0-dev.2905+5d71e3051/zig"

"${ZIG_BIN}" build test
