#!/usr/bin/env bash
# Format all C++ sources with clang-format (Google style, 100-col).
set -euo pipefail
if ! command -v clang-format >/dev/null 2>&1; then
  echo "clang-format not found; install it to format sources." >&2
  exit 1
fi
clang-format -i taskq.hpp example.cpp tests.cpp
echo "Formatted taskq.hpp, example.cpp, tests.cpp"
