#!/usr/bin/env bash
# Build the taskq test suite. Resolves Homebrew include/lib paths automatically
# on macOS; on Linux the system paths usually suffice.
set -euo pipefail

CXX=${CXX:-g++}
INCLUDES=(-I.)
LIBS=(-lhiredis -lpthread)

# Pull in Homebrew-installed dependencies when brew is available.
if command -v brew >/dev/null 2>&1; then
  for pkg in hiredis nlohmann-json catch2; do
    prefix=$(brew --prefix "$pkg" 2>/dev/null || true)
    if [ -n "$prefix" ]; then
      INCLUDES+=("-I$prefix/include")
      LIBS+=("-L$prefix/lib")
    fi
  done
  LIBS+=(-lCatch2Main -lCatch2)
else
  # System install of Catch2 v3.
  LIBS+=(-lCatch2Main -lCatch2)
fi

echo "Building tests..."
"$CXX" -std=c++17 -Wall -Wextra -O2 tests.cpp "${INCLUDES[@]}" "${LIBS[@]}" -o tests
echo "Done. Run ./tests  (try ./tests '[retry]' to filter by tag)"
