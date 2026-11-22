#!/bin/zsh
# Runs the attribute-filter property check: copies the checker into the test
# project, runs only it against the real DuckDB engine, and removes the copy
# whatever happens. Env knobs: FILTER_FUZZ_ITERATIONS (default 20000),
# FILTER_FUZZ_SEED (default 20261121).
set -e
cd "$(dirname "$0")/../../.."
cp .claude/skills/formal-verify-attribute-filter/Scratch_AttributeFilterPropertyCheck.cs DuckDBGeoparquet.Tests/
trap 'rm -f DuckDBGeoparquet.Tests/Scratch_AttributeFilterPropertyCheck.cs' EXIT
dotnet test DuckDBGeoparquet.Tests/DuckDBGeoparquet.Tests.csproj \
  --filter "FullyQualifiedName~Scratch_AttributeFilterPropertyCheck" \
  --logger "console;verbosity=normal" "$@"
