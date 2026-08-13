# Environment

Toolchain versions recorded at build time. Development on Apple Silicon macOS;
CI on `ubuntu-latest`.

## Dev machine (build-time snapshot, 2026-08-08)
- OS: macOS (Darwin 25.5.0), Apple Silicon (arm64), 10 cores
- Go: `go1.26.5 darwin/arm64` (module pinned to `go 1.23` for CI portability)
- golangci-lint: `2.12.2` (config schema v2)
- gh: `2.93.0`

## CI (`.github/workflows/ci.yml`)
- `ubuntu-latest`, Go `1.23`
- Steps: gofmt check · `go build ./...` · `go vet ./...` · golangci-lint · `go test ./...`
  · `go test -race ./...` · bounded 200-seed sweep.
- Long seed sweeps (1k at , 10k at ) run in extended runs as background jobs, not in CI,
  so CI stays fast.

## Dependencies (see DESIGN.md for justification)
- Standard library
- `github.com/anishathalye/porcupine` — linearizability checker
- golangci-lint (dev/CI tool, not a runtime dep)

No other dependencies without a written justification in DESIGN.md.
