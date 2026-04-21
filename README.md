# gRPCServer.jl

[![DOI](https://zenodo.org/badge/1131647982.svg)](https://doi.org/10.5281/zenodo.18642866)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/s-celles/gRPCServer.jl)
[![Build Status](https://github.com/s-celles/gRPCServer.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/s-celles/gRPCServer.jl/actions/workflows/CI.yml)
[![Coverage](https://codecov.io/gh/s-celles/gRPCServer.jl/branch/develop/graph/badge.svg)](https://codecov.io/gh/s-celles/gRPCServer.jl)
[![Documentation](https://img.shields.io/badge/docs-dev-blue.svg)](https://s-celles.github.io/gRPCServer.jl/dev)

A native Julia implementation of a [gRPC](https://grpc.io/) server library.

## Installation

```julia
using Pkg
Pkg.dev("https://github.com/s-celles/gRPCServer.jl")

# Pkg.add("gRPCServer")  # when registered
```

## Documentation

Full documentation is available at [s-celles.github.io/gRPCServer.jl](https://s-celles.github.io/gRPCServer.jl/dev).

## Requirements

- Julia 1.10 or later
- ProtoBuf.jl for message serialization

## Operational Limits

`ServerConfig.max_connections`, `max_concurrent_requests`, and
`max_queued_requests` are enforced by the runtime. Excess accepted connections
are closed at the server boundary, and requests beyond the active plus queued
budget are rejected with `RESOURCE_EXHAUSTED`. Queued requests also honor the
incoming `grpc-timeout` deadline while waiting for server capacity. A graceful
`stop!(server; force=false)` closes the listener, keeps admitted requests alive
through drain, closes remaining idle connections once in-flight requests are
finished, and refuses new work while the server is draining, including new
streams opened on existing HTTP/2 connections.

## Related Packages

- [gRPCClient.jl](https://github.com/JuliaIO/gRPCClient.jl) - gRPC client for Julia
- [ProtoBuf.jl](https://github.com/JuliaIO/ProtoBuf.jl) - Protocol buffer support for Julia

## License

MIT License - see LICENSE file for details.
