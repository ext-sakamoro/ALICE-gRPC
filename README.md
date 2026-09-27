**English** | [日本語](README_JP.md)

# ALICE-gRPC

Pure Rust gRPC framework for the ALICE ecosystem. Provides Protobuf encoding/decoding, service definition, unary/streaming RPC, metadata, status codes, and channel management with zero external dependencies.

## Overview

| Item | Value |
|------|-------|
| **Crate** | `alice-grpc` |
| **Version** | 1.0.0 |
| **License** | AGPL-3.0 |
| **Edition** | 2021 |

## Features

- **Protobuf Wire Format** — Full varint, fixed32/64, and length-delimited encoding/decoding
- **Zigzag Encoding** — Efficient signed integer encoding (sint32/sint64)
- **Message Codec** — Encode/decode structured messages with field tags and wire types
- **Service Definition** — Define gRPC services with method descriptors
- **Unary & Streaming RPC** — Support for unary, server-streaming, client-streaming, and bidirectional modes
- **Metadata** — Key-value metadata (headers/trailers) for RPC calls
- **Status Codes** — Full gRPC status code enum (OK, Cancelled, InvalidArgument, etc.)
- **Channel Management** — Logical connection abstraction for service communication

## Architecture

```
alice-grpc (lib.rs — single-file crate)
├── WireType                     # Protobuf wire types
├── encode_varint / decode_varint # Varint codec
├── zigzag_encode / zigzag_decode # Signed integer codec
├── ProtoMessage / ProtoField    # Message abstraction
├── ServiceDescriptor / Method   # Service definition
├── StatusCode / Status          # gRPC status handling
├── Metadata                     # Headers and trailers
└── Channel                      # Connection management
```

## Quick Start

```rust
use alice_grpc::{encode_varint, decode_varint, WireType};

let mut buf = Vec::new();
encode_varint(300, &mut buf);
let (value, bytes_read) = decode_varint(&buf).unwrap();
assert_eq!(value, 300);
```

## Build

```bash
cargo build
cargo test
cargo clippy -- -W clippy::all
```

## License

`AGPL-3.0-or-later OR LicenseRef-Commercial` — dual-licensed. Pick either.

| Option | Terms | Use it when |
|--------|-------|-------------|
| **AGPL-3.0-or-later** | [LICENSE-AGPL](LICENSE-AGPL) — free, no reporting obligation | Your project is itself AGPL-compatible open source, or you are only using it internally |
| **Commercial License** | [LICENSE-COMMERCIAL.md](LICENSE-COMMERCIAL.md) — paid, removes the copyleft | Closed-source product, proprietary SaaS, edge / firmware distribution, plugin redistribution, or a platform NDA that forbids source disclosure |

AGPL is a strong copyleft: a product, firmware image, or service that links
`alice-grpc` and is distributed or served to users must be released under the AGPL
as well. That is intentional for the open ecosystem, and the Commercial
License exists for the cases where it is not something you are able to do.

Commercial licence enquiries: <contact@extoria.co.jp>
