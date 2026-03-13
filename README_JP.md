[English](README.md) | **日本語**

# ALICE-gRPC

ALICEエコシステムの純Rust gRPCフレームワーク。Protobufエンコーディング/デコーディング、サービス定義、ユナリ/ストリーミングRPC、メタデータ、ステータスコード、チャネル管理を外部依存なしで提供。

## 概要

| 項目 | 値 |
|------|-----|
| **クレート名** | `alice-grpc` |
| **バージョン** | 1.0.0 |
| **ライセンス** | AGPL-3.0 |
| **エディション** | 2021 |

## 機能

- **Protobufワイヤフォーマット** — varint、fixed32/64、length-delimitedの完全なエンコード/デコード
- **Zigzagエンコーディング** — 符号付き整数の効率的エンコーディング (sint32/sint64)
- **メッセージコーデック** — フィールドタグとワイヤタイプによる構造化メッセージのエンコード/デコード
- **サービス定義** — メソッドディスクリプタ付きgRPCサービス定義
- **ユナリ＆ストリーミングRPC** — ユナリ、サーバーストリーミング、クライアントストリーミング、双方向モード対応
- **メタデータ** — RPC呼び出し用のキーバリューメタデータ（ヘッダー/トレーラー）
- **ステータスコード** — 完全なgRPCステータスコード列挙型
- **チャネル管理** — サービス通信のための論理接続抽象化

## アーキテクチャ

```
alice-grpc (lib.rs — 単一ファイルクレート)
├── WireType                     # Protobufワイヤタイプ
├── encode_varint / decode_varint # Varintコーデック
├── zigzag_encode / zigzag_decode # 符号付き整数コーデック
├── ProtoMessage / ProtoField    # メッセージ抽象化
├── ServiceDescriptor / Method   # サービス定義
├── StatusCode / Status          # gRPCステータス処理
├── Metadata                     # ヘッダーとトレーラー
└── Channel                      # 接続管理
```

## クイックスタート

```rust
use alice_grpc::{encode_varint, decode_varint, WireType};

let mut buf = Vec::new();
encode_varint(300, &mut buf);
let (value, bytes_read) = decode_varint(&buf).unwrap();
assert_eq!(value, 300);
```

## ビルド

```bash
cargo build
cargo test
cargo clippy -- -W clippy::all
```

## ライセンス

AGPL-3.0 — 詳細は [LICENSE](LICENSE) を参照。
