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

`AGPL-3.0-or-later OR LicenseRef-Commercial` — デュアルライセンス どちらかを選べる

| 選択肢 | 条文 | こういう時 |
|--------|------|-----------|
| **AGPL-3.0-or-later** | [LICENSE-AGPL](LICENSE-AGPL) — 無償、報告義務なし | 自分の project も AGPL 互換の OSS、または社内利用のみ |
| **商用ライセンス** | [LICENSE-COMMERCIAL.md](LICENSE-COMMERCIAL.md) — 有償、コピーレフト義務を解除 | クローズドソース製品 / 商用 SaaS / エッジ・ファームウェア配布 / plugin 再配布 / ソース開示を禁じるプラットフォーム NDA |

AGPL は強いコピーレフト: `alice-grpc` を link して配布 / 提供する製品・ファームウェア・
サービスは AGPL で公開する義務がある これはオープンなエコシステムのための意図的な
選択で、それが実行できない場合のために商用ライセンスを用意している

商用ライセンスの問い合わせ: <contact@extoria.co.jp>
