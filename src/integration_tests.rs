//! Integration tests spanning multiple modules.

#![allow(
    clippy::float_cmp,
    clippy::unreadable_literal,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::too_many_lines,
    clippy::needless_range_loop,
    clippy::explicit_iter_loop,
    clippy::bool_to_int_with_if,
    clippy::approx_constant,
    clippy::cast_lossless,
    clippy::redundant_clone,
    clippy::format_collect,
    clippy::similar_names,
    clippy::needless_collect
)]

use crate::channel::*;
use crate::channel_pool::*;
use crate::errors::*;
use crate::field_tag::*;
use crate::field_value::*;
use crate::grpc_frame::*;
use crate::metadata::*;
use crate::method_type::*;
use crate::proto_message::*;
use crate::router::*;
use crate::rpc::*;
use crate::service::*;
use crate::status::*;
use crate::status_code::*;
use crate::streaming::*;
use crate::varint::*;
use crate::wire_type::*;

// -- Varint tests --

#[test]
fn test_varint_zero() {
    let mut buf = Vec::new();
    encode_varint(0, &mut buf);
    assert_eq!(buf, vec![0]);
    let (val, n) = decode_varint(&buf).unwrap();
    assert_eq!(val, 0);
    assert_eq!(n, 1);
}

#[test]
fn test_varint_one() {
    let mut buf = Vec::new();
    encode_varint(1, &mut buf);
    assert_eq!(buf, vec![1]);
    let (val, n) = decode_varint(&buf).unwrap();
    assert_eq!(val, 1);
    assert_eq!(n, 1);
}

#[test]
fn test_varint_127() {
    let mut buf = Vec::new();
    encode_varint(127, &mut buf);
    assert_eq!(buf, vec![0x7F]);
    let (val, _) = decode_varint(&buf).unwrap();
    assert_eq!(val, 127);
}

#[test]
fn test_varint_128() {
    let mut buf = Vec::new();
    encode_varint(128, &mut buf);
    assert_eq!(buf, vec![0x80, 0x01]);
    let (val, n) = decode_varint(&buf).unwrap();
    assert_eq!(val, 128);
    assert_eq!(n, 2);
}

#[test]
fn test_varint_300() {
    let mut buf = Vec::new();
    encode_varint(300, &mut buf);
    let (val, _) = decode_varint(&buf).unwrap();
    assert_eq!(val, 300);
}

#[test]
fn test_varint_large() {
    let mut buf = Vec::new();
    let large = 0xFFFF_FFFF_FFFF_FFFF_u64;
    encode_varint(large, &mut buf);
    let (val, _) = decode_varint(&buf).unwrap();
    assert_eq!(val, large);
}

#[test]
fn test_varint_max_u32() {
    let mut buf = Vec::new();
    encode_varint(u64::from(u32::MAX), &mut buf);
    let (val, _) = decode_varint(&buf).unwrap();
    assert_eq!(val, u64::from(u32::MAX));
}

#[test]
fn test_varint_empty_buffer() {
    assert_eq!(decode_varint(&[]), Err(DecodeError::BufferTooShort));
}

#[test]
fn test_varint_incomplete() {
    assert_eq!(decode_varint(&[0x80]), Err(DecodeError::BufferTooShort));
}

#[test]
fn test_varint_powers_of_two() {
    for shift in 0..63 {
        let val = 1u64 << shift;
        let mut buf = Vec::new();
        encode_varint(val, &mut buf);
        let (decoded, _) = decode_varint(&buf).unwrap();
        assert_eq!(decoded, val);
    }
}

// -- Zigzag tests --

#[test]
fn test_zigzag_i32_zero() {
    assert_eq!(zigzag_encode_i32(0), 0);
    assert_eq!(zigzag_decode_i32(0), 0);
}

#[test]
fn test_zigzag_i32_positive() {
    assert_eq!(zigzag_encode_i32(1), 2);
    assert_eq!(zigzag_decode_i32(2), 1);
}

#[test]
fn test_zigzag_i32_negative() {
    assert_eq!(zigzag_encode_i32(-1), 1);
    assert_eq!(zigzag_decode_i32(1), -1);
}

#[test]
fn test_zigzag_i32_various() {
    let values = [0, 1, -1, 2, -2, 100, -100, i32::MAX, i32::MIN];
    for &v in &values {
        assert_eq!(zigzag_decode_i32(zigzag_encode_i32(v)), v);
    }
}

#[test]
fn test_zigzag_i64_zero() {
    assert_eq!(zigzag_encode_i64(0), 0);
    assert_eq!(zigzag_decode_i64(0), 0);
}

#[test]
fn test_zigzag_i64_positive() {
    assert_eq!(zigzag_encode_i64(1), 2);
    assert_eq!(zigzag_decode_i64(2), 1);
}

#[test]
fn test_zigzag_i64_negative() {
    assert_eq!(zigzag_encode_i64(-1), 1);
    assert_eq!(zigzag_decode_i64(1), -1);
}

#[test]
fn test_zigzag_i64_various() {
    let values = [0i64, 1, -1, 2, -2, 1000, -1000, i64::MAX, i64::MIN];
    for &v in &values {
        assert_eq!(zigzag_decode_i64(zigzag_encode_i64(v)), v);
    }
}

// -- Wire Type tests --

#[test]
fn test_wire_type_from_valid() {
    assert_eq!(WireType::from_u8(0).unwrap(), WireType::Varint);
    assert_eq!(WireType::from_u8(1).unwrap(), WireType::Fixed64);
    assert_eq!(WireType::from_u8(2).unwrap(), WireType::LengthDelimited);
    assert_eq!(WireType::from_u8(5).unwrap(), WireType::Fixed32);
}

#[test]
fn test_wire_type_from_invalid() {
    assert_eq!(WireType::from_u8(6), Err(DecodeError::InvalidWireType(6)));
    assert_eq!(WireType::from_u8(7), Err(DecodeError::InvalidWireType(7)));
}

// -- Field Tag tests --

#[test]
fn test_field_tag_encode_decode() {
    let tag = FieldTag::new(1, WireType::Varint);
    let mut buf = Vec::new();
    tag.encode(&mut buf);
    let (decoded, _) = FieldTag::decode(&buf).unwrap();
    assert_eq!(decoded, tag);
}

#[test]
fn test_field_tag_large_number() {
    let tag = FieldTag::new(536_870_911, WireType::LengthDelimited);
    let mut buf = Vec::new();
    tag.encode(&mut buf);
    let (decoded, _) = FieldTag::decode(&buf).unwrap();
    assert_eq!(decoded, tag);
}

#[test]
fn test_field_tag_all_wire_types() {
    for wt in [
        WireType::Varint,
        WireType::Fixed64,
        WireType::LengthDelimited,
        WireType::Fixed32,
    ] {
        let tag = FieldTag::new(42, wt);
        let mut buf = Vec::new();
        tag.encode(&mut buf);
        let (decoded, _) = FieldTag::decode(&buf).unwrap();
        assert_eq!(decoded, tag);
    }
}

// -- Field Value tests --

#[test]
fn test_field_value_varint() {
    let val = FieldValue::Varint(42);
    let mut buf = Vec::new();
    val.encode(&mut buf);
    let (decoded, _) = FieldValue::decode(WireType::Varint, &buf).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn test_field_value_fixed32() {
    let val = FieldValue::Fixed32(0x1234_5678);
    let mut buf = Vec::new();
    val.encode(&mut buf);
    let (decoded, _) = FieldValue::decode(WireType::Fixed32, &buf).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn test_field_value_fixed64() {
    let val = FieldValue::Fixed64(0x1234_5678_9ABC_DEF0);
    let mut buf = Vec::new();
    val.encode(&mut buf);
    let (decoded, _) = FieldValue::decode(WireType::Fixed64, &buf).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn test_field_value_bytes() {
    let val = FieldValue::Bytes(vec![1, 2, 3, 4, 5]);
    let mut buf = Vec::new();
    val.encode(&mut buf);
    let (decoded, _) = FieldValue::decode(WireType::LengthDelimited, &buf).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn test_field_value_empty_bytes() {
    let val = FieldValue::Bytes(vec![]);
    let mut buf = Vec::new();
    val.encode(&mut buf);
    let (decoded, _) = FieldValue::decode(WireType::LengthDelimited, &buf).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn test_field_value_group_unsupported() {
    assert_eq!(
        FieldValue::decode(WireType::StartGroup, &[]),
        Err(DecodeError::UnsupportedWireType)
    );
}

// -- ProtoMessage tests --

#[test]
fn test_proto_message_empty() {
    let msg = ProtoMessage::new();
    let encoded = msg.encode();
    assert!(encoded.is_empty());
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    assert_eq!(decoded.field_count(), 0);
}

#[test]
fn test_proto_message_single_varint() {
    let mut msg = ProtoMessage::new();
    msg.add_varint(1, 150);
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    assert_eq!(decoded.get_field(1), vec![&FieldValue::Varint(150)]);
}

#[test]
fn test_proto_message_string() {
    let mut msg = ProtoMessage::new();
    msg.add_string(2, "hello");
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    let vals = decoded.get_field(2);
    assert_eq!(vals.len(), 1);
    match vals[0] {
        FieldValue::Bytes(b) => assert_eq!(b, b"hello"),
        _ => panic!("expected bytes"),
    }
}

#[test]
fn test_proto_message_multiple_fields() {
    let mut msg = ProtoMessage::new();
    msg.add_varint(1, 42);
    msg.add_string(2, "world");
    msg.add_fixed32(3, 99);
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    assert_eq!(decoded.field_count(), 3);
    assert_eq!(decoded.get_field(1), vec![&FieldValue::Varint(42)]);
}

#[test]
fn test_proto_message_nested() {
    let mut inner = ProtoMessage::new();
    inner.add_varint(1, 100);
    let mut outer = ProtoMessage::new();
    outer.add_message(1, &inner);
    let encoded = outer.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    let inner_bytes = decoded.get_field(1);
    assert_eq!(inner_bytes.len(), 1);
}

#[test]
fn test_proto_message_repeated_field() {
    let mut msg = ProtoMessage::new();
    msg.add_varint(1, 10);
    msg.add_varint(1, 20);
    msg.add_varint(1, 30);
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    let vals = decoded.get_field(1);
    assert_eq!(vals.len(), 3);
}

#[test]
fn test_proto_message_has_field() {
    let mut msg = ProtoMessage::new();
    msg.add_varint(1, 42);
    assert!(msg.has_field(1));
    assert!(!msg.has_field(2));
}

#[test]
fn test_proto_message_clear() {
    let mut msg = ProtoMessage::new();
    msg.add_varint(1, 42);
    msg.clear();
    assert_eq!(msg.field_count(), 0);
}

#[test]
fn test_proto_message_field_numbers() {
    let mut msg = ProtoMessage::new();
    msg.add_varint(3, 1);
    msg.add_varint(1, 2);
    msg.add_varint(2, 3);
    assert_eq!(msg.field_numbers(), vec![1, 2, 3]);
}

#[test]
fn test_proto_message_sint32() {
    let mut msg = ProtoMessage::new();
    msg.add_sint32(1, -42);
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    let vals = decoded.get_field(1);
    match vals[0] {
        FieldValue::Varint(v) => {
            assert_eq!(zigzag_decode_i32(*v as u32), -42);
        }
        _ => panic!("expected varint"),
    }
}

#[test]
fn test_proto_message_sint64() {
    let mut msg = ProtoMessage::new();
    msg.add_sint64(1, -1000);
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    let vals = decoded.get_field(1);
    match vals[0] {
        FieldValue::Varint(v) => {
            assert_eq!(zigzag_decode_i64(*v), -1000);
        }
        _ => panic!("expected varint"),
    }
}

#[test]
fn test_proto_message_fixed64() {
    let mut msg = ProtoMessage::new();
    msg.add_fixed64(1, 0xDEAD_BEEF);
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    assert_eq!(
        decoded.get_field(1),
        vec![&FieldValue::Fixed64(0xDEAD_BEEF)]
    );
}

#[test]
fn test_proto_message_bytes() {
    let mut msg = ProtoMessage::new();
    msg.add_bytes(1, vec![0xFF, 0x00, 0xAB]);
    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    assert_eq!(
        decoded.get_field(1),
        vec![&FieldValue::Bytes(vec![0xFF, 0x00, 0xAB])]
    );
}

// -- Status Code tests --

#[test]
fn test_status_code_all_values() {
    for i in 0..=16u8 {
        let code = StatusCode::from_u8(i).unwrap();
        assert_eq!(code as u8, i);
    }
}

#[test]
fn test_status_code_invalid() {
    assert!(StatusCode::from_u8(17).is_none());
    assert!(StatusCode::from_u8(255).is_none());
}

#[test]
fn test_status_code_is_ok() {
    assert!(StatusCode::Ok.is_ok());
    assert!(!StatusCode::Internal.is_ok());
}

#[test]
fn test_status_code_is_error() {
    assert!(!StatusCode::Ok.is_error());
    assert!(StatusCode::NotFound.is_error());
}

#[test]
fn test_status_code_names() {
    assert_eq!(StatusCode::Ok.name(), "OK");
    assert_eq!(StatusCode::NotFound.name(), "NOT_FOUND");
    assert_eq!(StatusCode::Internal.name(), "INTERNAL");
    assert_eq!(StatusCode::Unauthenticated.name(), "UNAUTHENTICATED");
}

#[test]
fn test_status_code_display() {
    assert_eq!(format!("{}", StatusCode::Ok), "OK");
    assert_eq!(format!("{}", StatusCode::Cancelled), "CANCELLED");
}

// -- GrpcStatus tests --

#[test]
fn test_grpc_status_ok() {
    let status = GrpcStatus::ok();
    assert!(status.is_ok());
    assert_eq!(status.code, StatusCode::Ok);
}

#[test]
fn test_grpc_status_internal() {
    let status = GrpcStatus::internal("something broke");
    assert!(!status.is_ok());
    assert_eq!(status.message, "something broke");
}

#[test]
fn test_grpc_status_not_found() {
    let status = GrpcStatus::not_found("resource missing");
    assert_eq!(status.code, StatusCode::NotFound);
}

#[test]
fn test_grpc_status_invalid_argument() {
    let status = GrpcStatus::invalid_argument("bad input");
    assert_eq!(status.code, StatusCode::InvalidArgument);
}

#[test]
fn test_grpc_status_unimplemented() {
    let status = GrpcStatus::unimplemented("not yet");
    assert_eq!(status.code, StatusCode::Unimplemented);
}

#[test]
fn test_grpc_status_unauthenticated() {
    let status = GrpcStatus::unauthenticated("no token");
    assert_eq!(status.code, StatusCode::Unauthenticated);
}

#[test]
fn test_grpc_status_permission_denied() {
    let status = GrpcStatus::permission_denied("forbidden");
    assert_eq!(status.code, StatusCode::PermissionDenied);
}

#[test]
fn test_grpc_status_unavailable() {
    let status = GrpcStatus::unavailable("try later");
    assert_eq!(status.code, StatusCode::Unavailable);
}

#[test]
fn test_grpc_status_cancelled() {
    let status = GrpcStatus::cancelled("user cancelled");
    assert_eq!(status.code, StatusCode::Cancelled);
}

#[test]
fn test_grpc_status_display_with_message() {
    let status = GrpcStatus::internal("err");
    assert_eq!(format!("{status}"), "INTERNAL: err");
}

#[test]
fn test_grpc_status_display_no_message() {
    let status = GrpcStatus::ok();
    assert_eq!(format!("{status}"), "OK");
}

#[test]
fn test_grpc_status_to_from_proto() {
    let status = GrpcStatus::not_found("missing");
    let proto = status.to_proto();
    let encoded = proto.encode();
    let decoded_proto = ProtoMessage::decode(&encoded).unwrap();
    let decoded_status = GrpcStatus::from_proto(&decoded_proto).unwrap();
    assert_eq!(decoded_status, status);
}

// -- Metadata tests --

#[test]
fn test_metadata_empty() {
    let md = Metadata::new();
    assert!(md.is_empty());
    assert_eq!(md.len(), 0);
}

#[test]
fn test_metadata_insert_text() {
    let mut md = Metadata::new();
    md.insert_text("key", "value");
    assert_eq!(md.get_text("key"), Some("value"));
}

#[test]
fn test_metadata_insert_binary() {
    let mut md = Metadata::new();
    md.insert_binary("key-bin", vec![1, 2, 3]);
    assert_eq!(md.get_binary("key-bin"), Some(&[1u8, 2, 3][..]));
}

#[test]
fn test_metadata_contains_key() {
    let mut md = Metadata::new();
    md.insert_text("auth", "token");
    assert!(md.contains_key("auth"));
    assert!(!md.contains_key("missing"));
}

#[test]
fn test_metadata_remove() {
    let mut md = Metadata::new();
    md.insert_text("key", "val");
    md.remove("key");
    assert!(!md.contains_key("key"));
}

#[test]
fn test_metadata_get_all() {
    let mut md = Metadata::new();
    md.insert_text("key", "a");
    md.insert_text("key", "b");
    assert_eq!(md.get_all("key").len(), 2);
}

#[test]
fn test_metadata_keys() {
    let mut md = Metadata::new();
    md.insert_text("a", "1");
    md.insert_text("b", "2");
    let keys = md.keys();
    assert!(keys.contains(&"a"));
    assert!(keys.contains(&"b"));
}

#[test]
fn test_metadata_merge() {
    let mut md1 = Metadata::new();
    md1.insert_text("a", "1");
    let mut md2 = Metadata::new();
    md2.insert_text("b", "2");
    md1.merge(&md2);
    assert_eq!(md1.len(), 2);
}

#[test]
fn test_metadata_iter() {
    let mut md = Metadata::new();
    md.insert_text("x", "y");
    let items: Vec<_> = md.iter().collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, "x");
}

#[test]
fn test_metadata_value_is_text() {
    let v = MetadataValue::Text("hello".into());
    assert!(v.is_text());
    assert!(!v.is_binary());
}

#[test]
fn test_metadata_value_is_binary() {
    let v = MetadataValue::Binary(vec![1]);
    assert!(v.is_binary());
    assert!(!v.is_text());
}

#[test]
fn test_metadata_value_as_text() {
    let v = MetadataValue::Text("hello".into());
    assert_eq!(v.as_text(), Some("hello"));
    let v2 = MetadataValue::Binary(vec![]);
    assert_eq!(v2.as_text(), None);
}

#[test]
fn test_metadata_value_as_binary() {
    let v = MetadataValue::Binary(vec![1, 2]);
    assert_eq!(v.as_binary(), Some(&[1u8, 2][..]));
    let v2 = MetadataValue::Text("x".into());
    assert_eq!(v2.as_binary(), None);
}

#[test]
fn test_metadata_to_proto() {
    let mut md = Metadata::new();
    md.insert_text("key", "val");
    let proto = md.to_proto();
    assert_eq!(proto.field_count(), 1);
}

// -- Method Type tests --

#[test]
fn test_method_type_unary() {
    assert!(!MethodType::Unary.is_client_streaming());
    assert!(!MethodType::Unary.is_server_streaming());
}

#[test]
fn test_method_type_client_streaming() {
    assert!(MethodType::ClientStreaming.is_client_streaming());
    assert!(!MethodType::ClientStreaming.is_server_streaming());
}

#[test]
fn test_method_type_server_streaming() {
    assert!(!MethodType::ServerStreaming.is_client_streaming());
    assert!(MethodType::ServerStreaming.is_server_streaming());
}

#[test]
fn test_method_type_bidi_streaming() {
    assert!(MethodType::BidiStreaming.is_client_streaming());
    assert!(MethodType::BidiStreaming.is_server_streaming());
}

#[test]
fn test_method_type_display() {
    assert_eq!(format!("{}", MethodType::Unary), "unary");
    assert_eq!(format!("{}", MethodType::BidiStreaming), "bidi_streaming");
}

// -- Service Descriptor tests --

#[test]
fn test_service_descriptor_new() {
    let svc = ServiceDescriptor::new("Greeter", "helloworld");
    assert_eq!(svc.full_path(), "/helloworld.Greeter");
}

#[test]
fn test_service_descriptor_add_method() {
    let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
    svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
    assert_eq!(svc.method_count(), 1);
}

#[test]
fn test_service_descriptor_find_method() {
    let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
    svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
    let method = svc.find_method("SayHello").unwrap();
    assert_eq!(method.method_type, MethodType::Unary);
    assert!(svc.find_method("NotExist").is_none());
}

#[test]
fn test_method_descriptor_full_path() {
    let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
    svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
    let method = svc.find_method("SayHello").unwrap();
    assert_eq!(method.full_path, "/helloworld.Greeter/SayHello");
}

// -- gRPC Frame tests --

#[test]
fn test_grpc_frame_encode_decode() {
    let data = b"hello grpc";
    let frame = encode_grpc_frame(data, false);
    let (compressed, payload, consumed) = decode_grpc_frame(&frame).unwrap();
    assert!(!compressed);
    assert_eq!(payload, data);
    assert_eq!(consumed, frame.len());
}

#[test]
fn test_grpc_frame_compressed() {
    let data = b"compressed data";
    let frame = encode_grpc_frame(data, true);
    let (compressed, payload, _) = decode_grpc_frame(&frame).unwrap();
    assert!(compressed);
    assert_eq!(payload, data);
}

#[test]
fn test_grpc_frame_empty() {
    let frame = encode_grpc_frame(&[], false);
    let (compressed, payload, consumed) = decode_grpc_frame(&frame).unwrap();
    assert!(!compressed);
    assert!(payload.is_empty());
    assert_eq!(consumed, 5);
}

#[test]
fn test_grpc_frame_too_short() {
    assert_eq!(
        decode_grpc_frame(&[0, 0, 0]),
        Err(DecodeError::BufferTooShort)
    );
}

// -- Request / Response tests --

#[test]
fn test_request_new() {
    let req = Request::new("/pkg.Svc/Method", vec![1, 2, 3]);
    assert_eq!(req.method_path, "/pkg.Svc/Method");
    assert_eq!(req.payload, vec![1, 2, 3]);
}

#[test]
fn test_request_metadata() {
    let mut req = Request::new("/pkg.Svc/Method", vec![]);
    req.add_metadata("auth", "bearer token");
    assert_eq!(req.metadata.get_text("auth"), Some("bearer token"));
}

#[test]
fn test_request_set_metadata() {
    let mut req = Request::new("/test", vec![]);
    let mut md = Metadata::new();
    md.insert_text("k", "v");
    req.set_metadata(md);
    assert_eq!(req.metadata.get_text("k"), Some("v"));
}

#[test]
fn test_request_to_grpc_frame() {
    let req = Request::new("/test", vec![0xAB]);
    let frame = req.to_grpc_frame();
    let (_, payload, _) = decode_grpc_frame(&frame).unwrap();
    assert_eq!(payload, vec![0xAB]);
}

#[test]
fn test_response_ok() {
    let resp = Response::ok(vec![1, 2, 3]);
    assert!(resp.is_ok());
    assert_eq!(resp.payload, vec![1, 2, 3]);
}

#[test]
fn test_response_error() {
    let resp = Response::error(GrpcStatus::internal("fail"));
    assert!(!resp.is_ok());
    assert!(resp.payload.is_empty());
}

#[test]
fn test_response_to_grpc_frame() {
    let resp = Response::ok(vec![42]);
    let frame = resp.to_grpc_frame();
    let (_, payload, _) = decode_grpc_frame(&frame).unwrap();
    assert_eq!(payload, vec![42]);
}

// -- MessageStream tests --

#[test]
fn test_stream_new() {
    let stream = MessageStream::new();
    assert_eq!(stream.total(), 0);
    assert!(!stream.has_next());
}

#[test]
fn test_stream_push_next() {
    let mut stream = MessageStream::new();
    stream.push(vec![1]);
    stream.push(vec![2]);
    assert_eq!(stream.total(), 2);
    assert_eq!(stream.remaining(), 2);
    assert_eq!(stream.next_message(), Some(vec![1]));
    assert_eq!(stream.remaining(), 1);
    assert_eq!(stream.next_message(), Some(vec![2]));
    assert_eq!(stream.next_message(), None);
}

#[test]
fn test_stream_close() {
    let mut stream = MessageStream::new();
    assert!(!stream.is_closed());
    stream.close();
    assert!(stream.is_closed());
}

#[test]
fn test_stream_reset() {
    let mut stream = MessageStream::new();
    stream.push(vec![1]);
    stream.next_message();
    assert!(!stream.has_next());
    stream.reset();
    assert!(stream.has_next());
}

#[test]
fn test_stream_grpc_frames_roundtrip() {
    let mut stream = MessageStream::new();
    stream.push(vec![10, 20]);
    stream.push(vec![30, 40, 50]);
    let frames = stream.to_grpc_frames();
    let decoded = MessageStream::from_grpc_frames(&frames).unwrap();
    assert_eq!(decoded.total(), 2);
}

// -- Channel tests --

#[test]
fn test_channel_new() {
    let ch = Channel::new("localhost:50051");
    assert_eq!(ch.target, "localhost:50051");
    assert_eq!(ch.state, ChannelState::Idle);
}

#[test]
fn test_channel_connect_ready() {
    let mut ch = Channel::new("localhost:50051");
    ch.connect();
    assert_eq!(ch.state, ChannelState::Connecting);
    ch.set_ready();
    assert!(ch.is_ready());
}

#[test]
fn test_channel_transient_failure() {
    let mut ch = Channel::new("localhost:50051");
    ch.set_transient_failure();
    assert_eq!(ch.state, ChannelState::TransientFailure);
}

#[test]
fn test_channel_shutdown() {
    let mut ch = Channel::new("localhost:50051");
    ch.shutdown();
    assert!(ch.is_shutdown());
    ch.connect(); // should not change state
    assert!(ch.is_shutdown());
}

#[test]
fn test_channel_register_service() {
    let mut ch = Channel::new("localhost:50051");
    let svc = ServiceDescriptor::new("Greeter", "pkg");
    ch.register_service(svc);
    assert_eq!(ch.service_count(), 1);
    assert!(ch.find_service("/pkg.Greeter").is_some());
}

#[test]
fn test_channel_default_metadata() {
    let mut ch = Channel::new("localhost:50051");
    let mut md = Metadata::new();
    md.insert_text("auth", "tok");
    ch.set_default_metadata(md);
    assert_eq!(ch.metadata.get_text("auth"), Some("tok"));
}

#[test]
fn test_channel_state_display() {
    assert_eq!(format!("{}", ChannelState::Idle), "IDLE");
    assert_eq!(format!("{}", ChannelState::Ready), "READY");
    assert_eq!(format!("{}", ChannelState::Shutdown), "SHUTDOWN");
}

// -- ChannelPool tests --

#[test]
fn test_channel_pool_empty() {
    let mut pool = ChannelPool::new();
    assert!(pool.is_empty());
    assert!(pool.next_ready().is_none());
}

#[test]
fn test_channel_pool_add() {
    let mut pool = ChannelPool::new();
    pool.add(Channel::new("a"));
    pool.add(Channel::new("b"));
    assert_eq!(pool.len(), 2);
}

#[test]
fn test_channel_pool_next_ready() {
    let mut pool = ChannelPool::new();
    let mut ch1 = Channel::new("a");
    ch1.connect();
    ch1.set_ready();
    pool.add(ch1);
    let ch = pool.next_ready().unwrap();
    assert_eq!(ch.target, "a");
}

#[test]
fn test_channel_pool_no_ready() {
    let mut pool = ChannelPool::new();
    pool.add(Channel::new("a"));
    assert!(pool.next_ready().is_none());
}

#[test]
fn test_channel_pool_ready_count() {
    let mut pool = ChannelPool::new();
    let mut ch = Channel::new("a");
    ch.connect();
    ch.set_ready();
    pool.add(ch);
    pool.add(Channel::new("b"));
    assert_eq!(pool.ready_count(), 1);
}

#[test]
fn test_channel_pool_shutdown_all() {
    let mut pool = ChannelPool::new();
    pool.add(Channel::new("a"));
    pool.add(Channel::new("b"));
    pool.shutdown_all();
    assert_eq!(pool.ready_count(), 0);
}

// -- ServiceRouter tests --

#[test]
fn test_router_new() {
    let router = ServiceRouter::new();
    assert_eq!(router.method_count(), 0);
}

#[test]
fn test_router_register_resolve() {
    let mut router = ServiceRouter::new();
    let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
    svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
    router.register(svc);
    let (_, method) = router.resolve("/helloworld.Greeter/SayHello").unwrap();
    assert_eq!(method.name, "SayHello");
}

#[test]
fn test_router_resolve_not_found() {
    let router = ServiceRouter::new();
    assert!(router.resolve("/pkg.Svc/Method").is_none());
}

#[test]
fn test_router_service_paths() {
    let mut router = ServiceRouter::new();
    router.register(ServiceDescriptor::new("A", "pkg"));
    router.register(ServiceDescriptor::new("B", "pkg"));
    let paths = router.service_paths();
    assert_eq!(paths.len(), 2);
}

#[test]
fn test_router_method_count() {
    let mut router = ServiceRouter::new();
    let mut svc = ServiceDescriptor::new("Svc", "pkg");
    svc.add_method("M1", MethodType::Unary, "Req", "Res");
    svc.add_method("M2", MethodType::ServerStreaming, "Req", "Res");
    router.register(svc);
    assert_eq!(router.method_count(), 2);
}

// -- Decode Error tests --

#[test]
fn test_decode_error_display() {
    assert_eq!(
        format!("{}", DecodeError::BufferTooShort),
        "buffer too short"
    );
    assert_eq!(format!("{}", DecodeError::VarintTooLong), "varint too long");
    assert_eq!(
        format!("{}", DecodeError::InvalidWireType(99)),
        "invalid wire type: 99"
    );
    assert_eq!(
        format!("{}", DecodeError::InvalidFieldNumber),
        "invalid field number (0)"
    );
    assert_eq!(
        format!("{}", DecodeError::UnsupportedWireType),
        "unsupported wire type (group)"
    );
}

#[test]
fn test_decode_error_is_std_error() {
    let err: Box<dyn std::error::Error> = Box::new(DecodeError::BufferTooShort);
    assert_eq!(err.to_string(), "buffer too short");
}

// -- Integration / roundtrip tests --

#[test]
fn test_full_message_roundtrip() {
    let mut msg = ProtoMessage::new();
    msg.add_varint(1, 42);
    msg.add_string(2, "alice");
    msg.add_fixed32(3, 0xDEAD);
    msg.add_fixed64(4, 0xBEEF);
    msg.add_bytes(5, vec![0xCA, 0xFE]);
    msg.add_sint32(6, -99);
    msg.add_sint64(7, -999_999);

    let encoded = msg.encode();
    let decoded = ProtoMessage::decode(&encoded).unwrap();
    assert_eq!(decoded.field_count(), 7);
    assert_eq!(decoded.get_field(1), vec![&FieldValue::Varint(42)]);
}

#[test]
fn test_grpc_request_response_roundtrip() {
    let mut inner_msg = ProtoMessage::new();
    inner_msg.add_string(1, "Hello");
    let payload = inner_msg.encode();

    let mut req = Request::new("/helloworld.Greeter/SayHello", payload.clone());
    req.add_metadata("authorization", "Bearer xyz");

    let frame = req.to_grpc_frame();
    let (_, decoded_payload, _) = decode_grpc_frame(&frame).unwrap();
    let decoded_msg = ProtoMessage::decode(&decoded_payload).unwrap();
    assert!(decoded_msg.has_field(1));

    let mut reply = ProtoMessage::new();
    reply.add_string(1, "Hello back!");
    let resp = Response::ok(reply.encode());
    assert!(resp.is_ok());
}

#[test]
fn test_streaming_rpc_simulation() {
    let mut svc = ServiceDescriptor::new("ChatService", "chat");
    svc.add_method(
        "StreamChat",
        MethodType::BidiStreaming,
        "ChatMessage",
        "ChatMessage",
    );
    let method = svc.find_method("StreamChat").unwrap();
    assert!(method.method_type.is_client_streaming());
    assert!(method.method_type.is_server_streaming());

    let mut client_stream = MessageStream::new();
    for i in 0..5 {
        let mut msg = ProtoMessage::new();
        msg.add_string(1, &format!("msg_{i}"));
        client_stream.push(msg.encode());
    }

    let frames = client_stream.to_grpc_frames();
    let decoded = MessageStream::from_grpc_frames(&frames).unwrap();
    assert_eq!(decoded.total(), 5);
}

#[test]
fn test_channel_full_lifecycle() {
    let mut ch = Channel::new("grpc.example.com:443");
    assert_eq!(ch.state, ChannelState::Idle);
    ch.connect();
    assert_eq!(ch.state, ChannelState::Connecting);
    ch.set_ready();
    assert_eq!(ch.state, ChannelState::Ready);

    let mut svc = ServiceDescriptor::new("TestSvc", "test");
    svc.add_method("DoStuff", MethodType::Unary, "Req", "Res");
    ch.register_service(svc);

    assert_eq!(ch.service_count(), 1);
    ch.shutdown();
    assert!(ch.is_shutdown());
}
