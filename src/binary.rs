use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use postgres_types::{IsNull, ToSql, Type};

/// The 11-byte PostgreSQL binary COPY signature.
const PG_BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Write the 19-byte PostgreSQL binary COPY file header into `buf`.
///
/// Layout:
///   11 bytes  magic signature (PGCOPY\n\xff\r\n\0)
///    4 bytes  flags word (0 = no OIDs)
///    4 bytes  header extension area length (0 = no extension)
pub fn write_file_header(buf: &mut Vec<u8>) {
    buf.extend_from_slice(PG_BINARY_SIGNATURE);
    buf.extend_from_slice(&0_u32.to_be_bytes()); // flags
    buf.extend_from_slice(&0_u32.to_be_bytes()); // header extension length
}

/// Write the 2-byte file trailer into `buf` (-1 as big-endian int16).
/// This marks the end of tuple data and is the only thing needed to close
/// the binary COPY stream; no `\.` text sentinel is written.
pub fn write_file_trailer(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(-1_i16).to_be_bytes());
}

/// Write the 2-byte tuple header (field count) for one row.
pub fn write_tuple_header(buf: &mut Vec<u8>, field_count: u16) {
    buf.extend_from_slice(&field_count.to_be_bytes());
}

/// Write the NULL sentinel (4-byte -1 as big-endian int32).
#[inline]
pub fn write_null(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(-1_i32).to_be_bytes());
}

/// Encode a column value in PostgreSQL binary wire format and append to `buf`.
///
/// An empty `value` is treated as SQL NULL (writes the 4-byte -1 marker).
/// The `datatype` string is matched against common PostgreSQL type names;
/// unrecognised types fall back to plain UTF-8 text encoding.
pub fn encode_field(buf: &mut Vec<u8>, value: &str, datatype: &str) {
    if value.is_empty() {
        write_null(buf);
        return;
    }

    match datatype.trim() {
        // ── Integer family ────────────────────────────────────────────────
        "smallint" | "int2" | "smallserial" => match value.parse::<i16>() {
            Ok(v) => append_sql(buf, &v, &Type::INT2),
            Err(_) => write_null(buf),
        },
        "integer" | "int" | "int4" | "serial" => match value.parse::<i32>() {
            Ok(v) => append_sql(buf, &v, &Type::INT4),
            Err(_) => write_null(buf),
        },
        "bigint" | "int8" | "bigserial" => match value.parse::<i64>() {
            Ok(v) => append_sql(buf, &v, &Type::INT8),
            Err(_) => write_null(buf),
        },

        // ── Floating-point family ─────────────────────────────────────────
        "real" | "float4" => match value.parse::<f32>() {
            Ok(v) => append_sql(buf, &v, &Type::FLOAT4),
            Err(_) => write_null(buf),
        },
        "double precision" | "float8" | "float" => match value.parse::<f64>() {
            Ok(v) => append_sql(buf, &v, &Type::FLOAT8),
            Err(_) => write_null(buf),
        },

        // ── Boolean ───────────────────────────────────────────────────────
        "boolean" | "bool" => {
            let v = matches!(
                value.to_ascii_lowercase().as_str(),
                "true" | "t" | "yes" | "on" | "1"
            );
            append_sql(buf, &v, &Type::BOOL);
        }

        // ── Date / time family ────────────────────────────────────────────
        "date" => {
            let parsed = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .or_else(|_| NaiveDate::parse_from_str(value, "%d-%m-%Y"))
                .or_else(|_| NaiveDate::parse_from_str(value, "%Y%m%d"));
            match parsed {
                Ok(v) => append_sql(buf, &v, &Type::DATE),
                Err(_) => write_null(buf),
            }
        }
        "timestamp" | "timestamp without time zone" => {
            let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"));
            match parsed {
                Ok(v) => append_sql(buf, &v, &Type::TIMESTAMP),
                Err(_) => write_null(buf),
            }
        }
        "timestamptz" | "timestamp with time zone" => {
            let parsed: Result<DateTime<Utc>, _> = DateTime::parse_from_rfc3339(value)
                .map(|dt| dt.with_timezone(&Utc))
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
                        .map(|ndt| ndt.and_utc())
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                        .map(|ndt| ndt.and_utc())
                });
            match parsed {
                Ok(v) => append_sql(buf, &v, &Type::TIMESTAMPTZ),
                Err(_) => write_null(buf),
            }
        }

        // ── Geometric types ────────────────────────────────────────────────
        "point" => match parse_point(value) {
            Some((x, y)) => {
                buf.extend_from_slice(&16_i32.to_be_bytes());
                buf.extend_from_slice(&x.to_be_bytes());
                buf.extend_from_slice(&y.to_be_bytes());
            }
            None => write_null(buf),
        },

        // ── Text and everything else ──────────────────────────────────────
        // For text, varchar, character varying, unknown types, and geometry
        // coordinate strings produced by gml-to-coord, write raw UTF-8 bytes
        // with a 4-byte big-endian length prefix.
        _ => encode_text(buf, value),
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Write a length-prefixed field using the `postgres-types` `ToSql` encoder.
/// Falls back to NULL on any encoding error.
fn append_sql<T: ToSql>(buf: &mut Vec<u8>, val: &T, ty: &Type) {
    let mut bytes = BytesMut::new();
    match val.to_sql(ty, &mut bytes) {
        Ok(IsNull::No) => {
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(&bytes);
        }
        Ok(IsNull::Yes) => write_null(buf),
        Err(_) => write_null(buf),
    }
}

/// Write raw UTF-8 bytes with a 4-byte big-endian length prefix.
/// Used for text types and as the fallback for unknown types.
#[inline]
fn encode_text(buf: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    buf.extend_from_slice(bytes);
}

/// Parse a PostgreSQL point textual representation into an `(x, y)` pair.
/// Accepted forms: `x,y` and `(x,y)` with optional surrounding whitespace.
fn parse_point(value: &str) -> Option<(f64, f64)> {
    let raw = value.trim();
    let trimmed = raw
        .strip_prefix('(')
        .and_then(|v| v.strip_suffix(')'))
        .unwrap_or(raw);
    let (x, y) = trimmed.split_once(',')?;
    let x = x.trim().parse::<f64>().ok()?;
    let y = y.trim().parse::<f64>().ok()?;
    Some((x, y))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn read_i16(buf: &[u8]) -> i16 { i16::from_be_bytes(buf[..2].try_into().unwrap()) }
    fn read_i32(buf: &[u8]) -> i32 { i32::from_be_bytes(buf[..4].try_into().unwrap()) }
    fn read_i64(buf: &[u8]) -> i64 { i64::from_be_bytes(buf[..8].try_into().unwrap()) }

    // ── File-level framing ────────────────────────────────────────────────

    #[test]
    fn file_header_is_19_bytes_with_correct_signature() {
        let mut buf = Vec::new();
        write_file_header(&mut buf);
        assert_eq!(buf.len(), 19);
        assert_eq!(&buf[0..11], b"PGCOPY\n\xff\r\n\0");
        assert_eq!(read_i32(&buf[11..]), 0, "flags must be 0");
        assert_eq!(read_i32(&buf[15..]), 0, "extension length must be 0");
    }

    #[test]
    fn file_trailer_is_minus_one_i16() {
        let mut buf = Vec::new();
        write_file_trailer(&mut buf);
        assert_eq!(read_i16(&buf), -1_i16);
    }

    #[test]
    fn tuple_header_encodes_field_count() {
        let mut buf = Vec::new();
        write_tuple_header(&mut buf, 5);
        assert_eq!(read_i16(&buf[..2]), 5_i16);
    }

    // ── NULL ─────────────────────────────────────────────────────────────

    #[test]
    fn empty_value_encodes_as_null() {
        for ty in &["integer", "text", "date", "boolean", "bigint", "double precision"] {
            let mut buf = Vec::new();
            encode_field(&mut buf, "", ty);
            assert_eq!(read_i32(&buf), -1, "NULL sentinel for type {ty}");
        }
    }

    #[test]
    fn unparseable_integer_encodes_as_null() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "not_a_number", "integer");
        assert_eq!(read_i32(&buf), -1);
    }

    // ── Text / fallback ───────────────────────────────────────────────────

    #[test]
    fn text_encodes_utf8_with_length_prefix() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "hello", "text");
        assert_eq!(read_i32(&buf[0..4]), 5);
        assert_eq!(&buf[4..], b"hello");
    }

    #[test]
    fn varchar_encodes_like_text() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "abc", "varchar");
        assert_eq!(read_i32(&buf[0..4]), 3);
        assert_eq!(&buf[4..], b"abc");
    }

    #[test]
    fn unknown_type_falls_back_to_text() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "5.123,52.456", "geometry");
        assert_eq!(read_i32(&buf[0..4]), 12);
        assert_eq!(&buf[4..], b"5.123,52.456");
    }

    // ── Integer family ────────────────────────────────────────────────────

    #[test]
    fn integer_encodes_as_4_byte_be() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "42", "integer");
        assert_eq!(read_i32(&buf[0..4]), 4, "length");
        assert_eq!(read_i32(&buf[4..8]), 42, "value");
    }

    #[test]
    fn negative_integer_roundtrips() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "-1", "integer");
        assert_eq!(read_i32(&buf[4..8]), -1);
    }

    #[test]
    fn bigint_encodes_as_8_byte_be() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "9999999999", "bigint");
        assert_eq!(read_i32(&buf[0..4]), 8, "length");
        assert_eq!(read_i64(&buf[4..12]), 9_999_999_999_i64, "value");
    }

    #[test]
    fn smallint_encodes_as_2_byte_be() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "100", "smallint");
        assert_eq!(read_i32(&buf[0..4]), 2, "length");
        assert_eq!(read_i16(&buf[4..6]), 100_i16, "value");
    }

    // ── Floating-point ────────────────────────────────────────────────────

    #[test]
    fn float8_roundtrips() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "3.14", "double precision");
        assert_eq!(read_i32(&buf[0..4]), 8, "length");
        let v = f64::from_be_bytes(buf[4..12].try_into().unwrap());
        assert!((v - 3.14).abs() < 1e-10);
    }

    #[test]
    fn float4_roundtrips() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "1.5", "real");
        assert_eq!(read_i32(&buf[0..4]), 4, "length");
        let v = f32::from_be_bytes(buf[4..8].try_into().unwrap());
        assert!((v - 1.5_f32).abs() < 1e-6);
    }

    // ── Boolean ───────────────────────────────────────────────────────────

    #[test]
    fn boolean_true_variants() {
        for s in &["true", "t", "yes", "on", "1", "True", "YES"] {
            let mut buf = Vec::new();
            encode_field(&mut buf, s, "boolean");
            assert_eq!(read_i32(&buf[0..4]), 1, "length for {s}");
            assert_eq!(buf[4], 1u8, "true byte for {s}");
        }
    }

    #[test]
    fn boolean_false_variants() {
        for s in &["false", "f", "no", "off", "0"] {
            let mut buf = Vec::new();
            encode_field(&mut buf, s, "bool");
            assert_eq!(read_i32(&buf[0..4]), 1, "length for {s}");
            assert_eq!(buf[4], 0u8, "false byte for {s}");
        }
    }

    // ── Date / time ───────────────────────────────────────────────────────

    #[test]
    fn date_pg_epoch_is_zero() {
        // PostgreSQL date epoch is 2000-01-01 = day 0
        let mut buf = Vec::new();
        encode_field(&mut buf, "2000-01-01", "date");
        assert_eq!(read_i32(&buf[0..4]), 4, "length");
        assert_eq!(read_i32(&buf[4..8]), 0, "epoch day");
    }

    #[test]
    fn date_one_day_after_epoch() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "2000-01-02", "date");
        assert_eq!(read_i32(&buf[4..8]), 1);
    }

    #[test]
    fn date_before_epoch_is_negative() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "1999-12-31", "date");
        assert_eq!(read_i32(&buf[4..8]), -1);
    }

    #[test]
    fn timestamp_pg_epoch_is_zero_microseconds() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "2000-01-01 00:00:00", "timestamp");
        assert_eq!(read_i32(&buf[0..4]), 8, "length");
        assert_eq!(read_i64(&buf[4..12]), 0, "epoch microseconds");
    }

    #[test]
    fn timestamp_one_second_after_epoch() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "2000-01-01 00:00:01", "timestamp");
        assert_eq!(read_i64(&buf[4..12]), 1_000_000_i64); // 1s = 1_000_000 µs
    }

    #[test]
    fn timestamptz_rfc3339_epoch() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "2000-01-01T00:00:00Z", "timestamptz");
        assert_eq!(read_i32(&buf[0..4]), 8, "length");
        assert_eq!(read_i64(&buf[4..12]), 0, "epoch microseconds");
    }

    #[test]
    fn point_encodes_as_two_float8_values() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "6.87376596,53.31873138", "point");
        assert_eq!(read_i32(&buf[0..4]), 16, "length");
        let x = f64::from_be_bytes(buf[4..12].try_into().unwrap());
        let y = f64::from_be_bytes(buf[12..20].try_into().unwrap());
        assert!((x - 6.87376596).abs() < 1e-12);
        assert!((y - 53.31873138).abs() < 1e-12);
    }

    #[test]
    fn point_invalid_value_encodes_as_null() {
        let mut buf = Vec::new();
        encode_field(&mut buf, "not-a-point", "point");
        assert_eq!(read_i32(&buf[0..4]), -1);
    }
}

