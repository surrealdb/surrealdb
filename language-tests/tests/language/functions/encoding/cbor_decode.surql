/**
[test]
reason = "Test encoding::cbor::decode with various value types via roundtrip"

[[test.results]]
value = "{ foo: 'bar' }"

[[test.results]]
value = "'hello'"

[[test.results]]
value = "''"

[[test.results]]
value = "42"

[[test.results]]
value = "-7"

[[test.results]]
value = "3.14f"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "NULL"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{  }"

[[test.results]]
value = "{ user: { name: 'Tobie', tags: ['admin', 'user'] } }"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
error = "Incorrect arguments for function encoding::cbor::decode(). invalid cbor"

[[test.results]]
error = "Incorrect arguments for function encoding::cbor::decode(). invalid cbor"

[[test.results]]
error = "Incorrect arguments for function encoding::cbor::decode(). invalid cbor"

[[test.results]]
error = "Incorrect arguments for function encoding::cbor::decode(). invalid cbor"
*/

encoding::cbor::decode(b"A163666F6F63626172");
encoding::cbor::decode(encoding::cbor::encode("hello"));
encoding::cbor::decode(encoding::cbor::encode(""));
encoding::cbor::decode(encoding::cbor::encode(42));
encoding::cbor::decode(encoding::cbor::encode(-7));
encoding::cbor::decode(encoding::cbor::encode(3.14));
encoding::cbor::decode(encoding::cbor::encode(true));
encoding::cbor::decode(encoding::cbor::encode(false));
encoding::cbor::decode(encoding::cbor::encode(NULL));
encoding::cbor::decode(encoding::cbor::encode(NONE));
encoding::cbor::decode(encoding::cbor::encode([1, 2, 3]));
encoding::cbor::decode(encoding::cbor::encode([]));
encoding::cbor::decode(encoding::cbor::encode({}));
encoding::cbor::decode(encoding::cbor::encode({ user: { name: "Tobie", tags: ["admin", "user"] } }));
encoding::cbor::decode(encoding::cbor::encode([{ id: 1 }, { id: 2 }]));
-- Tags 0, 9 and 13 (a datetime, uuid and duration as text) are refused
encoding::cbor::decode(b"C06161");
encoding::cbor::decode(b"C96161");
encoding::cbor::decode(b"CD6161");
-- An integer outside the signed 64-bit range is refused
encoding::cbor::decode(b"1BFFFFFFFFFFFFFFFF");
