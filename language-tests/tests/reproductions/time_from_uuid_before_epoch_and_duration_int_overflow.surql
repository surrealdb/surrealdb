/**
[env]
namespace = true
database = true

[test]
reason = """
`time::from_uuid` decodes a v1 or v6 UUID dated before 1970-01-01 to that
instant rather than wrapping the Gregorian-to-Unix offset. A UUID version
without a timestamp is refused. Multiplying a duration by an integer that
overflows the duration is the ordinary arithmetic-overflow error.
"""

[[test.results]]
value = "d'1900-01-01T00:00:00Z'"

[[test.results]]
value = "d'1582-10-15T00:00:00Z'"

[[test.results]]
value = "d'2006-06-10T10:48:31.013993Z'"

[[test.results]]
value = "d'1969-12-31T23:59:59.999999999Z'"

[[test.results]]
value = "d'1900-01-01T00:00:00Z'"

[[test.results]]
value = "d'1970-01-01T00:00:00Z'"

[[test.results]]
error = "Incorrect arguments for function time::from_uuid(). The first argument must be a v1, v6 or v7 UUID."

[[test.results]]
value = "d'2024-09-23T19:55:34.933Z'"

[[test.results]]
error = "Failed to compute: \"1h * 9223372036854775807\", as the operation results in an arithmetic overflow."

[[test.results]]
value = "4d4h"
*/
RETURN time::from_uuid(u"3c230000-a32f-1163-8000-000000000000");
RETURN time::from_uuid(u"00000000-0000-1000-8000-000000000000");
RETURN time::from_uuid(u"a8098c1a-f86e-11da-bd1a-00112444be1e");
RETURN time::from_uuid(u"13814000-1dd2-11b2-8000-000000000000" ) - 1ns;
RETURN time::from_uuid(u"163a32f3-c230-6000-8000-000000000000");
RETURN time::from_uuid(u"13814000-1dd2-11b2-8000-000000000000");
RETURN time::from_uuid(u"d9428888-122b-4e60-b2d6-0e1a2a9f6e7a");
RETURN time::from_uuid(u'01922074-2295-7cf6-906f-bcd0810639b0');
RETURN 1h * 9223372036854775807;
RETURN 1h * 100;
