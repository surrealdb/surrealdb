/**
[test]

# string::is_alphanum results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is_alpha results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is_ascii results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is_domain results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_email results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_hexadecimal results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_ip results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_ipv4 results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_ipv6 results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_latitude results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_longitude results

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is_numeric results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is_uuid results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is_uuid results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

string::is_alphanum("12345");
string::is_alphanum("y%*");
string::is_alphanum("");

string::is_alpha("abc");
string::is_alpha("1234");
string::is_alpha("");

string::is_ascii("abc");
string::is_ascii("中国");
string::is_ascii("");

string::is_domain("食狮.中国");
string::is_domain("example-.com");

string::is_email("user@[fd79:cdcb:38cc:9dd:f686:e06d:32f3:c123]");
string::is_email("john..doe@example.com");

string::is_hexadecimal("00FF00");
string::is_hexadecimal("SurrealDB");

string::is_ip("127.0.0.1");
string::is_ip("127.0.0");

string::is_ipv4("127.0.0.1");
string::is_ipv4("127.0.0");

string::is_ipv6("::1");
string::is_ipv6("200t:db8::");

string::is_latitude("-0.118092");
string::is_latitude("12345");

string::is_longitude("91.509865");
string::is_longitude("-91.509865");
string::is_longitude("-180.00000");
string::is_longitude("180.00000");
string::is_longitude("-180.00001");
string::is_longitude("180.00001");
string::is_longitude("12345");

string::is_numeric("12345");
string::is_numeric("abcde");
string::is_numeric("");

string::is_semver("1.0.0");
string::is_semver("1.0");

string::is_uuid("123e4567-e89b-12d3-a456-426614174000");
string::is_uuid("foo-bar");
