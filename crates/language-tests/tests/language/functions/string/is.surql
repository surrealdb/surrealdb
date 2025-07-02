/**
[test]

# string::is::alphanum results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is::alpha results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is::ascii results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is::domain results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::email results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::hexadecimal results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::ip results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::ipv4 results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::ipv6 results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::latitude results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::longitude results

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

# string::is::numeric results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

# string::is::uuid results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

# string::is::uuid results

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

string::is::alphanum("12345");
string::is::alphanum("y%*");
string::is::alphanum("");

string::is::alpha("abc");
string::is::alpha("1234");
string::is::alpha("");

string::is::ascii("abc");
string::is::ascii("中国");
string::is::ascii("");

string::is::domain("食狮.中国");
string::is::domain("example-.com");

string::is::email("user@[fd79:cdcb:38cc:9dd:f686:e06d:32f3:c123]");
string::is::email("john..doe@example.com");

string::is::hexadecimal("00FF00");
string::is::hexadecimal("SurrealDB");

string::is::ip("127.0.0.1");
string::is::ip("127.0.0");

string::is::ipv4("127.0.0.1");
string::is::ipv4("127.0.0");

string::is::ipv6("::1");
string::is::ipv6("200t:db8::");

string::is::latitude("-0.118092");
string::is::latitude("12345");

string::is::longitude("91.509865");
string::is::longitude("-91.509865");
string::is::longitude("-180.00000");
string::is::longitude("180.00000");
string::is::longitude("-180.00001");
string::is::longitude("180.00001");
string::is::longitude("12345");

string::is::numeric("12345");
string::is::numeric("abcde");
string::is::numeric("");

string::is::semver("1.0.0");
string::is::semver("1.0");

string::is::uuid("123e4567-e89b-12d3-a456-426614174000");
string::is::uuid("foo-bar");