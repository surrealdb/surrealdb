/**
[test]

[[test.results]]
value = "true"

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
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/
string::contains("", "");
string::contains("a", "");
string::contains("abcdefg", "");
string::contains("abcdefg", "bcd");
string::contains("abcdefg", "abcd");
string::contains("abcdefg", "xxabcd");
string::contains("abcdefg", "hij");
string::contains("ประเทศไทย中华", "ประเ");
string::contains("ประเทศไทย中华", "ะเ");
string::contains("ประเทศไทย中华", "ไท华");
string::contains("1234567ah012345678901ah", "hah");
string::contains("00abc01234567890123456789abc", "bcabc");
string::contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaba");
string::contains("* \t", " ");
string::contains("* \t", "?");
string::contains("好世界", "世");
string::contains("好世界", "你好");