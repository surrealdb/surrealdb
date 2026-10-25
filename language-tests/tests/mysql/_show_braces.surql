/**
[test]
reason = "Tables whose names hold braces and the names that a regular expression would mistake them for, for the show_like_braces MySQL test"
run = false
*/

DEFINE TABLE `a`;
DEFINE TABLE `aa`;
DEFINE TABLE `aaa`;
DEFINE TABLE `a{`;
DEFINE TABLE `a{1`;
DEFINE TABLE `a{1}`;
DEFINE TABLE `a{2,3}`;
DEFINE TABLE `{1}`;
DEFINE TABLE `}`;
