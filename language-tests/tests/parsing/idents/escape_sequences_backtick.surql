/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "{ \"\": 'DEFINE TABLE `` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"\u0000\"\u0000: 'DEFINE TABLE `\\\\0` TYPE ANY SCHEMALESS PERMISSIONS NONE' }"



*/

{
	DEFINE TABLE ``;
	DEFINE TABLE `\0`;
	-- DEFINE TABLE `\n`;
	-- DEFINE TABLE `\r`;
	-- DEFINE TABLE `\t`;
	-- DEFINE TABLE `\f`;
	-- DEFINE TABLE `\\`;
	-- DEFINE TABLE `\"`;
	-- DEFINE TABLE `\'`;
	-- DEFINE TABLE `\``;
	-- DEFINE TABLE `\u0053`;
	-- DEFINE TABLE `\u{55}`;
	"OK";
};
(INFO FOR DB).tables;
