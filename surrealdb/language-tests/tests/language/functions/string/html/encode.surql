/**
[test]

[[test.results]]
value = "'&lt;#!DOCTYPE&#32;HTML&gt;&lt;div&#32;onclick&#61;&quot;foo()&quot;&gt;Hello&#32;world!&lt;&#47;div&gt;'"

*/
string::html::encode("<#!DOCTYPE HTML><div onclick=\"foo()\">Hello world!</div>");
