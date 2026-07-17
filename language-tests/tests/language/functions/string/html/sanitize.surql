/**
[test]
reason = "Tests string::html::sanitize stripping unsafe HTML while keeping safe markup"

[[test.results]]
value = "'<b>Bold</b> and <i>italic</i>'"

[[test.results]]
value = "'Safe text'"

[[test.results]]
value = "'<div>Hello world!</div>'"

[[test.results]]
value = """'<a rel="noopener noreferrer">link</a>'"""

[[test.results]]
value = """'<img src="x">'"""

[[test.results]]
value = "'plain text'"

[[test.results]]
value = "''"
*/

string::html::sanitize("<b>Bold</b> and <i>italic</i>");
string::html::sanitize("<script>alert('xss')</script>Safe text");
string::html::sanitize("<div onclick=\"foo()\">Hello world!</div>");
string::html::sanitize("<a href=\"javascript:alert(1)\">link</a>");
string::html::sanitize("<img src=\"x\" onerror=\"alert(1)\">");
string::html::sanitize("plain text");
string::html::sanitize("");
