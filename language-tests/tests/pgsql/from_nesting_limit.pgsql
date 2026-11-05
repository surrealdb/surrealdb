/**
[env]
namespace = true
database = true

[test]
reason = "A FROM clause nested in more parentheses than the parser's recursion limit allows is refused. The byte offset in the error counts this header, so an edit to the header moves it."

[test.results]
request-error = "PostgreSQL parse error: at 493..494: parser recursion depth limit exceeded"
*/

SELECT * FROM ((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((a JOIN b ON a.id = b.id))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
