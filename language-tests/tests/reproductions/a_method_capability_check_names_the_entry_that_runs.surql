/**
[env]

[env.capabilities]
deny-functions = ["string::is::email", "type::string"]

[test]
reason = """
A method call runs one registry entry, and the capability check names that
entry — the same name a direct call is checked against.

- `'a@b.com'.is_email()` runs `string::is_email`, so a deny list naming the
  retired spelling `string::is::email` does not refuse it.
- `(5).to_string()` runs `type::string`, so `type::string` in a deny list
  refuses the call.

The first query shows `string::is_email` cannot be named in a list at all:
`FuncTarget` accepts no underscore, so the direct call to it is not refused.
"""

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Function 'type::string' is not allowed to be executed"

[[test.results]]
error = "Function 'type::string' is not allowed to be executed"
*/

-- Control: the entry's own name carries an underscore, so no deny list can
-- name it and the direct call runs.
RETURN string::is_email('a@b.com');

-- Method syntax reaches `string::is_email`, which the retired spelling the deny
-- list holds does not name.
RETURN 'a@b.com'.is_email();

-- Control: a name the list holds, refused wherever it is reached.
RETURN type::string(5);

-- Method syntax reaches `type::string`, which the list names, so the call is
-- refused.
RETURN (5).to_string();
