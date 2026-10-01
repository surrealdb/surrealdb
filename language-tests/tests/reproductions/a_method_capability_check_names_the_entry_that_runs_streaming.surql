/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[env.capabilities]
deny-functions = ["string::is::email", "type::string"]

[test]
reason = """
Half of a pair. Same queries as `a_method_capability_check_names_the_entry_that_runs_legacy.surql`, pinned to the streaming planner; the two files' expected results are the divergence.

A method call runs one registry entry, and on the streaming path the capability
check names that entry — the same name a direct call is checked against on
either engine. The per-row path derives a name instead, from `Value::kind_of()`
and the method name, and that name is often not the entry that runs:

- `'a@b.com'.is_email()` runs `string::is_email`. The per-row path checks
  `string::is::email`, the spelling the function was renamed from, so a deny
  list naming that retired spelling refuses the call there and not on the
  streaming path.
- `(5).to_string()` runs `type::string`. The per-row path checks
  `int::to_string`, which names no function at all — the mapper's `number` arm
  never sees a numeric receiver, because `kind_of()` spells it `int`. So
  `type::string` in a deny list does not refuse the call there: the denied
  function runs.

The streaming expectations hold the contract in both cases. Reaching them from
the per-row path means checking the entry's own name, which the first query
shows is not yet expressible: `FuncTarget` accepts no underscore, so
`string::is_email` cannot be written in a list at all and neither engine refuses
the direct call to it.
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
-- name it and both engines run the direct call.
RETURN string::is_email('a@b.com');

-- Method syntax reaches `string::is_email`; only the per-row path answers to
-- the retired spelling the deny list holds.
RETURN 'a@b.com'.is_email();

-- Control: a name both paths agree on, refused wherever it is reached.
RETURN type::string(5);

-- Method syntax reaches `type::string`; the per-row path checks a name that
-- exists nowhere and runs the denied function.
RETURN (5).to_string();
