/**
[env]
namespace = true
database = true

[test]
reason = "A `PERMISSIONS FOR select WHERE` predicate is refused at DEFINE where a block in it holds a `LET` of a name the system binds (`$this`, `$self`, `$parent`, `$value`, `$input`, `$before`, `$after`, `$event`), whether or not a closure in the block reads the name: each is a value a clause reads from its frame, so no `LET` binds it."

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
error = "'value' is a protected variable and cannot be set"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
error = "'before' is a protected variable and cannot be set"

[[test.results]]
error = "'after' is a protected variable and cannot be set"

[[test.results]]
error = "'event' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
error = "'value' is a protected variable and cannot be set"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
error = "'before' is a protected variable and cannot be set"

[[test.results]]
error = "'after' is a protected variable and cannot be set"

[[test.results]]
error = "'event' is a protected variable and cannot be set"
*/
DEFINE TABLE pt_this PERMISSIONS FOR select WHERE { LET $this = { lim: 3 }; (|$e| $this.lim)(0) } = 3;
DEFINE TABLE pt_self PERMISSIONS FOR select WHERE { LET $self = { lim: 3 }; (|$e| $self.lim)(0) } = 3;
DEFINE TABLE pt_parent PERMISSIONS FOR select WHERE { LET $parent = { lim: 3 }; (|$e| $parent.lim)(0) } = 3;
DEFINE TABLE pt_value PERMISSIONS FOR select WHERE { LET $value = { lim: 3 }; (|$e| $value.lim)(0) } = 3;
DEFINE TABLE pt_input PERMISSIONS FOR select WHERE { LET $input = { lim: 3 }; (|$e| $input.lim)(0) } = 3;
DEFINE TABLE pt_before PERMISSIONS FOR select WHERE { LET $before = { lim: 3 }; (|$e| $before.lim)(0) } = 3;
DEFINE TABLE pt_after PERMISSIONS FOR select WHERE { LET $after = { lim: 3 }; (|$e| $after.lim)(0) } = 3;
DEFINE TABLE pt_event PERMISSIONS FOR select WHERE { LET $event = { lim: 3 }; (|$e| $event.lim)(0) } = 3;
DEFINE TABLE pt_this_cf PERMISSIONS FOR select WHERE { LET $this = { lim: 3 }; $this.lim } = 3;
DEFINE TABLE pt_self_cf PERMISSIONS FOR select WHERE { LET $self = { lim: 3 }; $self.lim } = 3;
DEFINE TABLE pt_parent_cf PERMISSIONS FOR select WHERE { LET $parent = { lim: 3 }; $parent.lim } = 3;
DEFINE TABLE pt_value_cf PERMISSIONS FOR select WHERE { LET $value = { lim: 3 }; $value.lim } = 3;
DEFINE TABLE pt_input_cf PERMISSIONS FOR select WHERE { LET $input = { lim: 3 }; $input.lim } = 3;
DEFINE TABLE pt_before_cf PERMISSIONS FOR select WHERE { LET $before = { lim: 3 }; $before.lim } = 3;
DEFINE TABLE pt_after_cf PERMISSIONS FOR select WHERE { LET $after = { lim: 3 }; $after.lim } = 3;
DEFINE TABLE pt_event_cf PERMISSIONS FOR select WHERE { LET $event = { lim: 3 }; $event.lim } = 3;
