/**
[env]
planner-strategy = ["all-ro"]

[env.capabilities]
deny-functions = ["mod::secret"]

[test]
reason = "A denied Surrealism module call must not be discarded by an eliminating fold; its capability name carries the mod:: prefix the executor applies, not the bare name held in the AST"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
error = "Module function 'mod::secret' requires the 'surrealism' feature to be enabled"

*/

CREATE t:1 SET a = 5;

-- `Function::Module` holds the bare module name, while `SurrealismModuleExec`
-- checks `mod::<module>`. Matching on the bare name would read this call as
-- allowed and let the fold collapse the predicate to `false`, returning `[]`
-- with no error at all. Preserving the subtree means evaluation is reached.
--
-- Scoped to the new planner because the error text differs by build: with the
-- `surrealism` feature compiled out the module executor reports the missing
-- feature before consulting capabilities, whereas the legacy executor reaches
-- its capability check first and reports FunctionNotAllowed. What both agree
-- on, and what this pins, is that the call is not silently eliminated.
SELECT * FROM t WHERE mod::secret() IN [];
