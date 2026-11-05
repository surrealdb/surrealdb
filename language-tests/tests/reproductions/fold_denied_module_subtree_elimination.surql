/**
[env]

[env.capabilities]
deny-functions = ["mod::secret"]

[test]
reason = "A denied extension call must not be discarded by an eliminating fold; its capability name carries the prefix the executor applies, not the bare name held in the AST"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
error = "Extension function 'ext::secret' requires the 'surrealism' feature to be enabled"

*/

CREATE t:1 SET a = 5;

-- `Function::Module` holds the bare extension name, while the capability
-- check reads the call as `mod::<name>` and `ext::<name>`. Matching on the
-- bare name would read this call as allowed and let the fold collapse the
-- predicate to `false`, returning `[]` with no error at all. Preserving the subtree means evaluation is reached.
--
-- The error text differs by build: with the `surrealism` feature compiled out
-- the module executor reports the missing feature before consulting
-- capabilities. What this pins is that the call is not silently eliminated.
SELECT * FROM t WHERE mod::secret() IN [];
