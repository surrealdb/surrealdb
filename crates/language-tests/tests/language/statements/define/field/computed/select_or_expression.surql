/**
[test]
reason = "Test computed field with SELECT VALUE expression and OR operation"
issue = 6447

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['test']"

[[test.results]]
value = "['test']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ block_default: ['test'], block_empty: [], expr_default: ['test'], expr_empty: [], id: user:tester, math: ['test'] }]"
*/

-- Should return []
DEFINE TABLE user;

-- Should return []
SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE;

-- Should return []
(SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE);

-- Should return []
{ SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE };

-- Should return ['test']
(SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE) OR ['test'];

-- Should return ['test']
{ SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE } OR ['test'];

-- Field should have same behavour
DEFINE FIELD math ON user COMPUTED { (3 - 3) OR ['test'] };

-- Field should have same behavour
DEFINE FIELD expr_empty ON user COMPUTED { (SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE) };

-- Field should have same behavour
DEFINE FIELD block_empty ON user COMPUTED { { SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE } };

-- Field should have same behavour
DEFINE FIELD expr_default ON user COMPUTED { (SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE) OR ['test'] };

-- Field should have same behavour
DEFINE FIELD block_default ON user COMPUTED { { SELECT VALUE progress FROM [NONE] WHERE progress IS NOT NONE } OR ['test'] };

-- Both fields should be ['test']
CREATE user:tester;
