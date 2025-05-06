/**
[test]
wip = true
reason = """
When calling SELECT VALUE on an aggregate, the results are incorrect.

Breadcrumbs:
The issue shows up in `GroupsCollector::pushes`. If you replace the line:
```
stk.run(|stk| obj.get(stk, ctx, opt, None, idiom)).await.catch_return()?
```
with:
```
obj.clone()
```

Then it works and all other tests work, except `graph/graph_clauses.surql`.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ 'math::sum': 3 }]"

[[test.results]]
value = "[{ 'math::mean': 1.5f }]"

[[test.results]]
value = "[{ 'math::sum': 3 }]"

[[test.results]]
value = "[{ 'math::mean': 1.5f }]"

*/

BEGIN;
    UPSERT math_test CONTENT {num:1};
    UPSERT math_test CONTENT {num:2};

    RETURN "OK";
COMMIT;

# Works
SELECT math::sum(num) FROM math_test GROUP ALL;
SELECT math::mean(num) FROM math_test GROUP ALL;

# Broken, returns 0 and NaN
SELECT VALUE math::sum(num) FROM math_test GROUP ALL;
SELECT VALUE math::mean(num) FROM math_test GROUP ALL;
