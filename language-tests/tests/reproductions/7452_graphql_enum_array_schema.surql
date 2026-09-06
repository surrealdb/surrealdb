/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Schema fixture for the literal-union enum array-value reproductions (the value-conversion sibling of the #7452 nullability defect)"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS NONE;

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
-- Control: a scalar literal union converts in both directions today.
DEFINE FIELD status ON person TYPE 'NEW' | 'OLD';
-- Subject: the element type is the same enum, so it must convert the same way.
DEFINE FIELD roles ON person TYPE array<'ADMIN' | 'DEV'>;
-- Subject: the array layer may itself sit inside an `option<…>`.
DEFINE FIELD tags ON person TYPE option<array<'X' | 'Y'>>;

CREATE person:alice SET name = "Alice", status = 'NEW', roles = ['ADMIN', 'DEV'], tags = ['X'];
