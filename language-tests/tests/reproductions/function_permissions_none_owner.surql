/**
[env]
namespace = "test"
database = "test"
imports = ["reproductions/function_permissions_none_record_setup.surql"]
auth = { level = "owner" }

[test]
reason = "System-level users (owner) should still be able to access functions with PERMISSIONS NONE"

[[test.results]]
value = "[{ id: product:test, name: 'test product' }]"
*/

fn::get_products();
