/**
[env]
namespace = "test"
database = "test"
imports = ["reproductions/function_permissions_none_record_setup.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = "The NONE permission denies all record users access to the function"

[[test.results]]
error = "You don't have permission to run the fn::get_products function"
*/

fn::get_products();
