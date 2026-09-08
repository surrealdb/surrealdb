/**
[test]
reason = "Chain setup for index_standard.surql: creates the index on the first walked version. Standard indexes exist in every 3.x release, so this runs at the head of the chain."
run = false
*/
DEFINE INDEX chain_std_org ON TABLE chain_std COLUMNS org;
