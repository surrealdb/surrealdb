/**
[test]
reason = "Repair script for index_fulltext.surql: the manual step the 3.3.0 betas require before a pre-shared-doc-ID FULLTEXT index can be queried."
run = false
*/
REBUILD INDEX blog_title ON blog;
