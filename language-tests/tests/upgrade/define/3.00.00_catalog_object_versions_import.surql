/**
[test]
reason = "Setup for 3.00.00_catalog_object_versions.surql: a binary that writes no catalog object versions defines a param, an analyzer and a sequence."
run = false
*/
DEFINE PARAM $versioned VALUE 1;
DEFINE ANALYZER versioned TOKENIZERS blank FILTERS lowercase;
DEFINE SEQUENCE versioned START 10;
