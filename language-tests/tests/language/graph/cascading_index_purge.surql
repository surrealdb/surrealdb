/**
[test]
reason = "Index on the graph table should be purged when a edge is removed."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

*/

{
    CREATE dev:1;
    CREATE pm:1;
    CREATE ceo:1;

    RELATE dev:1 -> dev_ceo -> ceo:1;
    RELATE pm:1 -> pm_ceo -> ceo:1;

    DEFINE INDEX index_dev ON dev_ceo FIELDS in UNIQUE;
    DEFINE INDEX index_pm ON pm_ceo FIELDS in;
    NONE
};

DELETE ceo:1;

{
    CREATE ceo:1;
    RELATE dev:1 -> dev_ceo -> ceo:1;
    RELATE pm:1 -> pm_ceo -> ceo:1;
    RELATE pm:1 -> pm_ceo -> ceo:1;
    NONE
};

DELETE ceo:1;

{
    CREATE ceo:1;
    RELATE dev:1 -> dev_ceo -> ceo:1;
    RELATE pm:1 -> pm_ceo -> ceo:1;
    RELATE pm:1 -> pm_ceo -> ceo:1;
    NONE
};