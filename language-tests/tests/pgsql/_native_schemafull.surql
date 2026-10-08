/**
[test]
run = false
*/

DEFINE TABLE acct SCHEMAFULL;
DEFINE FIELD acct_no ON acct TYPE int;
DEFINE FIELD name ON acct TYPE string;
DEFINE FIELD balance ON acct TYPE number DEFAULT 0;
DEFINE FIELD tier ON acct TYPE string VALUE string::lowercase($value) DEFAULT 'basic';
DEFINE FIELD note ON acct TYPE option<string>;
DEFINE FIELD double ON acct COMPUTED balance * 2;
DEFINE INDEX acct_by_no ON acct FIELDS acct_no UNIQUE;

DEFINE TABLE tx SCHEMAFULL;
DEFINE FIELD acct_id ON tx TYPE int;
DEFINE FIELD amount ON tx TYPE int ASSERT $value > 0;
DEFINE FIELD kind ON tx TYPE string DEFAULT 'credit';

CREATE acct:1 SET acct_no = 1, name = 'Ann', balance = 50, tier = 'GOLD', note = 'vip' RETURN NONE;
CREATE acct:2 SET acct_no = 2, name = 'Ben' RETURN NONE;
CREATE acct:3 SET acct_no = 3, name = 'Cy', balance = 12.5, tier = 'Silver' RETURN NONE;
CREATE acct:4 SET acct_no = 4, name = 'Di', balance = 200 RETURN NONE;

CREATE tx:1 SET acct_id = 1, amount = 10 RETURN NONE;
CREATE tx:2 SET acct_id = 1, amount = 30, kind = 'debit' RETURN NONE;
CREATE tx:3 SET acct_id = 3, amount = 5 RETURN NONE;
CREATE tx:4 SET acct_id = 9, amount = 1 RETURN NONE;
