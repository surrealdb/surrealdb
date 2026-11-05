/**
[test]
reason = "Two tables with fields and indexes for the show_* MySQL tests"
run = false
*/

DEFINE TABLE show_accounts SCHEMAFULL;
DEFINE FIELD id ON show_accounts TYPE int;
DEFINE FIELD email ON show_accounts TYPE string;
DEFINE FIELD note ON show_accounts TYPE none | null | string;
DEFINE FIELD balance ON show_accounts TYPE decimal DEFAULT 0dec;
DEFINE INDEX show_accounts_id_pk ON show_accounts FIELDS id UNIQUE;
DEFINE INDEX show_accounts_email_uniq ON show_accounts FIELDS email UNIQUE;
DEFINE INDEX show_accounts_note ON show_accounts FIELDS note, balance;
DEFINE TABLE show_ledger SCHEMAFULL;
DEFINE FIELD n ON show_ledger TYPE int;
DEFINE FIELD amount ON show_ledger TYPE int;
