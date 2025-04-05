/**
[test]

[[test.results]]
value = "{ types: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ types: { email: 'DEFINE TYPE email AS string', phone: 'DEFINE TYPE phone AS string' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
error = "Type 'email' already exists"

[[test.results]]
value = "{ types: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/
DEFINE TYPE email AS string;
DEFINE TYPE phone AS string;

DEFINE TYPE email AS string;
DEFINE TYPE email AS string IF NOT EXISTS;

-- Test using user-defined types in field definitions
DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD email ON user TYPE email;
DEFINE FIELD phone ON user TYPE phone;

-- Test error when using undefined type
DEFINE FIELD invalid ON user TYPE undefined_type;

-- Test error when redefining a type
DEFINE TYPE email AS string;

-- Test IF NOT EXISTS clause
DEFINE TYPE email IF NOT EXISTS AS string;

-- Test using user-defined types in arrays
DEFINE TYPE contact_info AS record<email | phone>;
DEFINE FIELD contacts ON user TYPE array<contact_info>;

-- Test using user-defined types in options
DEFINE FIELD optional_email ON user TYPE option<email>;

-- Test using user-defined types in sets
DEFINE FIELD unique_phones ON user TYPE set<phone>;

-- Test using user-defined types in either
DEFINE FIELD contact ON user TYPE email | phone;
