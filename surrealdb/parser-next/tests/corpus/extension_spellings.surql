-- Every spelling of an extension statement and call: `EXTENSION` and `ext::`,
-- and the deprecated aliases `MODULE` and `mod::` in every combination.
DEFINE EXTENSION ext::a FROM f"bucket:/a.wasm" UNSIGNED;
DEFINE EXTENSION IF NOT EXISTS ext::a FROM f"bucket:/a.wasm" UNSIGNED COMMENT 'c' PERMISSIONS NONE;
DEFINE EXTENSION OVERWRITE ext::p FROM silo::acme::p::<1.2.3> UNSIGNED;
DEFINE MODULE ext::b FROM f"bucket:/b.wasm" UNSIGNED;
DEFINE EXTENSION mod::c FROM f"bucket:/c.wasm" UNSIGNED;
DEFINE MODULE mod::d FROM silo::acme::d::<0.1.0> UNSIGNED;
ALTER EXTENSION ext::a COMMENT 'x';
ALTER EXTENSION IF EXISTS mod::a DROP COMMENT;
ALTER MODULE silo::acme::p::<1.2.3> PERMISSIONS FULL;
REMOVE EXTENSION ext::a;
REMOVE EXTENSION IF EXISTS mod::a;
REMOVE MODULE silo::acme::p::<1.2.3>;
RETURN ext::a::f(1, 2);
RETURN ext::a();
RETURN mod::a::b::c();
RETURN silo::acme::p::<1.2.3>::f();
DEFINE ANALYZER x FUNCTION ext::a::lower TOKENIZERS class;
DEFINE ANALYZER y FUNCTION mod::a::lower TOKENIZERS class;
-- Refused: an extension without a name.
DEFINE EXTENSION silo::acme::p::<1.2.3> UNSIGNED;
DEFINE MODULE silo::acme::p::<1.2.3> UNSIGNED;
DEFINE EXTENSION f"bucket:/a.wasm" UNSIGNED;
-- Refused: no UNSIGNED, a bad prefix.
DEFINE EXTENSION ext::a FROM f"bucket:/a.wasm";
DEFINE EXTENSION fn::a FROM f"bucket:/a.wasm" UNSIGNED;
REMOVE EXTENSION a;
