-- SURREAL_CAPS_ALLOW_EXPERIMENTAL=surrealism SURREAL_BUCKET_FOLDER_ALLOWLIST=/Users/micha/Repositories/SurrealDB/surrealdb/crates/surrealism/test make repl

DEFINE BUCKET test BACKEND "file:/Users/micha/Repositories/SurrealDB/surrealdb/crates/surrealism/test";
DEFINE MODULE mod::test FROM f"test:/demo.surli" UNSIGNED;