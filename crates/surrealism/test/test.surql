-- SURREAL_CAPS_ALLOW_EXPERIMENTAL=files SURREAL_BUCKET_FOLDER_ALLOWLIST=/Users/micha/Repositories/SurrealDB/surrealdb/crates/surrealism/test make repl

DEFINE BUCKET test BACKEND "file:/Users/micha/Repositories/SurrealDB/surrealdb/crates/surrealism/test";

DEFINE FUNCTION fn::test AS f"test:/demo.surli";