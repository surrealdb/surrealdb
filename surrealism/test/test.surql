-- SURREAL_CAPS_ALLOW_EXPERIMENTAL=files,surrealism SURREAL_BUCKET_FOLDER_ALLOWLIST=/Users/micha/Repositories/SurrealDB/surrealdb/surrealdb/surrealism/test make repl

DEFINE BUCKET test BACKEND "file:/Users/micha/Repositories/SurrealDB/surrealdb/surrealdb/surrealism/test";
DEFINE MODULE mod::test AS f"test:/demo.surli";