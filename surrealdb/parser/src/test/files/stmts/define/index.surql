DEFINE INDEX OVERWRITE a ON b;
DEFINE INDEX OVERWRITE a ON TABLE b;
DEFINE INDEX OVERWRITE a ON b CONCURRENTLY;
DEFINE INDEX OVERWRITE a ON b COMMENT "comment";
DEFINE INDEX OVERWRITE a ON b FIELDS a,b COMMENT "comment";
DEFINE INDEX OVERWRITE a ON b COUNT;
DEFINE INDEX OVERWRITE a ON b COUNT WHERE 1 + 1;
DEFINE INDEX OVERWRITE a ON b HNSW;
DEFINE INDEX OVERWRITE a ON b HNSW DISTANCE EUCLIDEAN;
DEFINE INDEX OVERWRITE a ON b HNSW DISTANCE MINKOWSKI 1.0;
DEFINE INDEX OVERWRITE a ON b HNSW TYPE F64;
DEFINE INDEX OVERWRITE a ON b HNSW TYPE I16;
DEFINE INDEX OVERWRITE a ON b HNSW LM 1;
DEFINE INDEX OVERWRITE a ON b HNSW M0 1;
DEFINE INDEX OVERWRITE a ON b HNSW M 1;
DEFINE INDEX OVERWRITE a ON b HNSW EFC 1;
DEFINE INDEX OVERWRITE a ON b HNSW EXTEND_CANDIDATES;
DEFINE INDEX OVERWRITE a ON b HNSW KEEP_PRUNED_CONNECTIONS;
DEFINE INDEX OVERWRITE a ON b HNSW HASHED_VECTOR;
DEFINE INDEX OVERWRITE a ON b FULLTEXT ANALYZER nlyzr;
DEFINE INDEX OVERWRITE a ON b FULLTEXT HIGHLIGHTS;
DEFINE INDEX OVERWRITE a ON b FULLTEXT BM25;
DEFINE INDEX OVERWRITE a ON b FULLTEXT BM25(1.0,2.0);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: None
                    .concurrently: true
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: Some
                        Expr::String
                            StringLit
                                .text: "comment"
                    .index: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: Some
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .comment: Some
                        Expr::String
                            StringLit
                                .text: "comment"
                    .index: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Count
                            CountIndex
                                .condition: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Count
                            CountIndex
                                .condition: Some
                                    Expr::Binary
                                        BinaryExpr
                                            .left: Expr::Integer
                                                +1
                                            .op: Add
                                            .right: Expr::Integer
                                                +1
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: Some
                                    Distance::Euclidean
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: Some
                                    Distance::Minkowski
                                        1.0
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: Some
                                    F64
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: Some
                                    I16
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: Some
                                    +1
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: None
                                .m0: Some
                                    +1
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: Some
                                    +1
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: Some
                                    +1
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: true
                                .keep_pruned_connections: false
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: true
                                .use_hashed_vector: false
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::Hnsw
                            HnswIndex
                                .distance: None
                                .ty: None
                                .m: None
                                .m0: None
                                .ml: None
                                .ef_construction: None
                                .extend_candidates: false
                                .keep_pruned_connections: false
                                .use_hashed_vector: true
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::FullText
                            FullTextIndex
                                .analyzer: Some
                                    Ident
                                        .text: "nlyzr"
                                .highlights: false
                                .scoring: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::FullText
                            FullTextIndex
                                .analyzer: None
                                .highlights: true
                                .scoring: None
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::FullText
                            FullTextIndex
                                .analyzer: None
                                .highlights: false
                                .scoring: Some
                                    FullTextScoring::VectorSearch
                    .concurrently: false
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "b"
                            .parts: None
                    .fields: None
                    .comment: None
                    .index: Some
                        Index::FullText
                            FullTextIndex
                                .analyzer: None
                                .highlights: false
                                .scoring: Some
                                    FullTextScoring::Bm25
                                        .k1: 1.0
                                        .b: 2.0
                    .concurrently: false
*/