a:rand();
a:UUID();
a:UlId();
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Generate
                        Rand
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Generate
                        Uuid
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Generate
                        Ulid
*/