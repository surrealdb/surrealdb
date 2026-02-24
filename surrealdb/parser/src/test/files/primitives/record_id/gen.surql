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
                        DestructureOperator::Rand
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Generate
                        DestructureOperator::Uuid
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Generate
                        DestructureOperator::Ulid
*/