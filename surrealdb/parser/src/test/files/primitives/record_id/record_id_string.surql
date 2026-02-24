r"a:1";
r"a\u{3a}1";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Number
                        +1
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Number
                        +1
*/