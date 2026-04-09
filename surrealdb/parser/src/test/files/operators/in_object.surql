1 IN { 1: 1 };
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Binary
                BinaryExpr
                    .left: Expr::Integer
                        +1
                    .op: Inside
                    .right: Expr::Object
                        Object
                            .entries: Some
                                -ObjectEntry
                                    .key: "1"
                                    .value: Expr::Integer
                                        +1
*/