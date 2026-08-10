a + b * c
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Binary
                BinaryExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .op: BinaryOperator::Add
                    .right: Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "b"
                                    .parts: None
                            .op: BinaryOperator::Multiply
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "c"
                                    .parts: None
*/