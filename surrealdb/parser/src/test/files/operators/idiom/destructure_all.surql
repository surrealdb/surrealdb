a.{ b.* }
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "a"
                            .parts: None
                    .op: IdiomOperator::Destructure
                        Some
                            -Destructure
                                .field: Ident
                                    .text: "b"
                                .op: Some
                                DestructureOperator::All
*/