a.{ b.{ c }}
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
                                DestructureOperator::Destructure
                                    Some
                                        -Destructure
                                            .field: Ident
                                                .text: "c"
                                            .op: None
*/