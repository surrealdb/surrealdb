a.b;
a.b.c;
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
                    .op: IdiomOperator::Field
                        Ident
                            .text: "b"
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Idiom
                        IdiomExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: IdiomOperator::Field
                                Ident
                                    .text: "b"
                    .op: IdiomOperator::Field
                        Ident
                            .text: "c"
*/