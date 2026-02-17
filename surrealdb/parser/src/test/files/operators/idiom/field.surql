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
                    .op: PostfixOperator::Field
                        .name: "b"
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
                            .op: PostfixOperator::Field
                                .name: "b"
                    .op: PostfixOperator::Field
                        .name: "c"
*/