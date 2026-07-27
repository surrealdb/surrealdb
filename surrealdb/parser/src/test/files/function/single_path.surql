sleep(1ms);
sleep(1ms, 1ms);
count(1);
not(true);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "sleep"
                            .parts: None
                    .op: IdiomOperator::Call
                        Some
                            -Expr::Duration
                                1ms
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "sleep"
                            .parts: None
                    .op: IdiomOperator::Call
                        Some
                            -Expr::Duration
                                1ms
                            -Expr::Duration
                                1ms
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "count"
                            .parts: None
                    .op: IdiomOperator::Call
                        Some
                            -Expr::Integer
                                +1
        -TopLevelExpr::Expr
            Expr::Idiom
                IdiomExpr
                    .left: Expr::Path
                        Path
                            .start: Ident
                                .text: "not"
                            .parts: None
                    .op: IdiomOperator::Call
                        Some
                            -Expr::Builtin
                                Builtin::True
*/