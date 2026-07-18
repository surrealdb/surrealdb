SLEEP 1s;
RETURN sleep(1ms);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Sleep
                1s
        -TopLevelExpr::Expr
            Expr::Return
                Return
                    .expr: Expr::Idiom
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
                    .fetch: None
*/