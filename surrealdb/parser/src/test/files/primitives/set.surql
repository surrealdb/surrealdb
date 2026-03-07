{,};
{1,};
{1,2};
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Set
                Set
                    .entries: None
        -TopLevelExpr::Expr
            Expr::Set
                Set
                    .entries: Some
                        -Expr::Integer
                            +1
        -TopLevelExpr::Expr
            Expr::Set
                Set
                    .entries: Some
                        -Expr::Integer
                            +1
                        -Expr::Integer
                            +2
*/