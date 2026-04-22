{;};
{;;;;;};
{1;;;2;;};
{1;2};
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Block
                Block
                    .exprs: None
        -TopLevelExpr::Expr
            Expr::Block
                Block
                    .exprs: None
        -TopLevelExpr::Expr
            Expr::Block
                Block
                    .exprs: Some
                        -Expr::Integer
                            +1
                        -Expr::Integer
                            +2
        -TopLevelExpr::Expr
            Expr::Block
                Block
                    .exprs: Some
                        -Expr::Integer
                            +1
                        -Expr::Integer
                            +2
*/