1;
# a
2;
/* b */
3 /* c */;
-- d
4;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Integer
                +1
        -TopLevelExpr::Expr
            Expr::Integer
                +2
        -TopLevelExpr::Expr
            Expr::Integer
                +3
        -TopLevelExpr::Expr
            Expr::Integer
                +4
*/