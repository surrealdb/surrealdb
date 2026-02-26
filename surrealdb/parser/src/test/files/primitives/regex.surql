/abc/;
/\//;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Regex
                Regex
                    .source: "abc"
        -TopLevelExpr::Expr
            Expr::Regex
                Regex
                    .source: "/"
*/