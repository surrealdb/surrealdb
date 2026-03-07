$`param`;
$`with space`;
$`\n\r\u{2665}`;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Param
                Param
                    .text: "param"
        -TopLevelExpr::Expr
            Expr::Param
                Param
                    .text: "with space"
        -TopLevelExpr::Expr
            Expr::Param
                Param
                    .text: "\n\r♥"
*/