let $a = 1;
let $b = a + 2;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Let
                Let
                    .param: Param
                        .text: "a"
                    .expr: Expr::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Let
                Let
                    .param: Param
                        .text: "b"
                    .expr: Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "a"
                                    .parts: None
                            .op: Add
                            .right: Expr::Integer
                                +2
*/