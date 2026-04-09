THROW "a string";
THROW 1 + 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Throw
                Expr::String
                    StringLit
                        .text: "a string"
        -TopLevelExpr::Expr
            Expr::Throw
                Expr::Binary
                    BinaryExpr
                        .left: Expr::Integer
                            +1
                        .op: Add
                        .right: Expr::Integer
                            +1
*/