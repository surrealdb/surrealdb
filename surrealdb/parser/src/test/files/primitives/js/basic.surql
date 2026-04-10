function(1 + 1, $param) {
	return "foo"
};
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::JsFunction
                JsFunction
                    .args: Some
                        -Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: Add
                                .right: Expr::Integer
                                    +1
                        -Expr::Param
                            Param
                                .text: "param"
                    .body: JsFunctionBody
                        .source: "\n\treturn \"foo\"\n"
*/