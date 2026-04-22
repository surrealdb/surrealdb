function(){
	return `weird ${ `format ${ "s" }` }tring`
}
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::JsFunction
                JsFunction
                    .args: None
                    .body: JsFunctionBody
                        .source: "\n\treturn `weird ${ `format ${ \"s\" }` }tring`\n"
*/