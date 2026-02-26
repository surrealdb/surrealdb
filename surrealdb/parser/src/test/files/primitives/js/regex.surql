function(){
	/\{\[\(/
};
function(){
	/\}\]\)/
}
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::JsFunction
                JsFunction
                    .args: None
                    .body: JsFunctionBody
                        .source: "\n\t/\\{\\[\\(/\n"
        -TopLevelExpr::Expr
            Expr::JsFunction
                JsFunction
                    .args: None
                    .body: JsFunctionBody
                        .source: "\n\t/\\}\\]\\)/\n"
*/