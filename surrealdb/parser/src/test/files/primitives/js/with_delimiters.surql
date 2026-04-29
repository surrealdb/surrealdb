function(){
	if(1){
		foo([1,2,3])
	}
}
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::JsFunction
                JsFunction
                    .args: None
                    .body: JsFunctionBody
                        .source: "\n\tif(1){\n\t\tfoo([1,2,3])\n\t}\n"
*/