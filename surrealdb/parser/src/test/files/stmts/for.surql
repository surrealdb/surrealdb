FOR $i IN 0..10{
	if $i > 10 {
		BREAK;
	};
};
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::For
                For
                    .param: Param
                        .text: "i"
                    .range: Expr::Binary
                        BinaryExpr
                            .left: Expr::Integer
                                +0
                            .op: BinaryOperator::Range
                            .right: Expr::Integer
                                +10
                    .body: Block
                        .exprs: Some
                            -Expr::If
                                If
                                    .condition: Expr::Binary
                                        BinaryExpr
                                            .left: Expr::Param
                                                Param
                                                    .text: "i"
                                            .op: BinaryOperator::GreaterThan
                                            .right: Expr::Integer
                                                +10
                                    .then: Expr::Block
                                        Block
                                            .exprs: Some
                                                -Expr::Break
                                    .otherwise: None
*/