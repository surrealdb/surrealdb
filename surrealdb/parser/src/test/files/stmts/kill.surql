KILL u"2ba318c4-2267-4319-becd-3c60c237f1e2";
KILL $param;

/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Kill
            Kill
                .kind: KillKind::Uuid
                    2ba318c4-2267-4319-becd-3c60c237f1e2
        -TopLevelExpr::Kill
            Kill
                .kind: KillKind::Param
                    Param
                        .text: "param"
*/