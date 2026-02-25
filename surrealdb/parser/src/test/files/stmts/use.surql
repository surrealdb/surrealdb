USE NAMESPACE ns;
USE DATABASE db;
USE NAMESPACE ns DATABASE db;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Use
            Use
                .kind: UseStatementKind::Namespace
                    Ident
                        .text: "ns"
        -TopLevelExpr::Use
            Use
                .kind: UseStatementKind::Database
                    Ident
                        .text: "db"
        -TopLevelExpr::Use
            Use
                .kind: UseStatementKind::NamespaceDatabase
                    Ident
                        .text: "ns"
                    Ident
                        .text: "db"
*/