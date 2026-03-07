USE NAMESPACE ns;
USE DATABASE db;
USE NAMESPACE ns DATABASE db;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Use
            Use
                .kind: UseKind::Namespace
                    Ident
                        .text: "ns"
        -TopLevelExpr::Use
            Use
                .kind: UseKind::Database
                    Ident
                        .text: "db"
        -TopLevelExpr::Use
            Use
                .kind: UseKind::NamespaceDatabase
                    .namespace: Ident
                        .text: "ns"
                    .database: Ident
                        .text: "db"
*/