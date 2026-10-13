ALTER TABLE foo SCHEMAFULL;
ALTER TABLE IF EXISTS foo SCHEMALESS COMMENT "table";
ALTER TABLE foo DROP COMMENT;
ALTER TABLE foo CHANGEFEED 1s;
ALTER TABLE foo DROP CHANGEFEED;
ALTER TABLE foo INLINE EDGES 64;
ALTER TABLE foo INLINE REFERENCES 16;
ALTER TABLE foo DROP INLINE EDGES;
ALTER TABLE foo DROP INLINE REFERENCES;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .changefeed: None
                    .schema: Some
                    Full
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: None
                    .inline_refs_cap: None
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: true
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: Some
                    AlterKind::Set
                        Expr::String
                            StringLit
                                .text: "table"
                    .changefeed: None
                    .schema: Some
                    Less
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: None
                    .inline_refs_cap: None
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: Some
                    AlterKind::Drop
                    .changefeed: None
                    .schema: None
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: None
                    .inline_refs_cap: None
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .changefeed: Some
                    AlterKind::Set
                        ChangeFeed::Base
                            1s
                    .schema: None
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: None
                    .inline_refs_cap: None
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .changefeed: Some
                    AlterKind::Drop
                    .schema: None
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: None
                    .inline_refs_cap: None
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .changefeed: None
                    .schema: None
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: Some
                    AlterKind::Set
                        +64
                    .inline_refs_cap: None
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .changefeed: None
                    .schema: None
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: None
                    .inline_refs_cap: Some
                    AlterKind::Set
                        +16
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .changefeed: None
                    .schema: None
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: Some
                    AlterKind::Drop
                    .inline_refs_cap: None
        -TopLevelExpr::Expr
            Expr::AlterTable
                AlterTable
                    .if_exists: false
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .changefeed: None
                    .schema: None
                    .table_kind: None
                    .compact: false
                    .permissions: None
                    .inline_edges_cap: None
                    .inline_refs_cap: Some
                    AlterKind::Drop
*/