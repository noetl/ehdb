use std::collections::BTreeMap;

use ehdb_core::{
    EhdbError, NamespaceName, Result, SnapshotId, TableId, TableName, TableSchema, TenantId,
    TransactionId,
};
use ehdb_storage::ObjectRef;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TableKey {
    tenant: TenantId,
    namespace: NamespaceName,
    name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TableIdentity {
    tenant: TenantId,
    namespace: NamespaceName,
    id: TableId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SnapshotKey {
    table: TableIdentity,
    snapshot: SnapshotId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogTable {
    pub id: TableId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub name: TableName,
    pub schema: TableSchema,
    pub created_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub id: SnapshotId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub table_id: TableId,
    pub parent: Option<SnapshotId>,
    pub files: Vec<ObjectRef>,
    pub committed_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub name: TableName,
    pub schema: TableSchema,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSnapshot {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub table_id: TableId,
    pub snapshot_id: SnapshotId,
    pub parent_snapshot: Option<SnapshotId>,
    pub files: Vec<ObjectRef>,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryCatalog {
    tables: BTreeMap<TableKey, CatalogTable>,
    tables_by_id: BTreeMap<TableIdentity, CatalogTable>,
    snapshots: BTreeMap<SnapshotKey, CatalogSnapshot>,
    latest_snapshots: BTreeMap<TableIdentity, SnapshotId>,
}

impl InMemoryCatalog {
    pub fn create_table(&mut self, request: CreateTable) -> Result<CatalogTable> {
        let key = TableKey {
            tenant: request.tenant.clone(),
            namespace: request.namespace.clone(),
            name: request.name.clone(),
        };

        if self.tables.contains_key(&key) {
            return Err(EhdbError::AlreadyExists(format!(
                "{}.{}.{}",
                key.tenant, key.namespace, key.name
            )));
        }

        let table = CatalogTable {
            id: TableId::new(format!(
                "{}_{}_{}",
                request.tenant, request.namespace, request.name
            ))?,
            tenant: request.tenant,
            namespace: request.namespace,
            name: request.name,
            schema: request.schema,
            created_by: request.transaction_id,
        };

        let identity = TableIdentity {
            tenant: table.tenant.clone(),
            namespace: table.namespace.clone(),
            id: table.id.clone(),
        };

        self.tables.insert(key, table.clone());
        self.tables_by_id.insert(identity, table.clone());
        Ok(table)
    }

    pub fn commit_snapshot(&mut self, request: CommitSnapshot) -> Result<CatalogSnapshot> {
        if request.files.is_empty() {
            return Err(EhdbError::InvalidState(
                "snapshot requires at least one object reference".to_string(),
            ));
        }

        let table = TableIdentity {
            tenant: request.tenant.clone(),
            namespace: request.namespace.clone(),
            id: request.table_id.clone(),
        };
        if !self.tables_by_id.contains_key(&table) {
            return Err(EhdbError::NotFound(format!(
                "{}.{}.{}",
                table.tenant, table.namespace, table.id
            )));
        }

        let expected_parent = self.latest_snapshots.get(&table).cloned();
        if request.parent_snapshot != expected_parent {
            return Err(EhdbError::InvalidState(format!(
                "snapshot parent mismatch for {}.{}.{}: expected {:?}, got {:?}",
                table.tenant, table.namespace, table.id, expected_parent, request.parent_snapshot
            )));
        }

        let key = SnapshotKey {
            table: table.clone(),
            snapshot: request.snapshot_id.clone(),
        };
        if self.snapshots.contains_key(&key) {
            return Err(EhdbError::AlreadyExists(format!(
                "{}.{}.{}@{}",
                table.tenant, table.namespace, table.id, key.snapshot
            )));
        }

        let snapshot = CatalogSnapshot {
            id: request.snapshot_id,
            tenant: request.tenant,
            namespace: request.namespace,
            table_id: request.table_id,
            parent: request.parent_snapshot,
            files: request.files,
            committed_by: request.transaction_id,
        };

        self.snapshots.insert(key, snapshot.clone());
        self.latest_snapshots.insert(table, snapshot.id.clone());
        Ok(snapshot)
    }

    pub fn get_table(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        name: &TableName,
    ) -> Result<&CatalogTable> {
        let key = TableKey {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
        };

        self.tables
            .get(&key)
            .ok_or_else(|| EhdbError::NotFound(format!("{tenant}.{namespace}.{name}")))
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn get_snapshot(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        table_id: &TableId,
        snapshot_id: &SnapshotId,
    ) -> Result<&CatalogSnapshot> {
        let key = SnapshotKey {
            table: TableIdentity {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                id: table_id.clone(),
            },
            snapshot: snapshot_id.clone(),
        };

        self.snapshots.get(&key).ok_or_else(|| {
            EhdbError::NotFound(format!("{tenant}.{namespace}.{table_id}@{snapshot_id}"))
        })
    }

    pub fn latest_snapshot(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        table_id: &TableId,
    ) -> Result<&CatalogSnapshot> {
        let table = TableIdentity {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            id: table_id.clone(),
        };
        let snapshot_id = self
            .latest_snapshots
            .get(&table)
            .ok_or_else(|| EhdbError::NotFound(format!("{tenant}.{namespace}.{table_id}")))?;
        self.get_snapshot(tenant, namespace, table_id, snapshot_id)
    }

    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }
}

#[cfg(test)]
mod tests {
    use ehdb_core::{ColumnSchema, DataType};
    use ehdb_storage::{ObjectDigest, ObjectPath};

    use super::*;

    fn create_table_request() -> CreateTable {
        CreateTable {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("system").unwrap(),
            name: TableName::new("executions").unwrap(),
            schema: TableSchema::new(vec![ColumnSchema::new(
                "execution_id",
                DataType::Utf8,
                false,
            )
            .unwrap()])
            .unwrap(),
            transaction_id: TransactionId::new("txn-0001").unwrap(),
        }
    }

    fn object_ref(path: &str) -> ObjectRef {
        ObjectRef {
            path: ObjectPath::new(path).unwrap(),
            len: 128,
            digest: ObjectDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
        }
    }

    fn commit_snapshot_request(table_id: TableId, snapshot_id: &str) -> CommitSnapshot {
        CommitSnapshot {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("system").unwrap(),
            table_id,
            snapshot_id: SnapshotId::new(snapshot_id).unwrap(),
            parent_snapshot: None,
            files: vec![object_ref("tenant-a/system/table/snapshot/part-000.arrow")],
            transaction_id: TransactionId::new(format!("txn-{snapshot_id}")).unwrap(),
        }
    }

    #[test]
    fn creates_and_reads_table() {
        let mut catalog = InMemoryCatalog::default();
        let table = catalog.create_table(create_table_request()).unwrap();

        let found = catalog
            .get_table(&table.tenant, &table.namespace, &table.name)
            .unwrap();

        assert_eq!(found.id, table.id);
        assert_eq!(catalog.table_count(), 1);
    }

    #[test]
    fn rejects_duplicate_table() {
        let mut catalog = InMemoryCatalog::default();

        catalog.create_table(create_table_request()).unwrap();
        let error = catalog.create_table(create_table_request()).unwrap_err();

        assert!(matches!(error, EhdbError::AlreadyExists(_)));
    }

    #[test]
    fn missing_table_is_not_found() {
        let catalog = InMemoryCatalog::default();
        let error = catalog
            .get_table(
                &TenantId::new("tenant-a").unwrap(),
                &NamespaceName::new("system").unwrap(),
                &TableName::new("executions").unwrap(),
            )
            .unwrap_err();

        assert!(matches!(error, EhdbError::NotFound(_)));
    }

    #[test]
    fn commits_and_reads_table_snapshot() {
        let mut catalog = InMemoryCatalog::default();
        let table = catalog.create_table(create_table_request()).unwrap();
        let snapshot = catalog
            .commit_snapshot(commit_snapshot_request(table.id.clone(), "snapshot-0001"))
            .unwrap();

        assert_eq!(snapshot.table_id, table.id);
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(catalog.snapshot_count(), 1);
        assert_eq!(
            catalog
                .latest_snapshot(&table.tenant, &table.namespace, &table.id)
                .unwrap()
                .id,
            snapshot.id
        );
        assert_eq!(
            catalog
                .get_snapshot(
                    &table.tenant,
                    &table.namespace,
                    &table.id,
                    &SnapshotId::new("snapshot-0001").unwrap()
                )
                .unwrap()
                .files[0]
                .len,
            128
        );
    }

    #[test]
    fn rejects_snapshot_for_missing_table_empty_files_and_duplicates() {
        let mut catalog = InMemoryCatalog::default();
        let missing = commit_snapshot_request(TableId::new("missing").unwrap(), "snapshot-0001");
        assert!(matches!(
            catalog.commit_snapshot(missing).unwrap_err(),
            EhdbError::NotFound(_)
        ));

        let table = catalog.create_table(create_table_request()).unwrap();
        let mut empty = commit_snapshot_request(table.id.clone(), "snapshot-0001");
        empty.files.clear();
        assert!(matches!(
            catalog.commit_snapshot(empty).unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        catalog
            .commit_snapshot(commit_snapshot_request(table.id.clone(), "snapshot-0001"))
            .unwrap();
        assert!(matches!(
            catalog
                .commit_snapshot(commit_snapshot_request(table.id, "snapshot-0001"))
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn enforces_snapshot_parent_chain() {
        let mut catalog = InMemoryCatalog::default();
        let table = catalog.create_table(create_table_request()).unwrap();
        let first = catalog
            .commit_snapshot(commit_snapshot_request(table.id.clone(), "snapshot-0001"))
            .unwrap();

        let wrong_parent = CommitSnapshot {
            parent_snapshot: None,
            ..commit_snapshot_request(table.id.clone(), "snapshot-0002")
        };
        assert!(matches!(
            catalog.commit_snapshot(wrong_parent).unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let second = CommitSnapshot {
            parent_snapshot: Some(first.id.clone()),
            ..commit_snapshot_request(table.id.clone(), "snapshot-0002")
        };
        let committed = catalog.commit_snapshot(second).unwrap();

        assert_eq!(committed.parent, Some(first.id));
        assert_eq!(
            catalog
                .latest_snapshot(&table.tenant, &table.namespace, &table.id)
                .unwrap()
                .id,
            SnapshotId::new("snapshot-0002").unwrap()
        );
    }
}
