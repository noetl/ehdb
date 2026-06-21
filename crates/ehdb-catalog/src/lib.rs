use std::collections::BTreeMap;

use ehdb_core::{
    EhdbError, NamespaceName, Result, TableId, TableName, TableSchema, TenantId, TransactionId,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TableKey {
    tenant: TenantId,
    namespace: NamespaceName,
    name: TableName,
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
pub struct CreateTable {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub name: TableName,
    pub schema: TableSchema,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Default)]
pub struct InMemoryCatalog {
    tables: BTreeMap<TableKey, CatalogTable>,
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

        self.tables.insert(key, table.clone());
        Ok(table)
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
}

#[cfg(test)]
mod tests {
    use ehdb_core::{ColumnSchema, DataType};

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
}
