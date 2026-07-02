use std::{collections::BTreeSet, fmt};

pub use arrow_schema::DataType;
use serde::{de, Deserialize, Deserializer, Serialize};

pub type Result<T> = std::result::Result<T, EhdbError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EhdbError {
    InvalidIdentifier(String),
    InvalidState(String),
    AlreadyExists(String),
    NotFound(String),
    Storage(String),
}

impl fmt::Display for EhdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EhdbError::InvalidIdentifier(value) => write!(f, "invalid identifier: {value}"),
            EhdbError::InvalidState(value) => write!(f, "invalid state: {value}"),
            EhdbError::AlreadyExists(value) => write!(f, "already exists: {value}"),
            EhdbError::NotFound(value) => write!(f, "not found: {value}"),
            EhdbError::Storage(value) => write!(f, "storage error: {value}"),
        }
    }
}

impl std::error::Error for EhdbError {}

macro_rules! identifier_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                validate_identifier(&value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

identifier_type!(TenantId);
identifier_type!(NamespaceName);
identifier_type!(TableName);
identifier_type!(TableId);
identifier_type!(SnapshotId);
identifier_type!(TransactionId);
identifier_type!(PrincipalId);
identifier_type!(StreamName);
identifier_type!(ConsumerName);
identifier_type!(DocumentId);
identifier_type!(ChunkId);
identifier_type!(EmbeddingModelId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnSchema {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Result<Self> {
        let name = name.into();
        validate_identifier(&name)?;
        Ok(Self {
            name,
            data_type,
            nullable,
        })
    }
}

impl<'de> Deserialize<'de> for ColumnSchema {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ColumnSchemaJson {
            name: String,
            data_type: DataType,
            nullable: bool,
        }

        let value = ColumnSchemaJson::deserialize(deserializer)?;
        Self::new(value.name, value.data_type, value.nullable).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableSchema {
    columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(columns: Vec<ColumnSchema>) -> Result<Self> {
        if columns.is_empty() {
            return Err(EhdbError::InvalidIdentifier(
                "table schema requires at least one column".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        for column in &columns {
            validate_identifier(&column.name)?;
            if !seen.insert(column.name.as_str()) {
                return Err(EhdbError::InvalidIdentifier(format!(
                    "duplicate table schema column: {}",
                    column.name
                )));
            }
        }
        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[ColumnSchema] {
        &self.columns
    }
}

impl<'de> Deserialize<'de> for TableSchema {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TableSchemaJson {
            columns: Vec<ColumnSchema>,
        }

        let value = TableSchemaJson::deserialize(deserializer)?;
        Self::new(value.columns).map_err(de::Error::custom)
    }
}

fn validate_identifier(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');

    if valid {
        Ok(())
    } else {
        Err(EhdbError::InvalidIdentifier(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_identifiers() {
        assert!(TenantId::new("tenant-a").is_ok());
        assert!(TenantId::new("tenant a").is_err());
    }

    #[test]
    fn identifier_json_decode_rejects_invalid_values() {
        let tenant = TenantId::new("tenant-a").unwrap();
        let value = serde_json::to_value(&tenant).unwrap();
        assert_eq!(serde_json::from_value::<TenantId>(value).unwrap(), tenant);

        assert!(serde_json::from_value::<TenantId>(serde_json::json!("tenant a")).is_err());
        assert!(serde_json::from_value::<TableName>(serde_json::json!("table.name")).is_err());
        assert!(serde_json::from_value::<TransactionId>(serde_json::json!("txn bad")).is_err());
    }

    #[test]
    fn keeps_arrow_datatypes_in_schema() {
        let schema =
            TableSchema::new(vec![ColumnSchema::new("id", DataType::Utf8, false).unwrap()])
                .unwrap();

        assert_eq!(schema.columns()[0].data_type, DataType::Utf8);
    }

    #[test]
    fn rejects_duplicate_table_schema_columns() {
        let error = TableSchema::new(vec![
            ColumnSchema::new("id", DataType::Utf8, false).unwrap(),
            ColumnSchema::new("id", DataType::Int64, false).unwrap(),
        ])
        .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));
    }

    #[test]
    fn rejects_invalid_preconstructed_table_schema_columns() {
        let error = TableSchema::new(vec![ColumnSchema {
            name: "bad column".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
        }])
        .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));
    }

    #[test]
    fn schema_json_decode_rejects_invalid_columns_and_duplicates() {
        let mut column =
            serde_json::to_value(ColumnSchema::new("id", DataType::Utf8, false).unwrap()).unwrap();
        column["name"] = serde_json::json!("bad column");
        assert!(serde_json::from_value::<ColumnSchema>(column).is_err());

        let mut schema = serde_json::to_value(
            TableSchema::new(vec![ColumnSchema::new("id", DataType::Utf8, false).unwrap()])
                .unwrap(),
        )
        .unwrap();
        schema["columns"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "name": "id",
                "data_type": "Int64",
                "nullable": false
            }));
        assert!(serde_json::from_value::<TableSchema>(schema).is_err());
    }
}
