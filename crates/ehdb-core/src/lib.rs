use std::fmt;

pub use arrow_schema::DataType;

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
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    };
}

identifier_type!(TenantId);
identifier_type!(NamespaceName);
identifier_type!(TableName);
identifier_type!(TableId);
identifier_type!(SnapshotId);
identifier_type!(TransactionId);
identifier_type!(StreamName);
identifier_type!(ConsumerName);
identifier_type!(DocumentId);
identifier_type!(ChunkId);
identifier_type!(EmbeddingModelId);

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[ColumnSchema] {
        &self.columns
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
    fn keeps_arrow_datatypes_in_schema() {
        let schema =
            TableSchema::new(vec![ColumnSchema::new("id", DataType::Utf8, false).unwrap()])
                .unwrap();

        assert_eq!(schema.columns()[0].data_type, DataType::Utf8);
    }
}
