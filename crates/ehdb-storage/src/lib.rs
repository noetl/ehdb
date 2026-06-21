use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use ehdb_core::{EhdbError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPath(String);

impl ObjectPath {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let safe = !path.is_empty()
            && !path.starts_with('/')
            && !path.contains("..")
            && path
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '_' | '-' | '.'));

        if safe {
            Ok(Self(path))
        } else {
            Err(EhdbError::Storage(format!("unsafe object path: {path}")))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    pub path: ObjectPath,
    pub len: u64,
}

pub trait ImmutableObjectStore {
    fn put_if_absent(&self, path: ObjectPath, bytes: &[u8]) -> Result<ObjectRef>;
    fn get(&self, path: &ObjectPath) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, path: &ObjectPath) -> PathBuf {
        self.root.join(Path::new(path.as_str()))
    }
}

impl ImmutableObjectStore for LocalObjectStore {
    fn put_if_absent(&self, path: ObjectPath, bytes: &[u8]) -> Result<ObjectRef> {
        let target = self.resolve(&path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        file.write_all(bytes)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        Ok(ObjectRef {
            path,
            len: bytes.len() as u64,
        })
    }

    fn get(&self, path: &ObjectPath) -> Result<Vec<u8>> {
        fs::read(self.resolve(path)).map_err(|err| EhdbError::Storage(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_root() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ehdb-storage-test-{suffix}"))
    }

    #[test]
    fn writes_and_reads_immutable_object() {
        let root = temp_root();
        let store = LocalObjectStore::new(&root);
        let path = ObjectPath::new("tenant-a/system/executions/part-000.arrow").unwrap();

        let object = store
            .put_if_absent(path.clone(), b"arrow-ipc-placeholder")
            .unwrap();
        let bytes = store.get(&path).unwrap();

        assert_eq!(object.len, 21);
        assert_eq!(bytes, b"arrow-ipc-placeholder");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_overwrite() {
        let root = temp_root();
        let store = LocalObjectStore::new(&root);
        let path = ObjectPath::new("tenant-a/system/executions/part-000.arrow").unwrap();

        store.put_if_absent(path.clone(), b"first").unwrap();
        let error = store.put_if_absent(path, b"second").unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_unsafe_object_paths() {
        assert!(ObjectPath::new("../secret").is_err());
        assert!(ObjectPath::new("/absolute/path").is_err());
        assert!(ObjectPath::new("tenant a/object").is_err());
    }
}
