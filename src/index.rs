#[cfg(feature = "binary-tables")]
use bincode;
use errors::*;
use medium::Medium;
use serde::{Deserialize, Serialize};
#[cfg(not(feature = "binary-tables"))]
use serde_json;
use std::io::{Read, Write};
use std::path::Path as StdPath;
use std::path::PathBuf;
use unit::File;

#[derive(Debug, Deserialize, Serialize)]
pub struct MediaTable {
    identifier: &'static str,
    table: Vec<(usize, String)>,
}

impl MediaTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, medium: &Medium) -> usize {
        let id = self.table.len();
        self.table.push((id, medium.name.clone()));
        id
    }
}

impl Default for MediaTable {
    fn default() -> Self {
        Self {
            identifier: "Media Index Table",
            table: Default::default(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FileEntry {
    id: usize,
    medium_id: usize,
    path: String,
    size: u64,
    #[serde(skip)] actual_path: PathBuf,
}

impl FileEntry {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn medium_id(&self) -> usize {
        self.medium_id
    }

    #[allow(unused)]
    pub fn path(&self) -> &str {
        &self.path
    }

    #[allow(unused)]
    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn actual_path(&self) -> &StdPath {
        &self.actual_path
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FileTable {
    identifier: &'static str,
    table: Vec<FileEntry>,
}

impl FileTable {
    pub fn new(media: &[Medium]) -> Result<Self> {
        let mut file_index = FileTable::default();

        for medium in media {
            file_index.add_medium(medium)?;
        }

        Ok(file_index)
    }

    pub fn add_medium(&mut self, medium: &Medium) -> Result<()> {
        for file in medium.files().iter() {
            let _ = self.add(medium, file)?;
        }
        Ok(())
    }

    pub fn add(&mut self, medium: &Medium, file: &File) -> Result<usize> {
        let id = self.table.len();
        self.table.push(FileEntry {
            id,
            medium_id: medium.id(),
            path: file.path
                .logical()?
                .to_str()
                .ok_or_else(|| ErrorKind::from("utf8 error"))?
                .into(),
            size: file.len,
            actual_path: file.path.to_path_buf(),
        });
        Ok(id)
    }

    pub fn entries(&self) -> &[FileEntry] {
        &self.table
    }
}

impl Default for FileTable {
    fn default() -> Self {
        Self {
            identifier: "File Index Table",
            table: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Block {
    file: usize,
    block: usize,
    size: u32,
    hash: [u8; 20],
}

impl Block {
    pub fn new(file: usize, block: usize, size: u32, hash: &[u8]) -> Self {
        let mut block = Self {
            file,
            block,
            size,
            hash: Default::default(),
        };
        block.hash.copy_from_slice(hash);
        block
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum RedundancyIndex {
    Redundancy {
        left: Block,
        right: Block,
        redundancy: Block,
    },
    Replication {
        original: Block,
        replication: Block,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RedundancyTable {
    identifier: &'static str,
    table: Vec<RedundancyIndex>,
}

impl RedundancyTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, index: RedundancyIndex) {
        self.table.push(index);
    }
}

impl Default for RedundancyTable {
    fn default() -> Self {
        Self {
            identifier: "Redundancy Index Table",
            table: Default::default(),
        }
    }
}

#[cfg(not(feature = "binary-tables"))]
pub fn deserialise<R, T>(read: R) -> Result<T>
where
    R: Read,
    T: for<'a> Deserialize<'a>,
{
    serde_json::from_reader(read).chain_err(|| "deserialisation")
}

#[cfg(not(feature = "binary-tables"))]
pub fn serialise<W, T>(write: W, table: &T) -> Result<()>
where
    W: Write,
    T: Serialize,
{
    serde_json::to_writer_pretty(write, table).chain_err(|| "serialisation")
}

#[cfg(feature = "binary-tables")]
pub fn deserialise<R, T>(mut read: R) -> Result<T>
where
    R: Read,
    T: for<'a> Deserialize<'a>,
{
    bincode::deserialize_from(&mut read, bincode::Infinite).chain_err(|| "deserialisation")
}

#[cfg(feature = "binary-tables")]
pub fn serialise<W, T>(mut write: W, table: &T) -> Result<()>
where
    W: Write,
    T: Serialize,
{
    bincode::serialize_into(&mut write, table, bincode::Infinite).chain_err(|| "serialisation")
}
