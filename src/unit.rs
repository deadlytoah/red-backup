use consts::*;
use errors::*;
use path::Path;
use slog::Logger;
use std::fmt::{self, Debug, Formatter};
use std::result::Result as StdResult;

#[derive(Debug, Default)]
pub struct File {
    pub path: Path,
    pub len: u64,
}

impl File {
    pub fn new(path: Path, len: u64) -> Self {
        File { path, len }
    }
}

#[derive(Default)]
pub struct Files(pub Vec<File>);

impl Debug for Files {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), fmt::Error> {
        for file in &self.0 {
            writeln!(f, "    {:?}", file)?;
        }
        Ok(())
    }
}

pub struct Unit {
    pub parent: usize,
    pub len: u64,
    pub path: Path,
    pub files: Files,
}

impl Unit {
    pub fn root(root: Path, log: &Logger) -> Result<Self> {
        Self::new(root, 0, log)
    }

    pub fn new(path: Path, parent: usize, log: &Logger) -> Result<Self> {
        let mut files = vec![];
        let mut len = 0;

        for file in path.read_dir()
            .chain_err(|| format!("error reading directory {:?}", path))?
        {
            let file = file.chain_err(|| format!("error reading directory {:?}", path))?;
            let file_type = file.file_type()
                .chain_err(|| format!("error getting file type of {:?}", file.path()))?;
            let entry_path = Path::with_template(&path).path(file.path());

            if file_type.is_dir() {
                // skip directories
            } else if file_type.is_file()
                || (file_type.is_symlink() && entry_path.exists() && entry_path.is_file())
            {
                let file_len = file.metadata()
                    .chain_err(|| format!("error getting metadata of {:?}", entry_path))?
                    .len();
                len += file_len;
                files.push(File::new(entry_path, file_len));
            } else if file_type.is_symlink() && !entry_path.exists() {
                // warn about broken symlinks that are skipped
                slog_warn!(log, "skip"; "path" => format!("{:?}", entry_path));
            }
        }

        Ok(Unit {
            parent,
            len,
            path,
            files: Files(files),
        })
    }

    pub fn is_small(&self) -> bool {
        for &File { len, .. } in self.files.0.iter().filter(|file| {
            // don't count the hidden files
            !file.path
                .file_name()
                .expect("path unexpectedly points to ..")
                .to_string_lossy()
                .starts_with('.')
        }) {
            if len > SMALL_FILE_UPPER_BOUND {
                return false;
            }
        }
        true
    }

    pub fn merge(&mut self, unit: &mut Unit) {
        self.files.0.append(&mut unit.files.0);
        self.len += unit.len;
        unit.len = 0;
    }
}

impl Debug for Unit {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), fmt::Error> {
        f.debug_struct("Unit")
            .field("parent", &self.parent)
            .field("len", &self.len)
            .field("path", &self.path)
            .field("files", &self.files)
            .finish()
    }
}
