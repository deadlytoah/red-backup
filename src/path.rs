use errors::*;
use std::ops::Deref;
use std::path::Path as StdPath;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Clone, Debug, Default)]
pub struct Path {
    prefix: Rc<PathBuf>,
    path: PathBuf,
}

impl Path {
    pub fn with_prefix<P>(prefix: P) -> Self
    where
        P: AsRef<StdPath>,
    {
        Self {
            prefix: Rc::new(prefix.as_ref().into()),
            path: Default::default(),
        }
    }

    pub fn with_template(template: &Path) -> Self {
        Path {
            prefix: Rc::clone(&template.prefix),
            path: Default::default(),
        }
    }

    pub fn path<P>(mut self, path: P) -> Self
    where
        P: AsRef<StdPath>,
    {
        self.path = path.as_ref().into();
        self
    }

    pub fn logical(&self) -> Result<PathBuf> {
        make_logical_path(&*self.prefix, &self.path)
    }

    pub fn canonical(&self) -> Result<PathBuf> {
        self.path
            .canonicalize()
            .chain_err(|| "Path::canonicalize()")
    }

    pub fn is_ancestor(&self, other: &Path) -> Result<bool> {
        Ok(other.canonical()?.starts_with(&self.canonical()?))
    }
}

impl AsRef<StdPath> for Path {
    fn as_ref(&self) -> &StdPath {
        &self.path
    }
}

impl Deref for Path {
    type Target = StdPath;
    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Into<PathBuf> for Path {
    fn into(self) -> PathBuf {
        self.path
    }
}

fn make_logical_path<P, Q>(prefix: P, path: Q) -> Result<PathBuf>
where
    P: AsRef<StdPath>,
    Q: AsRef<StdPath>,
{
    path.as_ref()
        .strip_prefix(&prefix)
        .chain_err(|| {
            format!(
                "{:?} does not start with {:?}",
                path.as_ref(),
                prefix.as_ref()
            )
        })
        .map(|path| path.into())
}
