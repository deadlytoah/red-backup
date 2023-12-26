use consts::*;
use errors::*;
use itertools::Itertools;
use libc;
use slog::Logger;
use std::cell::RefCell;
use std::env;
use std::fs;
use std::mem;
use std::ops::Deref;
#[cfg(target_family = "unix")]
use std::os::unix::fs::MetadataExt;
use std::path::Path as StdPath;
use std::path::PathBuf;
use std::rc::Rc;
use tempdir::TempDir;

#[derive(Debug, Default)]
struct Location {
    path: PathBuf,
    source: PathBuf,
    same_fs: bool,
    writable: bool,
}

impl Location {
    fn new(path: &StdPath, source: &StdPath) -> Result<Self> {
        let mut location = Location {
            path: path.into(),
            source: source.into(),
            same_fs: false,
            writable: false,
        };

        if location.check_writable() {
            location.writable = true;
            location.same_fs = location.check_same_fs()?;
        } else {
            // location is not writable
        }

        Ok(location)
    }

    fn check_writable(&self) -> bool {
        match self.path.metadata() {
            Ok(metadata) => !metadata.permissions().readonly(),
            err => {
                error!("error getting metadata of {:?}: {:?}", self.path, err);
                false
            }
        }
    }

    fn check_same_fs(&self) -> Result<bool> {
        if self.path == self.source {
            Ok(true)
        } else if cfg!(target_family = "unix") {
            // check if source directory and temporary directory are
            // in the same file system.
            let source_metadata = self.source
                .metadata()
                .chain_err(|| format!("error getting metadata of {:?}", self.source))?;
            match self.path.metadata() {
                Ok(metadata) => Ok(source_metadata.dev() == metadata.dev()),
                err => {
                    error!("error getting metadata of {:?}: {:?}", self.path, err);
                    Ok(false)
                }
            }
        } else {
            // Not implemented on other platforms yet, just assume
            // target is on another file system.
            Ok(false)
        }
    }
}

#[derive(Debug, Default)]
struct LocationOptions(Vec<Location>, usize);

#[derive(Debug)]
pub struct Layout {
    locations: LocationOptions,
    closed: bool,

    // pairs of target directory and items that will be placed into
    // it.
    orders: Vec<(PathBuf, Item)>,

    temp_dir: Option<TempDir>,
    log: Logger,
}

impl Layout {
    pub fn new<P: AsRef<StdPath>>(source_dir: P, log: &Logger) -> Result<Self> {
        let log = log.new(o!("function" => "layout"));

        let source_dir = source_dir.as_ref();
        let locations = vec![
            Location::new(&env::temp_dir(), source_dir)?,
            Location::new(source_dir, source_dir)?,
        ];

        // select one of the locations
        let mut selection = locations
            .iter()
            .enumerate()
            .find(|&(_, option)| option.same_fs && option.writable)
            .map(|(index, _)| index);
        if selection.is_none() {
            selection = locations
                .iter()
                .enumerate()
                .find(|&(_, option)| !option.same_fs && option.writable)
                .map(|(index, _)| index);
        }
        let selection = selection.chain_err(|| "none of the locations are writable")?;

        let temp_dir = TempDir::new_in(&locations[selection].path, WORK_DIR).chain_err(|| {
            format!("error making a temp dir in {:?}", locations[selection].path)
        })?;
        slog_info!(&log, "Create temporary directory: {:?}", temp_dir.path());

        Ok(Layout {
            locations: LocationOptions(locations, selection),
            closed: false,
            orders: Default::default(),
            temp_dir: Some(temp_dir),
            log,
        })
    }

    pub fn force_location<P: AsRef<StdPath>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();
        let source = self.locations.0.pop().expect("locations not set up").source;
        self.locations = LocationOptions(vec![Location::new(path, &source)?], 0);
        self.temp_dir = Some(TempDir::new_in(path, WORK_DIR)
            .chain_err(|| format!("error making a temp dir in {:?}", path))?);
        slog_info!(&self.log, "Force: {:?}", self.temp_dir.as_ref().unwrap());
        Ok(())
    }

    pub fn location(&self) -> PathBuf {
        self.temp_dir
            .as_ref()
            .map(|temp_dir| temp_dir.path().to_owned())
            .unwrap()
    }

    pub fn dir<P: AsRef<StdPath>>(&mut self, dir: P) -> Dir {
        let path = self.location().join(dir.as_ref());
        Dir(Rc::new(RefCell::new(self)), path)
    }

    pub fn materialise(&mut self) -> Result<()> {
        for (dir, orders) in &mem::replace(&mut self.orders, vec![])
            .into_iter()
            .group_by(|&(ref dir, _)| dir.clone())
        {
            fs::create_dir_all(&dir).chain_err(|| format!("error making directory {:?}", dir))?;
            for order in orders {
                let item = (order.1).source;
                let dest = dir.join((order.1).name);
                match fs::hard_link(&item, &dest) {
                    Err(err) => match err.raw_os_error() {
                        Some(errno) if errno == libc::EXDEV => {
                            slog_debug!(&self.log, "file from another filesystem";
                                       o!("path" => format!("{:?}", item)));
                            fallback_to_copy(&item, &dest)?;
                            Ok(())
                        }
                        _ => Err(err),
                    },
                    ok => ok,
                }.chain_err(|| format!("error hard-linking {:?} to {:?}", item, dest))?;
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        let temp_dir = self.temp_dir.take().unwrap();

        // We are making a copy of the path for the unlikely case of
        // an IO error, but closing Layout only happens once so we are
        // not too worried about it.
        let path = temp_dir.path().to_owned();

        temp_dir
            .close()
            .chain_err(|| format!("error closing temp dir {:?}", path))
    }
}

cfg_if! {
    if #[cfg(not(feature = "debug"))] {
        impl Drop for Layout {
            fn drop(&mut self) {
                if !self.closed {
                    if let Err(err) = self.close() {
                        slog_error!(&self.log, "error closing Layout {:?}", err);
                    }
                }
            }
        }
    } else {
        impl Drop for Layout {
            fn drop(&mut self) {
                if !self.closed {
                    let _ = self.temp_dir.take().unwrap().into_path();
                }
            }
        }
    }
}

#[derive(Debug)]
struct Item {
    name: String,
    source: PathBuf,
}

#[derive(Debug)]
pub struct Dir<'a>(Rc<RefCell<&'a mut Layout>>, PathBuf);

impl<'a> Dir<'a> {
    pub fn ensure(self) -> Result<Dir<'a>> {
        fs::create_dir_all(&self.1).chain_err(|| format!("error making directory {:?}", self.1))?;
        Ok(self)
    }

    pub fn dir<P: AsRef<StdPath>>(self, dir: P) -> Dir<'a> {
        Dir(Rc::clone(&self.0), self.1.join(dir))
    }

    pub fn file<P: AsRef<StdPath>>(self, file: P) -> Result<File<'a>> {
        let path = self.1.join(file);
        let parent = path.parent().expect("path is unexpectedly a root");
        Ok(File(
            Rc::clone(&self.0),
            parent.into(),
            path.file_name()
                .expect("path unexpectedly points to ..")
                .to_str()
                .chain_err(|| format!("utf8 encoding error {:?}", path.file_name()))?
                .into(),
        ))
    }

    pub fn link_all(self, path: &StdPath) -> Result<Dir<'a>> {
        for entry in path.read_dir()
            .chain_err(|| format!("error reading directory {:?}", path))?
        {
            let entry = entry.chain_err(|| format!("error reading directory {:?}", path))?;
            self.0.borrow_mut().orders.push((
                self.1.clone(),
                Item {
                    name: entry
                        .file_name()
                        .to_str()
                        .chain_err(|| format!("utf8 encoding error {:?}", entry.file_name()))?
                        .into(),
                    source: entry.path(),
                },
            ));
        }
        Ok(self)
    }
}

impl<'a> Deref for Dir<'a> {
    type Target = StdPath;
    fn deref(&self) -> &Self::Target {
        self.1.as_path()
    }
}

#[derive(Debug)]
pub struct File<'a>(Rc<RefCell<&'a mut Layout>>, PathBuf, String);

impl<'a> File<'a> {
    pub fn link(self, path: &StdPath) {
        self.0.borrow_mut().orders.push((
            self.1,
            Item {
                name: self.2,
                source: path.into(),
            },
        ));
    }
}

impl<'a> Deref for File<'a> {
    type Target = StdPath;
    fn deref(&self) -> &Self::Target {
        self.1.as_path()
    }
}

fn fallback_to_copy<P, Q>(from: P, to: Q) -> Result<()>
where
    P: AsRef<StdPath>,
    Q: AsRef<StdPath>,
{
    fs::copy(&from, &to).map(|_| ()).chain_err(|| {
        format!("error copying {:?} to {:?}", from.as_ref(), to.as_ref())
    })
}
