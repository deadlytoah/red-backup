use errors::*;
use path::Path;
use slog::Logger;
use std::fmt::{self, Debug, Display, Formatter};
use std::fs::{DirEntry, ReadDir};
use std::io;
use std::iter::Enumerate;
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::slice::Iter;
use unit::{File, Unit};

#[derive(Debug)]
struct StackUnitItem {
    index: usize,
    cursor: ReadDir,
    path: PathBuf,
}

impl StackUnitItem {
    fn new(path: &Path, index: usize) -> Result<Self> {
        match path.read_dir() {
            Ok(cursor) => Ok(StackUnitItem {
                index,
                cursor,
                path: path.as_ref().into(),
            }),
            Err(err) => Err(Error::with_chain(
                err,
                format!("error reading directory {:?}", path),
            )),
        }
    }
}

impl Iterator for StackUnitItem {
    type Item = StdResult<DirEntry, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next()
    }
}

#[derive(Default)]
pub struct UnitSet(pub Vec<Unit>, u64);

impl UnitSet {
    pub fn from_path(root: Path, log: &Logger) -> Result<Self> {
        let mut set = vec![];
        let mut stack = vec![];
        let mut len;

        stack.push(StackUnitItem::new(&root, 0)?);
        let root = Unit::root(root, log)?;
        len = root.len;
        set.push(root);

        while !stack.is_empty() {
            if let Some(dir_entry) = stack.last_mut().unwrap().next() {
                let dir_entry = dir_entry.chain_err(|| {
                    format!("error reading directory {:?}", stack.last().unwrap().path)
                })?;
                let path = Path::with_template(&set.first().unwrap().path).path(dir_entry.path());
                let file_type = dir_entry.file_type().chain_err(|| {
                    format!("error getting file type of {:?}", dir_entry.path())
                })?;
                let log = log.new(o!("path" => format!("{:?}", path)));

                if file_type.is_dir()
                    || (file_type.is_symlink() && path.exists()
                        && path.read_link()
                            .chain_err(|| format!("error reading symlink {:?}", path))?
                            .is_dir())
                {
                    let parent = stack.last().unwrap().index;

                    if file_type.is_symlink() && detect_cycle(&path, parent, &set)? {
                        slog_warn!(log, "detected a cycle");
                    } else {
                        stack.push(StackUnitItem::new(&path, set.len())?);
                        let unit = Unit::new(path, parent, &log)?;
                        len += unit.len;
                        set.push(unit);
                    }
                } else {
                    // Pass because we are only interested in
                    // directories or symlnks to directories.
                }
            } else {
                let _ = stack.pop().expect("unexpectedly empty stack");
            }
        }

        Ok(UnitSet(set, len))
    }

    pub fn len(&self) -> u64 {
        self.1
    }

    pub fn plan_merges(&self) -> Vec<(usize, usize)> {
        let mut plan = vec![];

        for (index, small) in self.small_units() {
            let mut ancestor = small.parent;
            while ancestor != 0 {
                if !self.0[ancestor].is_small() {
                    break;
                } else {
                    ancestor = self.0[ancestor].parent;
                }
            }

            if index != ancestor {
                plan.push((index, ancestor));
            }
        }

        plan
    }

    /* Merges the units according to the given plan.  The plan is a list
     * of a pair of indices into the UnitSet.  The first item in the pair
     * is to be merged into the second.
     * */
    pub fn execute_merges(&mut self, plan: &[(usize, usize)]) {
        for &(merge, into) in plan {
            assert_ne!(merge, into);
            unsafe {
                // Okay because merge and into are guaranteed not to refer
                // to the same element due to the assertion above.
                let into_unit = &mut self.0[into] as *mut Unit;
                (*into_unit).merge(&mut self.0[merge]);
            }
        }

        for &(merge, _) in plan.iter().rev() {
            assert_eq!(self.0[merge].files.0.len(), 0);
            let _ = self.0.remove(merge);
        }
    }

    pub fn shift_from(&mut self, unit_set: &mut UnitSet) {
        assert!(!unit_set.0.is_empty());
        let first = unit_set.0.remove(0);
        self.1 += first.len;
        self.0.push(first);
    }

    pub fn undo_shift(&mut self, unit_set: &mut UnitSet) {
        let len = self.0.len();
        assert!(len > 0);
        let last = self.0.remove(len - 1);
        self.1 -= last.len;
        unit_set.0.insert(0, last);
    }

    pub fn shift_to(&mut self, other: &mut UnitSet) -> Result<()> {
        let last = self.0.pop().ok_or_else(|| ErrorKind::EmptyUnitSet)?;
        self.1 -= last.len;
        other.1 += last.len;
        other.0.insert(0, last);
        Ok(())
    }

    pub fn small_units(&self) -> SmallUnits {
        SmallUnits(self.0.iter().enumerate())
    }
}

pub struct SmallUnits<'a>(Enumerate<Iter<'a, Unit>>);

impl<'a> Iterator for SmallUnits<'a> {
    type Item = (usize, &'a Unit);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((i, unit)) = self.0.next() {
            if unit.is_small() {
                Some((i, unit))
            } else {
                self.next()
            }
        } else {
            None
        }
    }
}

impl From<UnitSet> for Vec<File> {
    fn from(set: UnitSet) -> Vec<File> {
        set.0.into_iter().flat_map(|unit| unit.files.0).collect()
    }
}

impl Debug for UnitSet {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), fmt::Error> {
        writeln!(f, "UnitSet {{ len: {},", self.1)?;
        for child in &self.0 {
            writeln!(f, "  {:?}", child)?;
        }
        writeln!(f, "}}")
    }
}

impl Display for UnitSet {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), fmt::Error> {
        write!(
            f,
            "UnitSet containing {} units taking {} bytes",
            self.0.len(),
            self.1
        )
    }
}

fn detect_cycle(path: &Path, parent: usize, set: &[Unit]) -> Result<bool> {
    let mut finger = &set[parent];

    loop {
        if path.is_ancestor(&finger.path)? || path.canonical()? == finger.path.canonical()? {
            return Ok(true);
        }

        let prev_parent = finger.parent;
        finger = &set[finger.parent];

        if prev_parent == finger.parent {
            debug_assert_eq!(finger.parent, 0);
            break;
        }
    }

    Ok(false)
}
