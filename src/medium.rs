use std::fmt::{self, Display, Formatter};
use unit::File;
use unitset::UnitSet;

#[derive(Debug, Default)]
pub struct Medium {
    id: Option<usize>,
    group_id: Option<usize>,
    pub name: String,
    size: u64,
    len: u64,
    files: Vec<File>,
    redundancy: bool,
}

impl Medium {
    pub fn new(name: &str, size: u64) -> Self {
        Medium {
            id: None,
            group_id: None,
            name: name.into(),
            size,
            len: 0,
            files: Default::default(),
            redundancy: false,
        }
    }

    pub fn redundancy(mut self, redundancy: bool) -> Self {
        self.redundancy = redundancy;
        self
    }

    pub fn unit_set(mut self, units: UnitSet) -> Self {
        self.len = units.len();
        self.files = units.into();
        self
    }

    pub fn set_id(&mut self, id: usize) {
        self.id = Some(id);
    }

    pub fn id(&self) -> usize {
        self.id.expect("Medium::id")
    }

    pub fn set_group_id(&mut self, id: usize) {
        self.group_id = Some(id);
    }

    pub fn group_id(&self) -> usize {
        self.group_id.expect("Medium::group_id")
    }

    pub fn is_redundancy(&self) -> bool {
        self.redundancy
    }

    #[allow(unused)]
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn files(&self) -> &[File] {
        &self.files
    }

    pub fn push_file(&mut self, file: File) {
        self.files.push(file);
    }
}

impl Display for Medium {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{} files using {}/{} in Medium {} {}",
            self.files.len(),
            self.len,
            self.size,
            self.name,
            if self.is_redundancy() {
                "(redundancy)"
            } else {
                ""
            }
        )
    }
}
