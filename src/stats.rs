use errors::*;
use itertools::Itertools;
use unit::File;

#[derive(Default)]
pub struct Stats {
    pub path_lens: Vec<usize>,
    pub file_sizes: Vec<u64>,
}

impl Stats {
    pub fn new() -> Stats {
        Self::default()
    }

    pub fn files(mut self, files: &[&File]) -> Result<Stats> {
        self.file_sizes = files.iter().map(|file| file.len).collect();
        self.path_lens = files.iter().map(|file| file.path.logical()).fold_results(
            vec![],
            |mut path_lens, logical| {
                path_lens.push(logical.as_os_str().len());
                path_lens
            },
        )?;
        Ok(self)
    }
}
