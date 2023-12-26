use errors::*;
use std::fs;
use std::io::{self, Read};
use std::marker::PhantomData;
use std::path::Path as StdPath;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Block {
    file_id: usize,
    block_id: usize,
    data: Box<[u8]>,
}

impl Block {
    pub fn file_id(&self) -> usize {
        self.file_id
    }

    pub fn block_id(&self) -> usize {
        self.block_id
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug)]
pub struct File {
    id: usize,
    path: PathBuf,
}

impl File {
    pub fn new(id: usize, path: &StdPath) -> Self {
        Self {
            id,
            path: path.into(),
        }
    }
}

pub struct BlockIter<'a, I>
where
    I: Iterator<Item = File> + 'a,
{
    block_size: usize,
    file_id: usize,
    block_id: usize,
    path: Option<PathBuf>,
    file: Option<fs::File>,
    file_iter: I,
    phantom: PhantomData<&'a I>,
}

impl<'a, I> BlockIter<'a, I>
where
    I: Iterator<Item = File> + 'a,
{
    pub fn new(block_size: usize, file_iter: I) -> Result<Self>
    where
        I: Iterator<Item = File> + 'a,
    {
        let mut iter = BlockIter {
            block_size,
            file_id: 0,
            block_id: 0,
            path: None,
            file: None,
            file_iter,
            phantom: PhantomData,
        };
        let _ = iter.open_next()?;
        Ok(iter)
    }

    fn open_next(&mut self) -> Result<bool> {
        if let Some(file) = self.file_iter.next() {
            self.file_id = file.id;
            self.file = Some(fs::File::open(&file.path)
                .chain_err(|| format!("error opening {:?}", file.path))?);
            self.path = Some(file.path);
            Ok(true)
        } else {
            self.file = None;
            Ok(false)
        }
    }

    pub fn next_block(&mut self) -> Result<Option<Block>> {
        if self.file.is_none() {
            if self.open_next()? {
                self.block_id = 0;
                self.next_block()
            } else {
                Ok(None)
            }
        } else {
            let mut file = self.file.take().unwrap();
            let mut block = vec![0; self.block_size];
            let mut bytes_read = 0;

            while bytes_read < self.block_size {
                match file.read(&mut block[bytes_read..]) {
                    Ok(byte_count) if byte_count == 0 => break,
                    Ok(byte_count) => bytes_read += byte_count,
                    Err(ref err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    err => {
                        err.chain_err(|| format!("error reading from {:?}", self.path))?;
                    }
                }
            }

            if bytes_read > 0 {
                let _ = block.split_off(bytes_read);
                let retval = Ok(Some(Block {
                    file_id: self.file_id,
                    block_id: self.block_id,
                    data: block.into_boxed_slice(),
                }));

                if !(bytes_read < self.block_size) {
                    self.file = Some(file);
                    self.block_id += 1;
                } else {
                    // end of file
                }

                retval
            } else {
                self.next_block()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use block::{self, BlockIter};
    use index::FileTable;
    use medium::Medium;
    use path::Path;
    use std::fs;
    use std::io::Write;
    use unit;

    #[test]
    fn test_block_iter() {
        let files = setup_files();
        let mut medium = Medium::default();
        medium.set_id(0);
        files.into_iter().for_each(|file| medium.push_file(file));
        let file_table = FileTable::new(&[medium]).expect("FileTable::new");

        let mut iter = BlockIter::new(
            16,
            file_table.entries().iter().map(|file_entry| {
                block::File::new(file_entry.id(), file_entry.actual_path())
            }),
        ).expect("BlockIter::new");
        let mut file_id = 0;
        let mut block_count = 0;
        let blocks: [&[u8]; 4] = [b"1234", b"1234567890123456", b"1234567890123456", b"7890"];

        fn abs(n: isize) -> isize {
            if n < 0 {
                -n
            } else {
                n
            }
        }

        while let Some(block) = iter.next_block().expect("next_block") {
            eprintln!("iteration: {}", block_count + 1);
            assert_eq!(
                block.file_id,
                (-abs(2 * file_id as isize - 5) + 5) as usize / 2
            );
            file_id += 1;
            assert_eq!(block.block_id, (block_count + 1) / 4);
            assert_eq!(block.data.as_ref(), blocks[block_count]);
            block_count += 1;
        }
        teardown_files();
    }

    fn setup_files() -> Vec<unit::File> {
        let mut test1 = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("test1")
            .expect("open");
        let mut test2 = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("test2")
            .expect("open");
        let mut test3 = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("test3")
            .expect("open");
        test1.write(b"1234").expect("write");
        test2.write(b"1234567890123456").expect("write");
        test3.write(b"12345678901234567890").expect("write");
        vec![
            unit::File::new(Path::with_prefix(".").path("./test1"), 0),
            unit::File::new(Path::with_prefix(".").path("./test2"), 0),
            unit::File::new(Path::with_prefix(".").path("./test3"), 0),
        ]
    }

    fn teardown_files() {
        fs::remove_file("test1").expect("remove_file");
        fs::remove_file("test2").expect("remove_file");
        fs::remove_file("test3").expect("remove_file");
    }
}
