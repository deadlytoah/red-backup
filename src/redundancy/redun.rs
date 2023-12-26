use block::{self, BlockIter};
use consts::*;
use crypto::aes;
use crypto::symmetriccipher::SynchronousStreamCipher;
use errors::*;
use index::{self, FileTable};
use medium::Medium;
use path::Path;
use rand::{OsRng, Rng};
use redundancy::{redundancy_copy, Hash};
use sha1::Sha1;
use std::cmp;
use std::collections::HashMap;
use std::io::Write;
use std::mem;
use std::path::Path as StdPath;
use std::path::PathBuf;
use time::get_time;
use unit;
use verifile::Verifile;

pub type EncKey = [u8; 16];
pub type Nonce = [u8; 16];

#[derive(Debug, Deserialize, Serialize)]
pub struct Block {
    nonce: Nonce,
    bytes: Box<[u8]>,
}

impl Block {
    pub fn new(nonce: &[u8], bytes: Box<[u8]>) -> Self {
        let mut inst = Self {
            nonce: Default::default(),
            bytes,
        };
        inst.nonce.copy_from_slice(nonce);
        inst
    }
}

#[derive(Debug)]
pub struct RedunFile {
    key: EncKey,
    file: Verifile,
}

impl RedunFile {
    pub fn new(key: &str) -> Result<Self> {
        Ok(RedunFile {
            key: Default::default(),
            file: Verifile::new(key)?,
        })
    }

    pub fn with_enc_key(mut self, key: &[u8]) -> Self {
        self.key.copy_from_slice(key);
        self
    }

    pub fn path(&self) -> &StdPath {
        self.file.path()
    }

    pub fn write_blocks(&mut self, blocks: &[Block], block_size: usize) -> Result<()> {
        let mut write = self.file.write()?;
        for block in blocks {
            debug_assert_eq!(block.nonce.len(), 16);
            debug_assert_eq!(block.bytes.len(), block_size);

            write
                .write_all(&block.nonce)
                .chain_err(|| format!("error writing to {:?}", write))?;
            write
                .write_all(&encrypt(&block.bytes, &self.key, &block.nonce))
                .chain_err(|| format!("error writing to {:?}", write))?;
        }
        write.close()?;
        Ok(())
    }

    #[cfg(test)]
    fn remove(self) -> Result<()> {
        self.file.remove()
    }
}

#[derive(Debug)]
pub enum PartialIndexKind {
    Redundancy {
        left: index::Block,
        right: index::Block,
    },
    Replication {
        original: index::Block,
    },
}

#[derive(Debug)]
pub struct PartialIndex {
    pub kind: PartialIndexKind,
    pub id: usize,
    pub len: u32,
    pub hash: Hash,
}

#[derive(Debug)]
pub struct Redundancy<'a> {
    block_size: usize,
    left: &'a Medium,
    right: &'a Medium,
    redun: &'a mut Medium,
    file_table: &'a FileTable,
    workdir: PathBuf,
    queue: Vec<Block>,
    partial_indices: HashMap<PathBuf, Vec<PartialIndex>>,
    key: EncKey,
}

impl<'a> Redundancy<'a> {
    pub fn new(
        block_size: usize,
        workdir: &StdPath,
        left: &'a Medium,
        right: &'a Medium,
        redun: &'a mut Medium,
        file_table: &'a FileTable,
    ) -> Self {
        Redundancy {
            block_size,
            left,
            right,
            redun,
            file_table,
            workdir: workdir.into(),
            queue: Default::default(),
            partial_indices: Default::default(),
            key: Default::default(),
        }
    }

    pub fn key(mut self, key: &[u8]) -> Self {
        self.key.copy_from_slice(key);
        self
    }

    pub fn partial_indices(&mut self) -> HashMap<PathBuf, Vec<PartialIndex>> {
        mem::replace(&mut self.partial_indices, Default::default())
    }

    pub fn build(&mut self) -> Result<()> {
        assert!(self.workdir.is_dir());

        // Used to sequentially name temporary files
        let mut counter: usize = 0;

        let left_id = self.left.id();
        let right_id = self.right.id();
        let mut left_iter = BlockIter::new(
            self.block_size,
            self.file_table
                .entries()
                .iter()
                .filter(|file_entry| file_entry.medium_id() == left_id)
                .map(|file_entry| {
                    block::File::new(file_entry.id(), file_entry.actual_path())
                }),
        )?;
        let mut right_iter = BlockIter::new(
            self.block_size,
            self.file_table
                .entries()
                .iter()
                .filter(|file_entry| file_entry.medium_id() == right_id)
                .map(|file_entry| {
                    block::File::new(file_entry.id(), file_entry.actual_path())
                }),
        )?;

        let mut partial_indices: Vec<PartialIndex> = Default::default();

        loop {
            let lblk = left_iter.next_block()?;
            let rblk = right_iter.next_block()?;

            if lblk.is_none() && rblk.is_none() {
                break;
            } else if lblk.is_none() || rblk.is_none() {
                // replication
                let block = lblk.unwrap_or_else(|| rblk.unwrap());
                let mut sha1 = Sha1::new();
                sha1.update(block.data());
                let hash = sha1.digest().bytes();

                assert!(block.data().len() <= u32::max_value() as usize);
                let index = PartialIndex {
                    kind: PartialIndexKind::Replication {
                        original: index::Block::new(
                            block.file_id(),
                            block.block_id(),
                            block.data().len() as u32,
                            &hash,
                        ),
                    },
                    id: self.queue.len(),
                    len: block.data().len() as u32,
                    hash,
                };
                partial_indices.push(index);

                let mut buf = vec![0u8; self.block_size];
                buf[..block.data().len()].copy_from_slice(block.data());

                self.queue.push(Block::new(&generate_nonce()?, buf.into()));
            } else {
                // create redundancy
                let lblk = lblk.unwrap();
                let rblk = rblk.unwrap();

                let mut sha1 = Sha1::new();
                sha1.update(lblk.data());
                let lhash = sha1.digest().bytes();
                sha1.reset();
                sha1.update(rblk.data());
                let rhash = sha1.digest().bytes();

                let mut buf = vec![0u8; cmp::max(lblk.data().len(), rblk.data().len())];
                redundancy_copy(lblk.data(), rblk.data(), &mut buf);
                sha1.reset();
                sha1.update(&buf);
                let redun_hash = sha1.digest().bytes();

                assert!(lblk.data().len() <= u32::max_value() as usize);
                assert!(rblk.data().len() <= u32::max_value() as usize);
                let index = PartialIndex {
                    kind: PartialIndexKind::Redundancy {
                        left: index::Block::new(
                            lblk.file_id(),
                            lblk.block_id(),
                            lblk.data().len() as u32,
                            &lhash,
                        ),
                        right: index::Block::new(
                            rblk.file_id(),
                            rblk.block_id(),
                            rblk.data().len() as u32,
                            &rhash,
                        ),
                    },
                    id: self.queue.len(),
                    len: buf.len() as u32,
                    hash: redun_hash,
                };
                partial_indices.push(index);

                if buf.len() < self.block_size {
                    let pad = self.block_size - buf.len();
                    buf.append(&mut vec![0u8; pad]);
                }

                self.queue.push(Block::new(&generate_nonce()?, buf.into()));
            }

            if self.queue.len() >= MAX_REDUNDANCY_BLOCKS {
                self.write_out_queue(
                    &format!("{:010}", counter),
                    mem::replace(&mut partial_indices, vec![]),
                )?;
                self.queue.clear();
                counter += 1;
            }
        }

        if !self.queue.is_empty() {
            self.write_out_queue(
                &format!("{:010}", counter),
                mem::replace(&mut partial_indices, vec![]),
            )?;
        }

        Ok(())
    }

    fn write_out_queue(&mut self, key: &str, partial_indices: Vec<PartialIndex>) -> Result<()> {
        let path = Path::with_prefix(&self.workdir).path(self.workdir.join(key));
        let mut redun_file =
            RedunFile::new(path.to_str()
                .chain_err(|| format!("utf8 encoding error {:?}", path))?)?
                .with_enc_key(&self.key);
        redun_file.write_blocks(&self.queue, self.block_size)?;

        let path = Path::with_template(&path).path(&redun_file.path());
        self.partial_indices
            .insert(path.to_path_buf(), partial_indices);

        let len = path.metadata()
            .chain_err(|| format!("error getting metadata of {:?}", path))?
            .len();
        self.redun.push_file(unit::File::new(path, len));
        Ok(())
    }
}

fn encrypt(data: &[u8], key: &EncKey, nonce: &Nonce) -> Box<[u8]> {
    let mut buf = vec![0u8; data.len()];
    let mut cipher = aes::ctr(aes::KeySize::KeySize128, key, nonce);
    cipher.process(data, &mut buf);
    buf.into()
}

// Generates random sequence of bytes.
fn generate_random(buf: &mut [u8]) -> Result<()> {
    let mut gen = OsRng::new().chain_err(|| "Failed to get OS random generator")?;
    gen.fill_bytes(buf);
    Ok(())
}

pub fn generate_key() -> Result<Box<[u8]>> {
    let mut v = vec![0u8; 16];
    generate_random(&mut v)?;
    Ok(v.into())
}

pub fn generate_nonce() -> Result<Box<[u8]>> {
    let timespec = get_time();
    let sec_part = (timespec.sec & i64::from(u32::max_value())) as u64;
    let time_part = sec_part << 32 | timespec.nsec as u64;
    let time_part: [u8; 8] = unsafe { mem::transmute(time_part) };

    let mut nonce = vec![0u8; 16];
    nonce[..8].copy_from_slice(&time_part);
    generate_random(&mut nonce[8..])?;
    Ok(nonce.into())
}

#[cfg(test)]
mod test {
    use redundancy::redun::{generate_key, generate_nonce, Block, RedunFile};

    const BLOCK_SIZE: usize = 4096;

    #[test]
    fn test_redun_file() {
        let mut rfile = RedunFile::new("test_redun_file")
            .expect("RedunFile::new")
            .with_enc_key(&generate_key().expect("generate_key"));

        let mut blocks = vec![];

        let mut buf = vec![0u8; BLOCK_SIZE];
        let data = "I can only imagine".as_bytes();
        buf[..data.len()].copy_from_slice(data);
        let block = Block::new(&generate_nonce().expect("generate_nonce"), buf.into());
        blocks.push(block);

        let mut buf = vec![0u8; BLOCK_SIZE];
        let data = "What it will be like".as_bytes();
        buf[..data.len()].copy_from_slice(data);
        let block = Block::new(&generate_nonce().expect("generate_nonce"), buf.into());
        blocks.push(block);

        rfile
            .write_blocks(&blocks, BLOCK_SIZE)
            .expect("write_blocks");
        assert_eq!(
            rfile.file.path().metadata().expect("metadata").len(),
            2 * (BLOCK_SIZE + 16) as u64
        );
        rfile.remove().expect("rfile.remove()");
    }
}
