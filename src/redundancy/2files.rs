extern crate env_logger;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate memmap;
extern crate serde;
extern crate serde_json;
extern crate sha1;
extern crate uuid;
extern crate xor;

use memmap::{Mmap, MmapView, Protection};
use sha1::Sha1;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::mem;
use std::path::Path;
use std::process;
use uuid::{Uuid, UuidVersion};
use xor::{HashedBlock, HashedBlockHeader, Metadata, MetadataBody, METADATA_VERSION, Version,
          redundancy_copy};
use xor::error::{ErrorKind, Result};

fn main_impl(infile1_rpath: &str, infile2_rpath: &str) -> Result<()> {
    let infile1_mmap: Mmap;
    let infile2_mmap: Mmap;
    let mut rfile_header_mmap: MmapView;
    let mut rfile_data_mmap: MmapView;

    let infile1_hash: [u8; 20];
    let infile2_hash: [u8; 20];
    let rfile_hash: [u8; 20];

    let version: Version;

    const METADATA_SUFFIX: &str = ".meta";
    let metadata1_path: String;
    let metadata2_path: String;

    let rfile_path: String;
    let rfile_size: u64;

    // Make sure we have relative paths.
    if infile1_rpath.starts_with('/') || infile2_rpath.starts_with('/') {
        bail!("expecting relative path");
    }

    metadata1_path = infile1_rpath.to_string() + METADATA_SUFFIX;
    metadata2_path = infile2_rpath.to_string() + METADATA_SUFFIX;

    info!(
        "opening {} and {} for reading",
        infile1_rpath,
        infile2_rpath
    );
    let infile1_path = Path::new(&infile1_rpath);
    let infile2_path = Path::new(&infile2_rpath);
    let infile1_metadata = infile1_path.metadata()?;
    let infile2_metadata = infile2_path.metadata()?;
    let infile1_size = infile1_metadata.len();
    let infile2_size = infile2_metadata.len();

    info!("infile1 size: {}", infile1_size);
    info!("infile2 size: {}", infile2_size);

    /*
     * Create memory map over the input and output files.
     */
    infile1_mmap = Mmap::open_path(&infile1_path, Protection::Read)?;
    infile2_mmap = Mmap::open_path(&infile2_path, Protection::Read)?;

    rfile_size = mem::size_of::<Version>() as u64 + Metadata::size_on_disk() +
        HashedBlockHeader::size_on_disk() +
        std::cmp::max(infile1_size, infile2_size);
    rfile_path = Uuid::new(UuidVersion::Random)
        .ok_or_else(|| ErrorKind::from("generate UUID"))?
        .hyphenated()
        .to_string() + ".r";

    let rfile = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&rfile_path)?;
    rfile.set_len(rfile_size)?;
    let split = Mmap::open(&rfile, Protection::ReadWrite)?
        .into_view()
        .split_at(
            mem::size_of::<Version>() + Metadata::size_on_disk() as usize,
        )?;
    rfile_header_mmap = split.0;
    rfile_data_mmap = split.1;

    {
        // create the hashed block container
        let mut block = HashedBlock::new(
            unsafe { &mut rfile_data_mmap.as_mut_slice() },
            std::cmp::max(infile1_size, infile2_size) as usize,
        )?;
        debug_assert_eq!(
            block.as_slice().len() as u64,
            std::cmp::max(infile1_size, infile2_size)
        );

        unsafe {
            redundancy_copy(
                infile1_mmap.as_slice(),
                infile2_mmap.as_slice(),
                block.as_mut_slice(),
            );
        }

        block.hash_update();
    }

    version = METADATA_VERSION;

    info!("generating hash");

    /*
     * Calculate the hashes and write the header in the redundancy
     * file.  Also create metadata files for the input files.
     */
    let mut sha = Sha1::new();
    sha.update(unsafe { infile1_mmap.as_slice() });
    infile1_hash = sha.digest().bytes();
    sha.reset();
    sha.update(unsafe { infile2_mmap.as_slice() });
    infile2_hash = sha.digest().bytes();
    rfile_hash = HashedBlock::from(unsafe { &mut rfile_data_mmap.as_mut_slice() })?
        .header()
        .hash();
    let header = Metadata::new(
        infile1_hash,
        infile2_hash,
        rfile_hash,
        infile1_rpath,
        infile2_rpath,
        &rfile_path,
    );

    let mut header_bytes =
        vec![0u8; mem::size_of::<HashedBlockHeader>() + MetadataBody::size_on_disk() as usize];
    {
        let mut block = HashedBlock::new(
            &mut header_bytes.as_mut_slice(),
            MetadataBody::size_on_disk() as usize,
        )?;
        header.serialise(&mut block.as_mut_slice())?;
        block.hash_update();
    }

    info!("writing redundancy file header");

    {
        let mut rfile_header_slice = unsafe { rfile_header_mmap.as_mut_slice() };
        version.serialise(&mut rfile_header_slice)?;
        rfile_header_slice.copy_from_slice(&header_bytes);
    }

    info!("creating metadata files");

    let mut metadata1_file = OpenOptions::new().write(true).create(true).open(
        metadata1_path,
    )?;
    let mut metadata2_file = OpenOptions::new().write(true).create(true).open(
        metadata2_path,
    )?;
    version.serialise(&mut metadata1_file)?;
    version.serialise(&mut metadata2_file)?;
    metadata1_file.write_all(&header_bytes)?;
    metadata2_file.write_all(&header_bytes)?;

    /*
     * Flush out the modifications.
     */
    infile1_mmap.flush()?;
    infile2_mmap.flush()?;
    rfile_data_mmap.flush()?;
    rfile_header_mmap.flush()?;

    Ok(())
}

fn main() {
    env_logger::init().unwrap();

    if let Some(infile1) = env::args().nth(1) {
        if let Some(infile2) = env::args().nth(2) {
            match main_impl(&infile1, &infile2) {
                Ok(_) => (),
                err @ Err(_) => {
                    error!("error in main thread {:?}", err);
                    process::exit(1);
                }
            }
        } else {
            usage();
            process::exit(1);
        }
    } else {
        usage();
        process::exit(1);
    }
}

fn usage() {
    eprintln!(
        "Usage: {} <first file> <second file>",
        env::args().next().unwrap(),
    );
}
