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

use memmap::{Mmap, Protection};
use sha1::Sha1;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::mem;
use std::path::{Path, PathBuf};
use std::process;
use std::str;
use xor::error::{self, ErrorKind, Result};
use xor::{METADATA_SUFFIX, RFILE_SUFFIX, Hash, HashedBlockState, Metadata, Version, hashed_blocks};

fn metadata_deserialise_from_bytes(bytes: &mut &mut [u8]) -> Result<Metadata> {
    let block = hashed_blocks(bytes).next().ok_or_else(
        || "metadata block is not found",
    )?;

    let mut infile_header = Metadata::new(
        Hash::default(),
        Hash::default(),
        Hash::default(),
        "",
        "",
        "",
    );

    match block {
        HashedBlockState::Intact(ref block) => {
            infile_header.deserialise(&mut block.as_slice())?;
            Ok(infile_header)
        }
        HashedBlockState::Invalid(_) => Err("invalid metadata".into()),
        HashedBlockState::Incomplete => Err("incomplete metadata".into()),
    }
}

fn verify_blob(blob: &[u8], hash: Hash) -> Result<()> {
    let mut sha1 = Sha1::new();
    sha1.update(blob);
    let blob_hash = sha1.digest().bytes();
    debug!("blob_hash = {:?}", sha1.digest().to_string());
    if hash == blob_hash {
        Ok(())
    } else {
        Err(ErrorKind::VerificationFailure.into())
    }
}

fn with_metadata_suffix<P: AsRef<Path>>(path: P) -> PathBuf {
    let mut new_path = path.as_ref().to_owned();
    let new_ext: OsString = match new_path.extension() {
        Some(ext) => {
            let mut new_ext: OsString = ext.to_owned();
            new_ext.push(METADATA_SUFFIX);
            new_ext
        }
        None => METADATA_SUFFIX.into(),
    };
    new_path.set_extension(new_ext);
    new_path
}

fn check_redundancy(infile_rpath: &str) -> Result<()> {
    let mut infile_mmap: Mmap;
    let mut infile_slice: &mut [u8];

    let mut version: Version;

    infile_mmap = Mmap::open_path(infile_rpath, Protection::Read)?;;
    infile_slice = unsafe { infile_mmap.as_mut_slice() };

    info!("verifying version");
    version = Default::default();
    version.deserialise(&mut &*infile_slice)?;
    version.verify_version()?;

    let mut iter = hashed_blocks(&mut infile_slice[mem::size_of::<Version>()..]);

    info!("verifying header");
    match iter.next() {
        Some(HashedBlockState::Incomplete) => Err("incomplete header block").into(),
        Some(HashedBlockState::Invalid(_)) => {
            warn!("invalid header block");
            Ok(())
        }
        Some(HashedBlockState::Intact(_)) => Ok(()),
        None => Err("missing header block"),
    }?;

    info!("verifying redundancy data");
    let block = iter.next().ok_or_else(|| "missing content block")?;

    match block {
        HashedBlockState::Intact(_) => Ok(()),
        HashedBlockState::Invalid(_) => Err("invalid redundancy block".into()),
        HashedBlockState::Incomplete => Err("incomplete redundancy block".into()),
    }
}

fn check_file(infile_rpath: &str) -> Result<()> {
    let mut infile_metadata_mmap: Mmap;
    let infile_data_mmap: Mmap;
    let mut infile_metadata_slice: &mut [u8];

    let mut version: Version;
    let infile_header: Metadata;

    let separate_metadata_path = with_metadata_suffix(&infile_rpath);

    infile_metadata_mmap = Mmap::open_path(separate_metadata_path, Protection::Read)?;;
    infile_data_mmap = Mmap::open_path(&infile_rpath, Protection::Read)?;
    infile_metadata_slice = unsafe { infile_metadata_mmap.as_mut_slice() };

    info!("verifying version");
    version = Default::default();
    version.deserialise(&mut &*infile_metadata_slice)?;
    version.verify_version()?;

    info!("reading metadata");
    infile_header = metadata_deserialise_from_bytes(
        &mut &mut infile_metadata_slice[mem::size_of::<Version>()..],
    )?;

    info!("calculating checksum");
    let hash = if infile_rpath == infile_header.rpath1()? {
        debug!("matching hash 1 {:?}", infile_header.hash1());
        infile_header.hash1()
    } else if infile_rpath == infile_header.rpath2()? {
        debug!("matching hash 2 {:?}", infile_header.hash2());
        infile_header.hash2()
    } else {
        bail!("couldn't find a matching hash in the metadata");
    };

    verify_blob(unsafe { infile_data_mmap.as_slice() }, hash)
}

fn main_impl(infile_rpath: &str) -> Result<()> {
    // Make sure we have relative paths.
    if infile_rpath.starts_with('/') {
        bail!("expecting relative path");
    }

    let result = if with_metadata_suffix(infile_rpath).exists() {
        check_file(infile_rpath)
    } else if infile_rpath.ends_with(RFILE_SUFFIX) {
        check_redundancy(infile_rpath)
    } else {
        bail!("no metadata")
    };

    let intact = match result {
        Ok(()) => true,
        Err(error::Error(ErrorKind::VerificationFailure, _)) => false,
        err => bail!("error {:?}", err),
    };

    println!(
        "{} is {}intact",
        infile_rpath,
        if intact { "" } else { "not " }
    );

    Ok(())
}

fn main() {
    env_logger::init().unwrap();

    if let Some(infile) = env::args().nth(1) {
        match main_impl(&infile) {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "error in main thread [{}] backtrace: {:?} cause: {:?}",
                    e,
                    e.backtrace(),
                    e.cause(),
                );
                process::exit(1);
            }
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
