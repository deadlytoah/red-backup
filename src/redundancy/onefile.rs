extern crate env_logger;
#[macro_use]
extern crate log;
extern crate memmap;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate sha1;
extern crate xor;

use memmap::{Mmap, MmapView, Protection};
use sha1::Sha1;
use std::env;
use std::fs::{self, OpenOptions};
use std::mem;
use std::path::Path;
use std::process;
use std::ptr;
use std::slice;
use xor::redundancy;

#[derive(Clone)]
#[repr(C)]
struct Metadata {
    orig_size: u64,
    part_type: PartType,
    hash1: [u8; 20],
    hash2: [u8; 20],
    hashr: [u8; 20],
    extended_length: usize,
}

impl Metadata {
    fn new(orig_size: u64) -> Self {
        Metadata {
            orig_size,
            part_type: PartType::Part1,
            hash1: [0u8; 20],
            hash2: [0u8; 20],
            hashr: [0u8; 20],
            extended_length: 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ExtendedHeader {
    perm: u32,
    uid: u32,
    gid: u32,
    ctime: i64,
    mtime: i64,
}

impl ExtendedHeader {
    #[cfg(target_os = "macos")]
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        ExtendedHeader {
            perm: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            ctime: metadata.ctime(),
            mtime: metadata.mtime(),
        }
    }
}

#[derive(Clone)]
#[repr(C)]
enum PartType {
    Part1 = 0,
    Part2,
    Redundancy,
}

struct State {
    read_mapped: bool,
    write_mapped: bool,

    orig_size: u64,
    part_size: u64,

    read_mmap: Option<Mmap>,
    metadata1_mmap: Option<MmapView>,
    data1_mmap: Option<MmapView>,
    metadata2_mmap: Option<MmapView>,
    data2_mmap: Option<MmapView>,
    metadatar_mmap: Option<MmapView>,
    datar_mmap: Option<MmapView>,
}

impl State {
    pub fn new() -> Self {
        State {
            read_mapped: false,
            write_mapped: false,
            orig_size: 0,
            part_size: 0,
            read_mmap: None,
            metadata1_mmap: None,
            data1_mmap: None,
            metadata2_mmap: None,
            data2_mmap: None,
            metadatar_mmap: None,
            datar_mmap: None,
        }
    }
}

fn main() {
    let mut state = State::new();

    env_logger::init().unwrap();

    if let Some(filename) = env::args().nth(1) {
        info!("opening {} for reading", filename);
        let path = Path::new(&filename);
        let read_metadata = path.metadata().expect("metadata");
        state.orig_size = read_metadata.len();
        state.part_size = (state.orig_size + 1) / 2;

        info!("input file size: {}", state.orig_size);
        info!("metadata size: {}", mem::size_of::<Metadata>());
        info!(
            "part size: {}",
            mem::size_of::<Metadata>() as u64 + state.part_size
        );

        /*
         * Create memory map over the input and output files.
         */
        let read_mmap = Mmap::open_path(&path, Protection::Read).expect("mmap::open_path");
        state.read_mmap = Some(read_mmap);
        state.read_mapped = true;

        let extheader = ExtendedHeader::from_metadata(&read_metadata);
        let extheader_json = serde_json::to_string(&extheader).expect("serialize extended header");
        let extheader_len = extheader_json.len();

        let partname = filename.clone() + ".1";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(partname)
            .expect("create file");
        file.set_len(
            mem::size_of::<Metadata>() as u64 + extheader_len as u64 + state.part_size,
        ).expect("set_len");
        let split = Mmap::open(&file, Protection::ReadWrite)
            .expect("mmap::open")
            .into_view()
            .split_at(mem::size_of::<Metadata>() + extheader_len)
            .expect("split MmapView");
        state.metadata1_mmap = Some(split.0);
        state.data1_mmap = Some(split.1);

        let partname = filename.clone() + ".2";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(partname)
            .expect("create file");
        file.set_len(
            mem::size_of::<Metadata>() as u64 + extheader_len as u64 + state.part_size,
        ).expect("set_len");
        let split = Mmap::open(&file, Protection::ReadWrite)
            .expect("mmap::open")
            .into_view()
            .split_at(mem::size_of::<Metadata>() + extheader_len)
            .expect("split MmapView");
        state.metadata2_mmap = Some(split.0);
        state.data2_mmap = Some(split.1);

        let partname = filename.clone() + ".r";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(partname)
            .expect("create file");
        file.set_len(
            mem::size_of::<Metadata>() as u64 + extheader_len as u64 + state.part_size,
        ).expect("set_len");
        let split = Mmap::open(&file, Protection::ReadWrite)
            .expect("mmap::open")
            .into_view()
            .split_at(mem::size_of::<Metadata>() + extheader_len)
            .expect("split MmapView");
        state.metadatar_mmap = Some(split.0);
        state.datar_mmap = Some(split.1);

        state.write_mapped = true;

        /*
         * Copy data over, splitting them over the two files.
         */
        unsafe {
            let source_ptr = state.read_mmap.as_ref().map(|mmap| mmap.ptr()).expect(
                "unwrap read_mmap ptr",
            );
            let data1_ptr = state
                .data1_mmap
                .as_mut()
                .map(|mmap| mmap.mut_ptr())
                .expect("unwrap data1_mmap mut_ptr");
            let data2_ptr = state
                .data2_mmap
                .as_mut()
                .map(|mmap| mmap.mut_ptr())
                .expect("unwrap data2_mmap mut_ptr");

            debug!("source ptr: {:x}", source_ptr as usize);
            debug!("data1 ptr: {:x}", data1_ptr as usize);
            debug!("data2 ptr: {:x}", data2_ptr as usize);
            debug!("part size: {}", state.part_size);

            ptr::copy_nonoverlapping(source_ptr, data1_ptr, state.part_size as usize);
            ptr::copy_nonoverlapping(
                source_ptr.offset(state.part_size as isize),
                data2_ptr,
                state.part_size as usize,
            );
        }

        /*
         * Create the redundancy data.
         */
        let data1: &[u8] = unsafe {
            mem::transmute(
                state
                    .data1_mmap
                    .as_ref()
                    .map(|mmap| mmap.as_slice())
                    .expect("unwrap data1_mmap slice"),
            )
        };
        let data2: &[u8] = unsafe {
            mem::transmute(
                state
                    .data2_mmap
                    .as_ref()
                    .map(|mmap| mmap.as_slice())
                    .expect("unwrap data2_mmap slice"),
            )
        };
        let datar: &mut [u8] = unsafe {
            mem::transmute(
                state
                    .datar_mmap
                    .as_mut()
                    .map(|mmap| mmap.as_mut_slice())
                    .expect("unwrap datar_mmap mut_slice"),
            )
        };

        redundancy(data1, data2, datar);

        /*
         * Calculate the hashes and write metadata in all three files.
         */
        let mut metadata = Metadata::new(state.orig_size);
        metadata.extended_length = extheader_len;
        let mut sha = Sha1::new();
        unsafe {
            sha.update(
                state
                    .data1_mmap
                    .as_ref()
                    .map(|mmap| mmap.as_slice())
                    .expect("unwrap slice"),
            );
        }
        metadata.hash1 = sha.digest().bytes();
        unsafe {
            sha.update(
                state
                    .data2_mmap
                    .as_ref()
                    .map(|mmap| mmap.as_slice())
                    .expect("unwrap slice"),
            );
        }
        metadata.hash2 = sha.digest().bytes();
        unsafe {
            sha.update(
                state
                    .datar_mmap
                    .as_ref()
                    .map(|mmap| mmap.as_slice())
                    .expect("unwrap slice"),
            );
        }
        metadata.hashr = sha.digest().bytes();

        unsafe {
            metadata.part_type = PartType::Part1;
            let target: *mut Metadata = mem::transmute(
                state
                    .metadata1_mmap
                    .as_mut()
                    .map(|mmap| mmap.mut_ptr())
                    .expect("unwrap metadata1_mmap as_mut_slice"),
            );
            *target = metadata.clone();
            metadata.part_type = PartType::Part2;
            let target: *mut Metadata = mem::transmute(
                state
                    .metadata2_mmap
                    .as_mut()
                    .map(|mmap| mmap.mut_ptr())
                    .expect("unwrap metadata2_mmap as_mut_slice"),
            );
            *target = metadata.clone();
            metadata.part_type = PartType::Redundancy;
            let target: *mut Metadata = mem::transmute(
                state
                    .metadatar_mmap
                    .as_mut()
                    .map(|mmap| mmap.mut_ptr())
                    .expect("unwrap metadatar_mmap as_mut_slice"),
            );
            *target = metadata.clone();
        }

        unsafe {
            let target: &mut [u8] = slice::from_raw_parts_mut(
                state
                    .metadata1_mmap
                    .as_mut()
                    .map(|mmap| {
                        mmap.mut_ptr().offset(mem::size_of::<Metadata>() as isize)
                    })
                    .expect("unwrap metadata1_mmap as_mut_slice"),
                extheader_len,
            );
            target.copy_from_slice(extheader_json.as_bytes());
            let target: &mut [u8] = slice::from_raw_parts_mut(
                state
                    .metadata2_mmap
                    .as_mut()
                    .map(|mmap| {
                        mmap.mut_ptr().offset(mem::size_of::<Metadata>() as isize)
                    })
                    .expect("unwrap metadata2_mmap as_mut_slice"),
                extheader_len,
            );
            target.copy_from_slice(extheader_json.as_bytes());
            let target: &mut [u8] = slice::from_raw_parts_mut(
                state
                    .metadatar_mmap
                    .as_mut()
                    .map(|mmap| {
                        mmap.mut_ptr().offset(mem::size_of::<Metadata>() as isize)
                    })
                    .expect("unwrap metadatar_mmap as_mut_slice"),
                extheader_len,
            );
            target.copy_from_slice(extheader_json.as_bytes());
        }

        /*
         * Flush out the modifications.
         */
        state
            .data1_mmap
            .as_ref()
            .expect("unwrap data1_mmap")
            .flush()
            .expect("flush data1_mmap");
        state
            .data2_mmap
            .as_ref()
            .expect("unwrap data2_mmap")
            .flush()
            .expect("flush data2_mmap");
        state
            .datar_mmap
            .as_ref()
            .expect("unwrap datar_mmap")
            .flush()
            .expect("flush datar_mmap");
        state
            .metadata1_mmap
            .as_ref()
            .expect("unwrap metadata1_mmap")
            .flush()
            .expect("flush metadata1_mmap");
        state
            .metadata2_mmap
            .as_ref()
            .expect("unwrap metadata2_mmap")
            .flush()
            .expect("flush metadata2_mmap");
        state
            .metadatar_mmap
            .as_ref()
            .expect("unwrap metadatar_mmap")
            .flush()
            .expect("flush metadatar_mmap");
    } else {
        eprintln!("expect a file name");
        process::exit(1);
    }
}
