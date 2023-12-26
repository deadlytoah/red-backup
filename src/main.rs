// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]

#[cfg(feature = "binary-tables")]
extern crate bincode;
#[macro_use]
extern crate cfg_if;
extern crate clap;
extern crate crypto;
#[macro_use]
extern crate error_chain;
extern crate itertools;
extern crate libc;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate sha1;
#[macro_use(o, kv, slog_kv, slog_log, slog_record, slog_record_static, slog_b, slog_info,
            slog_error, slog_warn, slog_debug, slog_crit)]
extern crate slog;
extern crate slog_async;
extern crate slog_json;
#[macro_use]
extern crate slog_scope;
extern crate slog_term;
extern crate tempdir;
extern crate tempfile;
extern crate time;
extern crate verifile;

mod autofill;
mod block;
mod block_size;
mod consts;
mod disperse;
mod errors;
mod index;
mod layout;
mod medium;
mod path;
mod redundancy;
mod stats;
mod unit;
mod unitset;

use autofill::AutoFill;
use block_size::BlockSize;
use clap::{App, Arg};
use consts::*;
use disperse::Disperse;
use error_chain::{ChainedError, ExitCode};
use errors::*;
use index::{Block, FileTable, MediaTable, RedundancyIndex, RedundancyTable};
use itertools::Itertools;
use layout::Layout;
use medium::Medium;
use path::Path;
use redundancy::{generate_key, PartialIndexKind, Redundancy};
use slog::{Drain, Logger};
use stats::Stats;
use std::fs::OpenOptions;
use std::io::Write;
use unitset::UnitSet;
use verifile::Verifile;

fn disperse_over(est_media_count: usize, sets: &mut Vec<UnitSet>, log: &Logger) -> Result<()> {
    assert!(sets.len() <= est_media_count);
    while sets.len() < est_media_count {
        sets.push(Default::default());
    }

    let mut disperse = Disperse::new(sets, 5., log);
    disperse.disperse();

    let autofill = AutoFill::new(&format!(
        concat!(
            "After dispersing, standard deviation of the used space is {:.2}M.",
            "  This is {:.2} percents of the mean size of {:.2}MB."
        ),
        disperse.measure() / 1024. / 1024.,
        disperse.measure() * 100.0 / disperse.mean(),
        disperse.mean() / 1024. / 1024.
    ));
    autofill.foreach(|line| slog_info!(log, "{}", line));

    Ok(())
}

fn run(log: &Logger) -> Result<()> {
    info!("started");

    let matches = App::new("Redundant Backup")
        .arg(
            Arg::with_name("WORK-DIR")
                .short("w")
                .long("work-dir")
                .help("Use the specified directory as work directory")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("START-PATH")
                .required(true)
                .index(1)
                .help("Specify the source directory for a backup"),
        )
        .arg(
            Arg::with_name("MEDIUM-SIZE")
                .required(true)
                .index(2)
                .help("Specify the size of the backup media")
                .validator(|arg| {
                    arg.parse::<u64>()
                        .map(|_| ())
                        .map_err(|_| "expecting the size of the backup media in MiB".into())
                }),
        )
        .get_matches();

    let work_dir = matches.value_of("WORK-DIR");
    let start_path = matches.value_of("START-PATH").unwrap();
    let medium_size: u64 = matches
        .value_of("MEDIUM-SIZE")
        .unwrap()
        .parse::<u64>()
        .unwrap() * 1024 * 1024;

    let mut unit_set = UnitSet::from_path(Path::with_prefix(&start_path).path(&start_path), log)?;
    debug_assert_eq!(unit_set.len(), unit_set.0.iter().fold(0, |s, u| s + u.len));
    let plan = unit_set.plan_merges();
    unit_set.execute_merges(&plan);

    if let Some(unit) = unit_set.0.iter().find(|unit| unit.len > medium_size) {
        crit!("Unit with length larger than the medium size isn't supported.");
        error!("Unit that caused the error: {:?}", unit);
        panic!("Unit with length larger than the medium size isn't supported.");
    }

    let mut est_media_count = ((unit_set.len() + (medium_size - 1)) / medium_size) as usize;
    info!("{}", unit_set);
    info!("estimated media count: {}", est_media_count);
    if est_media_count & 0x1 == 0x1 {
        // make even number
        est_media_count += 1;
    }

    let mut sets = Vec::with_capacity(est_media_count);
    sets.push(unit_set);
    loop {
        disperse_over(est_media_count, &mut sets, log)?;
        sets.iter().for_each(|set| info!("{}", set));

        if sets.iter().all(|set| set.len() <= medium_size) {
            break;
        } else {
            info!("{} media weren't enough.", est_media_count);
            est_media_count += 2;
        }
    }

    let medium_names = vec![
        "Apple",
        "Avocado",
        "Banana",
        "Blueberry",
        "Cherry",
        "Cranberry",
    ];
    let mut medium_name_iter = medium_names.iter();

    let media: Vec<_> = sets.into_iter()
        .map(|unit_set| {
            Medium::new(
                medium_name_iter
                    .next()
                    .expect("we ran out of names for media"),
                medium_size,
            ).unit_set(unit_set)
        })
        .collect();

    // Insert redundancy media.
    let mut media: Vec<_> = media
        .into_iter()
        .enumerate()
        .flat_map(|(n, item)| {
            if n & 1 == 1 {
                vec![
                    item,
                    Medium::new(
                        medium_name_iter
                            .next()
                            .expect("we ran out of names for media"),
                        medium_size,
                    ).redundancy(true),
                ]
            } else {
                vec![item]
            }
        })
        .collect();

    media.iter().foreach(|medium| slog_info!(log, "{}", medium));

    let block_size = {
        /*
         * I've got the list of file sizes here, and I can use the
         * list to calculate the optimal block size.  This block size
         * is fixed for all the media in this backup.
         */
        let files: Vec<_> = media.iter().flat_map(Medium::files).collect();

        /*
         *Calculate the optimal block size.
         */
        let stats = Stats::new().files(&files)?;
        BlockSize::new(stats, log).block_size()
    };

    let mut layout = Layout::new(&start_path, log)?;
    if let Some(work_dir) = work_dir {
        layout.force_location(work_dir)?;
    }

    for (group_id, group) in media.chunks_mut(3).enumerate() {
        let mut media_table = MediaTable::new();
        let enckey = generate_key()?;
        for medium in group.iter_mut() {
            medium.set_group_id(group_id);
            let id = media_table.add(medium);
            medium.set_id(id);
        }

        let mut file_table = FileTable::new(group)?;
        let mut redun_table = RedundancyTable::new();

        info!(
            "build redundancy for: {} and {}",
            group[0].name,
            group[1].name
        );
        let redun_dir = layout
            .dir(REDUNDANCY_SUBDIR)
            .dir(format!("{}", group_id))
            .ensure()?
            .to_owned();
        let (media, redun) = group.split_at_mut(2);
        let partial_indices = {
            let mut redundancy = Redundancy::new(
                block_size as usize,
                &redun_dir,
                &media[0],
                &media[1],
                &mut redun[0],
                &file_table,
            ).key(&enckey);
            redundancy.build()?;
            redundancy.partial_indices()
        };

        info!("build redundancy index table");
        for file in redun[0].files() {
            let file_id = file_table.add(&redun[0], file)?;
            let partial_indices = &partial_indices[&file.path.to_path_buf()];
            for partial_index in partial_indices {
                let index = match partial_index.kind {
                    PartialIndexKind::Redundancy { left, right } => RedundancyIndex::Redundancy {
                        left,
                        right,
                        redundancy: Block::new(
                            file_id,
                            partial_index.id,
                            partial_index.len,
                            &partial_index.hash,
                        ),
                    },
                    PartialIndexKind::Replication { original } => RedundancyIndex::Replication {
                        original,
                        replication: Block::new(
                            file_id,
                            partial_index.id,
                            partial_index.len,
                            &partial_index.hash,
                        ),
                    },
                };
                redun_table.add(index);
            }
        }

        info!("write index tables");
        let index_dir = layout
            .dir(INDEX_SUBDIR)
            .dir(format!("{}", group_id))
            .ensure()?
            .to_owned();
        let mut media_table_file = Verifile::new(index_dir.join("media-table").to_str().unwrap())?;
        let mut write = media_table_file.write()?;
        index::serialise(&mut write, &media_table)?;
        write.close()?;
        let mut file_table_file = Verifile::new(index_dir.join("file-table").to_str().unwrap())?;
        let mut write = file_table_file.write()?;
        index::serialise(&mut write, &file_table)?;
        write.close()?;
        let mut redun_table_file = Verifile::new(index_dir.join("redun-table").to_str().unwrap())?;
        let mut write = redun_table_file.write()?;
        index::serialise(&mut write, &redun_table)?;
        write.close()?;

        info!("write encryption key");
        let enc_key_dir = layout
            .dir(ENCRYPTION_KEY_SUBDIR)
            .dir(format!("{}", group_id))
            .ensure()?
            .to_owned();
        let mut enc_key_file = Verifile::new(enc_key_dir.join("encryption-key").to_str().unwrap())?;
        let mut write = enc_key_file.write()?;
        write
            .write(&enckey)
            .chain_err(|| format!("error writing to {:?}", write))?;
        write.close()?;
    }

    info!("link files in appropriate locations");
    for medium in &media {
        for file in medium.files() {
            let logical = file.path.logical()?;
            layout
                .dir(LAYOUT_SUBDIR)
                .dir(&medium.name)
                .dir(FILES_SUBDIR)
                .file(logical)?
                .link(&file.path.canonical()?);
        }

        // link the index tables
        let index_dir = layout
            .dir(INDEX_SUBDIR)
            .dir(format!("{}", medium.group_id()))
            .to_owned();
        layout
            .dir(LAYOUT_SUBDIR)
            .dir(&medium.name)
            .link_all(&index_dir)?;

        // link the encryption key, only for media that are not
        // redundancy.
        if !medium.is_redundancy() {
            let enc_key_dir = layout
                .dir(ENCRYPTION_KEY_SUBDIR)
                .dir(format!("{}", medium.group_id()))
                .to_owned();
            layout
                .dir(LAYOUT_SUBDIR)
                .dir(&medium.name)
                .link_all(&enc_key_dir)?;
        }
    }

    info!("build layout");
    layout.materialise()?;
    #[cfg(not(feature = "debug"))]
    layout.close()?;

    info!("finished");
    Ok(())
}

fn main_log() -> i32 {
    let log_file_json = OpenOptions::new()
        .create(true)
        .append(true)
        .open(LOG_PATH_JSON)
        .expect(&format!("failed to open log file {}", LOG_PATH_JSON));
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(LOG_PATH)
        .expect(&format!("failed to open log file {}", LOG_PATH));

    let term_decor = slog_term::TermDecorator::new().build();
    let file_drain = slog_json::Json::default(log_file_json);
    let term_drain = slog_term::CompactFormat::new(term_decor).build().fuse();
    let plain_decor = slog_term::PlainSyncDecorator::new(log_file);
    let plain_drain = slog_term::CompactFormat::new(plain_decor).build().fuse();
    let drain = slog_async::Async::new(
        slog::Duplicate::new(
            slog::LevelFilter::new(term_drain, slog::Level::Warning),
            slog::Duplicate::new(file_drain, plain_drain),
        ).fuse(),
    ).chan_size(1_000_000)
        .build()
        .fuse();

    let log = Logger::root(drain, o!());
    let _scope_guard = slog_scope::set_global_logger(log.new(o!()));

    match run(&log) {
        Ok(ret) => ExitCode::code(ret),
        Err(ref e) => {
            slog_error!(&log, "{}", ChainedError::display_chain(e));
            1
        }
    }
}

fn main() {
    ::std::process::exit(main_log());
}
