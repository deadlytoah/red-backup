use autofill::AutoFill;
use consts::*;
use itertools::Itertools;
use slog::Logger;
use stats::Stats;

const BLOCK_SIZES: &[u64] = &[
    0x200,
    0x400,
    0x800,
    0x1000,
    0x2000,
    0x4000,
    0x8000,
    0x10_000,
    0x20_000,
    0x40_000,
    0x80_000,
    0x100_000,
    0x200_000,
    0x400_000,
    0x800_000,
    0x1_000_000,
    0x2_000_000,
    0x4_000_000,
    0x8_000_000,
];

pub struct BlockSize {
    stats: Stats,
    log: Logger,
}

impl BlockSize {
    pub fn new(stats: Stats, log: &Logger) -> Self {
        BlockSize {
            stats,
            log: log.new(o!("function" => "block_size")),
        }
    }

    pub fn block_size(&self) -> u64 {
        let mut loss_table = vec![];

        AutoFill::new(concat!(
            "For each block size, I lose the following number of",
            " bytes in the redundancy data.  I don't lose these",
            " bytes in the actual backup media."
        )).foreach(|line| slog_info!(&self.log, "{}", line));

        for block_size in BLOCK_SIZES {
            let loss = self.stats
                .file_sizes
                .iter()
                .fold(0, |loss, size| loss + block_size - size % block_size);
            let size_of_tables = estimate_size_of_tables(
                *block_size,
                self.stats.path_lens.iter(),
                self.stats.file_sizes.iter(),
            );
            slog_info!(
                &self.log,
                "block size: {}, loss: {}, record size: {}, size of tables: {}",
                block_size,
                loss,
                RECORD_SIZE,
                size_of_tables
            );
            loss_table.push((*block_size, loss, size_of_tables));
        }

        /*
         * Calculate the optimal block size.
         */
        let optimal = loss_table
            .iter()
            .min_by_key(|&&(_, loss, size_of_tables)| loss + size_of_tables)
            .expect("unexpectedly empty loss_table");
        let sum: u64 = self.stats.file_sizes.iter().sum();
        AutoFill::new(&format!(
            concat!(
                "The optimal block size is {} bytes. ",
                "I lose {} bytes due to inefficiencies and the index",
                " and record tables. ",
                "That is {} percents of the total {} bytes of data."
            ),
            optimal.0,
            optimal.1 + optimal.2,
            (optimal.1 + optimal.2) * 100 / sum,
            sum
        )).foreach(|line| slog_info!(&self.log, "{}", line));

        optimal.0
    }
}

fn estimate_size_of_tables<'a, I1, I2>(block_size: u64, path_lens: I1, sizes: I2) -> u64
where
    I1: Iterator<Item = &'a usize>,
    I2: Iterator<Item = &'a u64>,
{
    let record_table_size = sizes.fold(0, |block_count, size| {
        block_count + (size + block_size - 1) * RECORD_SIZE / block_size
    });
    let index_table_size = path_lens.fold(0, |sum, path_len| {
        sum + *path_len as u64 // NOTE: must times 2 on windows
    });

    record_table_size + index_table_size
}
