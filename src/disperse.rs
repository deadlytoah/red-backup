use errors::*;
use itertools::Itertools;
use slog::Logger;
use std::mem;
use unitset::UnitSet;

#[derive(Debug)]
pub struct Disperse<'a> {
    media: &'a mut [UnitSet],
    mean: f64,
    goal: f64,

    log: Logger,
}

impl<'a> Disperse<'a> {
    pub fn new(media: &'a mut [UnitSet], goal: f64, log: &Logger) -> Self {
        let sum: u64 = media.iter().map(UnitSet::len).sum();
        let mean: f64 = sum as f64 / media.len() as f64;
        Disperse {
            media,
            mean,
            goal,
            log: log.new(o!("function" => "disperse", "mean" => mean)),
        }
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn measure(&self) -> f64 {
        measure_impl(
            self.mean,
            &self.media.iter().map(UnitSet::len).collect::<Vec<_>>(),
        )
    }

    pub fn is_goal_met(&self) -> bool {
        self.measure() / self.mean * 100.0 < self.goal
    }

    pub fn disperse(&mut self) {
        let mut iteration = 0u32;

        while !self.is_goal_met() {
            iteration += 1;

            let mut candidates = vec![];
            for medium_index in 0..self.media.len() - 1 {
                if self.media[medium_index].len() > 0 {
                    if let Ok(candidate) = Candidate::new(self.media, medium_index, self.mean()) {
                        candidates.push(candidate);
                    } else {
                        slog_warn!(self.log, "discard candidate: results in an empty medium");
                    }
                }
            }

            if !candidates.is_empty() {
                let best_candidate = candidates
                    .iter()
                    .minmax_by_key(|candidate| candidate.value)
                    .into_option()
                    .expect("there aren't any candidates")
                    .0;

                /*
                let mut msg: String = Default::default();
                for medium in self.media.iter() {
                    msg.push_str(&format!(
                        "{}/{}, ",
                        medium.0.len(),
                        medium.len()
                    ));
                }
                slog_debug!(self.log, "{}", msg);
                slog_debug!(
                    self.log,
                    "Among {:?} the best candidate is {:?}",
                    candidates,
                    best_candidate
                );
                 */

                if best_candidate.value < self.measure() {
                    // unwrap() because Candidate::new() would catch
                    // such cases.
                    best_candidate.execute(self.media).unwrap();
                } else {
                    slog_info!(self.log, "discard candidate: no improvements";
                          o!("iteration" => iteration));
                    break;
                }
            } else {
                slog_info!(self.log, "no more candidates"; o!("iteration" => iteration));
                break;
            }
        }
    }
}

#[derive(Debug)]
struct Candidate {
    from_medium: usize,
    value: f64,
}

impl Candidate {
    fn new(media: &[UnitSet], from_medium: usize, mean: f64) -> Result<Candidate> {
        let last_unit = media[from_medium]
            .0
            .last()
            .ok_or_else(|| ErrorKind::EmptyUnitSet)?;
        let new_lens: Vec<_> = media
            .iter()
            .map(UnitSet::len)
            .enumerate()
            .map(|(i, len)| {
                if i == from_medium {
                    len - last_unit.len
                } else if i == from_medium + 1 {
                    len + last_unit.len
                } else {
                    len
                }
            })
            .collect();
        let value = measure_impl(mean, &new_lens);

        Ok(Candidate { from_medium, value })
    }

    fn execute(&self, media: &mut [UnitSet]) -> Result<()> {
        let mut to_medium = mem::replace(&mut media[self.from_medium + 1], Default::default());
        media[self.from_medium].shift_to(&mut to_medium)?;
        mem::replace(&mut media[self.from_medium + 1], to_medium);
        Ok(())
    }
}

fn measure_impl(mean: f64, media_lens: &[u64]) -> f64 {
    (media_lens
        .iter()
        .map(|len| *len as f64 - mean)
        .map(|sub| sub.powi(2))
        .sum::<f64>() / (media_lens.len() - 1) as f64)
        .sqrt()
}
