/// Sliding-window clutter map for land / stationary-return suppression.
///
/// Maintains a per-cell count of how many of the last `WINDOW_SWEEPS` sweeps
/// contained a detection in that cell.  Cells whose count exceeds
/// `SUPPRESS_THRESHOLD` are considered stationary (land, docks, fixed buoys)
/// and are filtered out before detections reach the tracker.
///
/// Land appears in every sweep → count quickly reaches the full window.
/// A moving vessel at 5+ knots crosses a ~100 m cell in a few sweeps, so its
/// count stays well below the threshold.
use std::collections::{HashMap, HashSet, VecDeque};

/// Quantisation scale for latitude (units per degree).
/// 1° lat ≈ 111 320 m → scale 1000 → cell height ≈ 111 m.
const LAT_SCALE: f64 = 1000.0;

/// Quantisation scale for longitude (units per degree).
/// At 53 °N, 1° lon ≈ 67 000 m → scale 1000 → cell width ≈ 67 m.
/// Cells are not perfectly square at all latitudes but are good enough for
/// land-suppression purposes.
const LON_SCALE: f64 = 1000.0;

/// Number of sweeps in the sliding window.
pub const WINDOW_SWEEPS: usize = 30;

/// Suppress a cell if it appeared in at least this many of the last
/// `WINDOW_SWEEPS` sweeps (25/30 ≈ 83 %).  Land always scores 30/30;
/// a vessel visible for ≤ 5 sweeps in one cell scores ≤ 5/30.
pub const SUPPRESS_THRESHOLD: u32 = 25;

type Cell = (i32, i32);

#[inline]
fn to_cell(lat: f64, lon: f64) -> Cell {
    ((lat * LAT_SCALE).round() as i32, (lon * LON_SCALE).round() as i32)
}

/// Sliding-window land / clutter filter.
pub struct ClutterMap {
    /// Ring buffer — one `HashSet<Cell>` per past sweep.
    history: VecDeque<HashSet<Cell>>,
    /// Running per-cell hit counts within the window.
    counts: HashMap<Cell, u32>,
}

impl ClutterMap {
    pub fn new() -> Self {
        Self {
            history: VecDeque::with_capacity(WINDOW_SWEEPS + 1),
            counts: HashMap::new(),
        }
    }

    /// Feed one sweep's `(lat, lon)` detections; returns those that are not
    /// suppressed as stationary clutter.
    pub fn filter(&mut self, dets: &[(f64, f64)]) -> Vec<(f64, f64)> {
        // Build this sweep's cell set.
        let current: HashSet<Cell> = dets.iter().map(|&(la, lo)| to_cell(la, lo)).collect();

        // Expire the oldest sweep from the window.
        if self.history.len() >= WINDOW_SWEEPS {
            if let Some(old) = self.history.pop_front() {
                for cell in &old {
                    if let Some(c) = self.counts.get_mut(cell) {
                        if *c <= 1 {
                            self.counts.remove(cell);
                        } else {
                            *c -= 1;
                        }
                    }
                }
            }
        }

        // Accumulate current sweep.
        for &cell in &current {
            *self.counts.entry(cell).or_insert(0) += 1;
        }
        self.history.push_back(current);

        // Return detections whose cell count is below the suppression threshold.
        dets.iter()
            .filter(|&&(la, lo)| {
                self.counts
                    .get(&to_cell(la, lo))
                    .copied()
                    .unwrap_or(0)
                    < SUPPRESS_THRESHOLD
            })
            .copied()
            .collect()
    }

    /// Number of unique cells currently tracked in the window (diagnostic).
    pub fn active_cells(&self) -> usize {
        self.counts.len()
    }
}

impl Default for ClutterMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stationary_target_suppressed_after_window() {
        let mut map = ClutterMap::new();
        let det = (53.178, 5.268);
        // Feed the same detection for WINDOW_SWEEPS + 1 sweeps.
        for _ in 0..=WINDOW_SWEEPS {
            let out = map.filter(&[det]);
            let _ = out; // may or may not be suppressed yet
        }
        // After a full window of the same cell it must be suppressed.
        let out = map.filter(&[det]);
        assert!(out.is_empty(), "stationary detection should be suppressed");
    }

    #[test]
    fn moving_target_not_suppressed() {
        let mut map = ClutterMap::new();
        // Each sweep the detection is in a clearly different cell (~5 km apart).
        for i in 0..WINDOW_SWEEPS {
            let lat = 53.0 + i as f64 * 0.05; // steps of ~5.5 km
            map.filter(&[(lat, 5.0)]);
        }
        // The latest cell has only appeared once — must pass through.
        let lat_new = 53.0 + WINDOW_SWEEPS as f64 * 0.05;
        let out = map.filter(&[(lat_new, 5.0)]);
        assert_eq!(out.len(), 1, "moving target should not be suppressed");
    }

    #[test]
    fn fresh_map_passes_all_detections() {
        let mut map = ClutterMap::new();
        let dets: Vec<(f64, f64)> = (0..10).map(|i| (52.0 + i as f64 * 0.001, 5.0)).collect();
        let out = map.filter(&dets);
        assert_eq!(out.len(), dets.len());
    }
}
