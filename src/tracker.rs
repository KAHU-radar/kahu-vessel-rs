/// Target tracking: associates per-spoke detections into tracks across sweeps.
///
/// Uses a simple nearest-neighbour gate.  Each sweep's detections are matched
/// to existing tracks; unmatched detections birth new tracks; tracks that go
/// ungated for too long are declared lost and ready to flush.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::geo::haversine_m;

/// Maximum distance (m) a detection may be from a track's *predicted* position
/// and still be considered the same target.  At 1 Hz, a 30-knot vessel moves
/// ~15 m/sweep — 200 m gives ample headroom while preventing cross-track swaps
/// when ships pass within a few hundred metres of each other.
const GATE_M: f64 = 200.0;

/// Number of consecutive sweeps a track can go unupdated before it is lost.
const MAX_MISSED_SWEEPS: u32 = 8;

/// Minimum fixes required before a track is worth uploading.
pub const MIN_FIXES: usize = 3;

/// Segment a track once it reaches this many fixes, even if still active.
/// Prevents unbounded memory growth and enables regular uploads during long
/// radar sessions. At ~1 Hz sweep rate, 20 = first upload within ~20 s.
const MAX_TRACK_FIXES: usize = 20;

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Fix {
    pub lat: f64,
    pub lon: f64,
    pub ts: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Track {
    pub id: Uuid,
    pub fixes: Vec<Fix>,
    missed: u32,
}

impl Track {
    fn new(fix: Fix) -> Self {
        Self {
            id: Uuid::new_v4(),
            fixes: vec![fix],
            missed: 0,
        }
    }

    fn last(&self) -> &Fix {
        self.fixes.last().expect("track always has at least one fix")
    }

    /// Dead-reckoning: predict position at `now` using the velocity estimated
    /// from the last two fixes.  Falls back to the last fix when there is only
    /// one fix or the inter-fix interval is zero.
    fn predicted_pos(&self, now: DateTime<Utc>) -> (f64, f64) {
        let n = self.fixes.len();
        if n < 2 {
            let l = self.last();
            return (l.lat, l.lon);
        }
        let f1 = &self.fixes[n - 2];
        let f2 = &self.fixes[n - 1];
        let dt_hist = (f2.ts - f1.ts).num_milliseconds() as f64 / 1000.0;
        if dt_hist <= 0.0 {
            return (f2.lat, f2.lon);
        }
        let vlat = (f2.lat - f1.lat) / dt_hist;
        let vlon = (f2.lon - f1.lon) / dt_hist;
        let dt_pred = (now - f2.ts).num_milliseconds() as f64 / 1000.0;
        let dt_pred = dt_pred.max(0.0);
        (f2.lat + vlat * dt_pred, f2.lon + vlon * dt_pred)
    }

    fn distance_to(&self, lat: f64, lon: f64, now: DateTime<Utc>) -> f64 {
        let (plat, plon) = self.predicted_pos(now);
        haversine_m(plat, plon, lat, lon)
    }
}

// ── Tracker ───────────────────────────────────────────────────────────────────

/// Manages all active tracks and returns completed ones.
pub struct Tracker {
    tracks: Vec<Track>,
}

impl Tracker {
    pub fn new() -> Self {
        Self { tracks: Vec::new() }
    }

    /// Feed one sweep's worth of (lat, lon) detections into the tracker.
    ///
    /// Returns any tracks that have just been declared lost (ready to upload).
    pub fn update(&mut self, detections: &[(f64, f64)], ts: DateTime<Utc>) -> Vec<Track> {
        // --- match detections to existing tracks (greedy nearest-neighbour) ---
        let mut matched = vec![false; self.tracks.len()];
        let mut used = vec![false; detections.len()];

        for (det_i, &(lat, lon)) in detections.iter().enumerate() {
            let best = self
                .tracks
                .iter()
                .enumerate()
                .filter(|(t_i, _)| !matched[*t_i])
                .map(|(t_i, t)| (t_i, t.distance_to(lat, lon, ts)))
                .filter(|(_, d)| *d < GATE_M)
                .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap());

            if let Some((t_i, _)) = best {
                self.tracks[t_i].fixes.push(Fix { lat, lon, ts });
                self.tracks[t_i].missed = 0;
                matched[t_i] = true;
                used[det_i] = true;
            }
        }

        // --- birth new tracks for unmatched detections ---
        for (det_i, &(lat, lon)) in detections.iter().enumerate() {
            if !used[det_i] {
                self.tracks.push(Track::new(Fix { lat, lon, ts }));
            }
        }

        // --- increment missed counter for original unmatched tracks only ---
        // (newly birthed tracks start at missed=0 and must not be indexed here)
        for t_i in 0..matched.len() {
            if !matched[t_i] {
                self.tracks[t_i].missed += 1;
            }
        }

        // --- drain lost tracks, collecting them for the caller ---
        let mut lost = Vec::new();
        let mut i = 0;
        while i < self.tracks.len() {
            if self.tracks[i].missed >= MAX_MISSED_SWEEPS {
                lost.push(self.tracks.remove(i));
            } else {
                i += 1;
            }
        }

        // --- segment long tracks: flush accumulated fixes and keep last as seed ---
        // Prevents unbounded memory growth and enables periodic uploads without
        // requiring the target to disappear first.
        for track in self.tracks.iter_mut() {
            if track.fixes.len() >= MAX_TRACK_FIXES {
                let last_fix = track.fixes.last().expect("track always has at least one fix").clone();
                let old_fixes = std::mem::replace(&mut track.fixes, vec![last_fix]);
                let old_id = std::mem::replace(&mut track.id, Uuid::new_v4());
                lost.push(Track { id: old_id, fixes: old_fixes, missed: 0 });
            }
        }

        lost
    }

    /// Flush all remaining active tracks (e.g. on shutdown / reconnect).
    pub fn drain(&mut self) -> Vec<Track> {
        std::mem::take(&mut self.tracks)
    }

    pub fn active_count(&self) -> usize {
        self.tracks.len()
    }
}

impl Default for Tracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn single_detection_births_track() {
        let mut tracker = Tracker::new();
        let lost = tracker.update(&[(52.0, 4.0)], ts());
        assert!(lost.is_empty());
        assert_eq!(tracker.active_count(), 1);
    }

    #[test]
    fn repeated_detection_extends_track() {
        let mut tracker = Tracker::new();
        tracker.update(&[(52.0, 4.0)], ts());
        tracker.update(&[(52.0001, 4.0001)], ts()); // nearby → same track
        assert_eq!(tracker.active_count(), 1);
        assert_eq!(tracker.tracks[0].fixes.len(), 2);
    }

    #[test]
    fn far_detection_births_second_track() {
        let mut tracker = Tracker::new();
        tracker.update(&[(52.0, 4.0)], ts());
        tracker.update(&[(53.0, 5.0)], ts()); // 100+ km away → new track
        assert_eq!(tracker.active_count(), 2);
    }

    #[test]
    fn missed_sweeps_causes_loss() {
        let mut tracker = Tracker::new();
        tracker.update(&[(52.0, 4.0)], ts());
        let mut lost = Vec::new();
        for _ in 0..MAX_MISSED_SWEEPS {
            lost.extend(tracker.update(&[], ts()));
        }
        assert_eq!(tracker.active_count(), 0);
        assert_eq!(lost.len(), 1);
    }

    #[test]
    fn drain_returns_all_tracks() {
        let mut tracker = Tracker::new();
        tracker.update(&[(52.0, 4.0), (53.0, 5.0)], ts());
        let drained = tracker.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(tracker.active_count(), 0);
    }
}
