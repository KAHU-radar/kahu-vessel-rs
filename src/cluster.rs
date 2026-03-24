//! Sweep-level detection clustering.
//!
//! After collecting all detections from one full radar revolution, nearby
//! points are merged into a single centroid before being handed to the
//! tracker.  This prevents a large vessel — which produces several distinct
//! blobs (bow, superstructure, stern) across multiple spokes — from being
//! tracked as several parallel ghost tracks.
//!
//! Algorithm: greedy single-pass.  Each detection is tested against the
//! current centroid of every existing cluster.  If it falls within `EPS_M`
//! metres it is absorbed; otherwise it seeds a new cluster.  O(n·k) where k
//! is the number of clusters (typically small for a radar sweep).

use crate::geo::haversine_m;

/// Detections within this distance of a cluster centroid are merged into it.
/// 150 m covers the length of a typical large vessel while leaving a safe
/// margin between closely spaced but distinct targets.
const EPS_M: f64 = 150.0;

/// Cluster a sweep's detections and return one centroid per cluster.
pub fn cluster(dets: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let mut clusters: Vec<Vec<(f64, f64)>> = Vec::new();

    'det: for &(lat, lon) in dets {
        for cluster in clusters.iter_mut() {
            let (clat, clon) = centroid(cluster);
            if haversine_m(clat, clon, lat, lon) < EPS_M {
                cluster.push((lat, lon));
                continue 'det;
            }
        }
        clusters.push(vec![(lat, lon)]);
    }

    clusters.iter().map(|c| centroid(c)).collect()
}

fn centroid(pts: &[(f64, f64)]) -> (f64, f64) {
    let n = pts.len() as f64;
    (
        pts.iter().map(|p| p.0).sum::<f64>() / n,
        pts.iter().map(|p| p.1).sum::<f64>() / n,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_detection_passes_through() {
        let result = cluster(&[(52.0, 4.0)]);
        assert_eq!(result.len(), 1);
        assert!((result[0].0 - 52.0).abs() < 1e-9);
    }

    #[test]
    fn nearby_detections_merge() {
        // Two points ~100 m apart (well within EPS_M) → one centroid
        let a = (52.0, 4.0);
        let b = (52.0009, 4.0); // ~100 m north
        let result = cluster(&[a, b]);
        assert_eq!(result.len(), 1);
        // Centroid latitude should be between the two
        assert!(result[0].0 > 52.0 && result[0].0 < 52.0009);
    }

    #[test]
    fn distant_detections_stay_separate() {
        // Two points ~1 km apart → two clusters
        let a = (52.0, 4.0);
        let b = (52.009, 4.0); // ~1 km north
        let result = cluster(&[a, b]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn three_blobs_from_one_ship_merge() {
        // Bow, mid, stern of a ~130 m vessel — each ~65 m from the next.
        // The greedy algorithm compares each new point against the running
        // centroid, so the test geometry must stay within EPS_M of the
        // centroid as it shifts after each absorption.
        let bow = (52.0, 4.0);
        let mid = (52.0006, 4.0); // ~67 m — joins bow, centroid shifts to ~33 m
        let stern = (52.0012, 4.0); // ~67 m from mid, ~100 m from centroid → within EPS_M
        let result = cluster(&[bow, mid, stern]);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn empty_input_returns_empty() {
        assert!(cluster(&[]).is_empty());
    }
}
