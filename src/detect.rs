/// Blob detection on a single radar spoke.
///
/// Scans the raw pixel array for consecutive runs of intensity above a
/// threshold.  Each qualifying run produces one Detection at its centroid.

// Pixels must exceed this intensity to be counted.
// Navico/HALO spokes use 4-bit pixel values (0–15); 0 = no return, 15 = max.
// A threshold of 12 passes strong returns (13+/15) including the emulator's
// smooth target model (intensity 13–15 within target radius).
// Land clutter is handled by the sliding-window land filter (--land-filter).
const THRESHOLD: u8 = 12;
// Blob must span at least this many pixels.
const MIN_BLOB_PX: usize = 3;
// Blob wider than this is clutter/interference — skip it.
const MAX_BLOB_PX: usize = 200;
// Ignore this many pixels nearest the antenna (main bang / sidelobe).
const MIN_RANGE_PX: usize = 4;

/// A single detection: the centroid of one blob on one spoke.
#[derive(Debug, Clone, PartialEq)]
pub struct Detection {
    /// Slant range from the radar to the centroid, in metres.
    pub range_m: f32,
    /// True-north bearing of this spoke, in radians [0, 2π).
    pub bearing_rad: f32,
}

/// Detect blobs on one spoke.
///
/// * `data`            – raw pixel bytes from the protobuf Spoke
/// * `total_range_m`   – range of the last pixel, in metres
/// * `bearing_spoke`   – bearing field from the protobuf (spoke units, True North)
/// * `spokes_per_rev`  – total spokes in one full revolution
pub fn detect(
    data: &[u8],
    total_range_m: u32,
    bearing_spoke: u32,
    spokes_per_rev: u32,
) -> Vec<Detection> {
    if data.is_empty() || total_range_m == 0 || spokes_per_rev == 0 {
        return vec![];
    }

    let m_per_px = total_range_m as f32 / data.len() as f32;
    let bearing_rad =
        bearing_spoke as f32 * (2.0 * std::f32::consts::PI) / spokes_per_rev as f32;

    let mut detections = Vec::new();
    let mut i = MIN_RANGE_PX;

    while i < data.len() {
        if data[i] <= THRESHOLD {
            i += 1;
            continue;
        }
        // Leading edge of a blob — scan to the trailing edge.
        let start = i;
        while i < data.len() && data[i] > THRESHOLD {
            i += 1;
        }
        let end = i; // exclusive
        let len = end - start;

        if len >= MIN_BLOB_PX && len <= MAX_BLOB_PX {
            let centroid_px = (start + end) as f32 / 2.0;
            detections.push(Detection {
                range_m: centroid_px * m_per_px,
                bearing_rad,
            });
        }
    }

    detections
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32::consts::PI;

    fn approx_eq(a: f32, b: f32, eps: f32) -> bool {
        (a - b).abs() < eps
    }

    #[test]
    fn empty_spoke_returns_no_detections() {
        assert!(detect(&[], 1000, 0, 2048).is_empty());
    }

    #[test]
    fn all_noise_returns_no_detections() {
        let data = vec![3u8; 512]; // below threshold (0-15 range, threshold=7)
        assert!(detect(&data, 1000, 0, 2048).is_empty());
    }

    #[test]
    fn single_blob_detected() {
        let mut data = vec![0u8; 512];
        // Plant a blob at pixels 100–109 (10 px wide, above threshold and MIN_BLOB_PX).
        for p in &mut data[100..110] {
            *p = 15;
        }
        let dets = detect(&data, 25600, 0, 2048); // 50 m/px
        assert_eq!(dets.len(), 1);
        // start=100, end=110 → centroid = 105.0 px × 50 m/px = 5250 m
        assert!(approx_eq(dets[0].range_m, 5250.0, 1.0));
        // Bearing 0 spoke = 0 rad (True North)
        assert!(approx_eq(dets[0].bearing_rad, 0.0, 0.001));
    }

    #[test]
    fn blob_below_min_size_ignored() {
        let mut data = vec![0u8; 512];
        // 2 pixels — below MIN_BLOB_PX (3)
        data[100] = 15;
        data[101] = 15;
        assert!(detect(&data, 25600, 0, 2048).is_empty());
    }

    #[test]
    fn blob_in_main_bang_zone_ignored() {
        let mut data = vec![0u8; 512];
        // Blob within the first MIN_RANGE_PX pixels.
        for p in &mut data[0..3] {
            *p = 15;
        }
        assert!(detect(&data, 25600, 0, 2048).is_empty());
    }

    #[test]
    fn bearing_converted_correctly() {
        let mut data = vec![0u8; 512];
        for p in &mut data[100..110] {
            *p = 15;
        }
        // spoke 512 out of 2048 = 90° = π/2
        let dets = detect(&data, 25600, 512, 2048);
        assert_eq!(dets.len(), 1);
        assert!(approx_eq(dets[0].bearing_rad, PI / 2.0, 0.001));
    }

    #[test]
    fn two_blobs_detected() {
        let mut data = vec![0u8; 512];
        for p in &mut data[50..55] {
            *p = 15;
        }
        for p in &mut data[300..310] {
            *p = 15;
        }
        assert_eq!(detect(&data, 25600, 0, 2048).len(), 2);
    }
}
