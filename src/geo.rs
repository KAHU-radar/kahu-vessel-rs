/// Coordinate geometry utilities.
///
/// All conversions use an equirectangular (flat-earth) approximation,
/// which is accurate to better than 0.1 % within 24 nm — sufficient for
/// radar target tracking.

/// Convert polar radar coordinates to absolute lat/lon.
///
/// * `own_lat_deg` / `own_lon_deg` – radar position (decimal degrees)
/// * `range_m`                     – slant range to target (metres)
/// * `bearing_rad`                 – True-North bearing to target (radians)
pub fn polar_to_latlon(
    own_lat_deg: f64,
    own_lon_deg: f64,
    range_m: f64,
    bearing_rad: f64,
) -> (f64, f64) {
    const EARTH_R: f64 = 6_371_000.0;
    let dlat_rad = range_m * bearing_rad.cos() / EARTH_R;
    let dlon_rad =
        range_m * bearing_rad.sin() / (EARTH_R * own_lat_deg.to_radians().cos());
    (
        own_lat_deg + dlat_rad.to_degrees(),
        own_lon_deg + dlon_rad.to_degrees(),
    )
}

/// Haversine distance between two lat/lon points, in metres.
pub fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_R: f64 = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_R * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    #[test]
    fn north_displacement() {
        // 1000 m due north should increase lat only
        let (lat, lon) = polar_to_latlon(52.0, 4.0, 1000.0, 0.0);
        assert!(lat > 52.0);
        assert!((lon - 4.0).abs() < 1e-6);
    }

    #[test]
    fn east_displacement() {
        // 1000 m due east (bearing = π/2) should increase lon only
        let (lat, lon) = polar_to_latlon(52.0, 4.0, 1000.0, PI / 2.0);
        assert!(lon > 4.0);
        assert!((lat - 52.0).abs() < 1e-6);
    }

    #[test]
    fn haversine_known_distance() {
        // Amsterdam to Rotterdam ≈ 57 km
        let d = haversine_m(52.3676, 4.9041, 51.9225, 4.4792);
        assert!((d - 57_000.0).abs() < 2_000.0);
    }

    #[test]
    fn haversine_zero_for_same_point() {
        assert!(haversine_m(52.0, 4.0, 52.0, 4.0) < 1e-3);
    }
}
