use crate::models::{BBox, Geometry, Settings};
use std::cell::RefCell;
use std::fmt::Write as _;

pub fn gml_to_ewkb(
    cell: &RefCell<String>,
    coll: &[Geometry],
    bbox: Option<&BBox>,
    multitype: bool,
    settings: &Settings,
) -> bool {
    let mut ewkb: Vec<u8> = vec![];

    if multitype || coll.len() > 1 {
        let multitype = coll.first().unwrap().gtype + 3;
        ewkb.extend_from_slice(&[1, multitype, 0, 0, 0]);
        ewkb.extend_from_slice(&(coll.len() as u32).to_le_bytes());
    }

    for geom in coll {
        // println!("{:?}", geom);
        let code = match geom.dims {
            2 => 32,       // Indicate EWKB where the srid follows this byte
            3 => 32 | 128, // Add bit to indicate the presence of Z values
            _ => {
                if !settings.hush_warning {
                    eprintln!(
                        "Warning: GML number of dimensions {} not supported",
                        geom.dims
                    );
                }
                32
            }
        };
        ewkb.extend_from_slice(&[1, geom.gtype, 0, 0, code]);
        ewkb.extend_from_slice(&geom.srid.to_le_bytes());
        if geom.gtype == 3 {
            ewkb.extend_from_slice(&(geom.rings.len() as u32).to_le_bytes());
        } // Only polygons can have multiple rings
        if let Some(bbox) = bbox {
            let mut overlap = false;
            let mut overlapx = false;
            for ring in geom.rings.iter() {
                if geom.gtype != 1 {
                    ewkb.extend_from_slice(&((ring.len() as u32) / geom.dims as u32).to_le_bytes());
                } // Points don't have multiple vertices
                for (i, pos) in ring.iter().enumerate() {
                    if overlap {
                    } else if geom.dims == 2 {
                        if i % 2 == 0 {
                            overlapx = false;
                            if *pos >= bbox.minx && *pos <= bbox.maxx {
                                overlapx = true;
                            }
                        } else if overlapx && *pos < bbox.miny && *pos > bbox.maxy {
                            overlap = true;
                        }
                    } else {
                        // geom.dims == 3
                        if i % 3 == 0 {
                            overlapx = false;
                            if *pos >= bbox.minx && *pos <= bbox.maxx {
                                overlapx = true;
                            }
                        } else if overlapx && i % 3 == 1 && (*pos >= bbox.miny && *pos <= bbox.maxy)
                        {
                            overlap = true;
                        }
                    }
                    ewkb.extend_from_slice(&pos.to_le_bytes());
                }
            }
            if !overlap {
                return false;
            }
        } else {
            for ring in geom.rings.iter() {
                if geom.gtype != 1 {
                    ewkb.extend_from_slice(&((ring.len() as u32) / geom.dims as u32).to_le_bytes());
                } // Points don't have multiple vertices
                for pos in ring.iter() {
                    ewkb.extend_from_slice(&pos.to_le_bytes());
                }
            }
        }
    }

    static CHARS: &[u8] = b"0123456789ABCDEF";
    let mut value = cell.borrow_mut();
    value.reserve(ewkb.len() * 2);
    for byte in ewkb.iter() {
        value.push(CHARS[(byte >> 4) as usize].into());
        value.push(CHARS[(byte & 0xf) as usize].into());
    }
    true
}

pub fn rd_to_wgs84(x: f64, y: f64) -> (f64, f64) {
    // Polynomial approximation for RD New (EPSG:28992) -> WGS84 (EPSG:4326).
    let dx = (x - 155000.0) / 100000.0;
    let dy = (y - 463000.0) / 100000.0;

    let lat = 52.15517440
        + (3235.65389 * dy
            + -32.58297 * dx * dx
            + -0.2475 * dy * dy
            + -0.84978 * dx * dx * dy
            + -0.0655 * dy * dy * dy
            + -0.01709 * dx * dx * dx
            + -0.00738 * dx * dy * dy
            + 0.0053 * dx * dx * dx * dy
            + -0.00039 * dx * dx * dx * dx
            + 0.00033 * dx * dy * dy * dy
            + -0.00012 * dx * dx * dy * dy)
            / 3600.0;

    let lon = 5.38720621
        + (5260.52916 * dx
            + 105.94684 * dx * dy
            + 2.45656 * dx * dy * dy
            + -0.81885 * dx * dx * dx
            + 0.05594 * dx * dy * dy * dy
            + -0.05607 * dx * dx * dx * dy
            + 0.01199 * dy
            + -0.00256 * dx * dx * dx * dx
            + 0.00128 * dx * dy * dy * dy * dy
            + 0.00022 * dy * dy
            + -0.00022 * dx * dx
            + 0.00026 * dx * dx * dx * dx * dx)
            / 3600.0;

    (lat, lon)
}

pub fn gml_to_coord(cell: &RefCell<String>, coll: &[Geometry], settings: &Settings) -> bool {
    let mut sumx = 0.0;
    let mut sumy = 0.0;
    let mut count: u64 = 0;

    for geom in coll {
        let step = match geom.dims {
            2 => 2,
            3 => 3,
            _ => {
                if !settings.hush_warning {
                    eprintln!(
                        "Warning: GML number of dimensions {} not supported",
                        geom.dims
                    );
                }
                continue;
            }
        };

        for ring in geom.rings.iter() {
            let mut i = 0usize;
            while i + 1 < ring.len() {
                sumx += ring[i];
                sumy += ring[i + 1];
                count += 1;
                i += step;
            }
        }
    }

    if count == 0 {
        if !settings.hush_warning {
            eprintln!("Warning: no valid GML coordinates found for gml-to-coord conversion");
        }
        return false;
    }

    let (lat, lon) = rd_to_wgs84(sumx / count as f64, sumy / count as f64);
    let mut value = cell.borrow_mut();
    value.clear();
    write!(value, "{:.8},{:.8}", lon, lat).unwrap();
    true
}
