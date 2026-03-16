use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use regex::Regex;
use quick_xml::Reader;
use std::io::BufRead;

pub struct Settings {
    pub filemode: String,
    pub skip: String,
    pub emit_copyfrom: bool,
    pub emit_createtable: bool,
    pub emit_starttransaction: bool,
    pub emit_truncate: bool,
    pub emit_droptable: bool,
    pub hush_version: bool,
    pub hush_info: bool,
    pub hush_notice: bool,
    pub hush_warning: bool,
    pub show_progress: bool,
    pub binary_format: bool,
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Cardinality {
    Default,
    OneToMany,
    ManyToOne,
    ManyToMany,
    None,
}

pub struct Table<'a> {
    pub name: String,
    pub path: String,
    pub buf: RefCell<Vec<u8>>,
    pub writer_channel: mpsc::SyncSender<Vec<u8>>,
    pub writer_thread: Option<thread::JoinHandle<()>>,
    pub columns: Vec<Column<'a>>,
    pub lastid: RefCell<String>,
    pub domain: Box<Option<RefCell<Domain<'a>>>>,
    pub cardinality: Cardinality,
    pub emit_copyfrom: bool,
    pub emit_starttransaction: bool,
    pub binary_format: bool,
}

impl<'a> Table<'a> {
    pub fn flush(&self) {
        let mut buf = self.buf.borrow_mut();
        if !buf.is_empty() {
            self.writer_channel
                .send(std::mem::take(&mut *buf))
                .unwrap();
        }
    }

    pub fn clear_columns(&self) {
        for col in &self.columns {
            col.value.borrow_mut().clear();
        }
    }
}

impl<'a> Drop for Table<'a> {
    fn drop(&mut self) {
        if self.binary_format {
            // Only write a binary COPY trailer if we appear to have written at least one row.
            // This helps avoid emitting a trailer for tables that never had a header emitted.
            if !self.lastid.borrow().is_empty() {
                crate::binary::write_file_trailer(&mut self.buf.borrow_mut());
            }
        } else {
            if self.emit_copyfrom {
                self.buf.borrow_mut().extend_from_slice(b"\\.\n");
            }
            if self.emit_starttransaction {
                self.buf.borrow_mut().extend_from_slice(b"COMMIT;\n");
            }
        }
        self.flush();
        self.writer_channel.send(Vec::new()).unwrap(); // Terminates the writer thread
        let thread = std::mem::take(&mut self.writer_thread);
        thread
            .unwrap()
            .join()
            .unwrap_or_else(|_| eprintln!("Table writer thread for [{}] crashed", self.name));
    }
}

pub struct Domain<'a> {
    pub lastid: u32,
    pub map: HashMap<String, u32>,
    pub table: Table<'a>,
}

#[derive(Default)]
pub struct Column<'a> {
    pub name: String,
    pub path: String,
    pub serial: Option<Cell<u64>>,
    pub fkey: Option<(String, String)>,
    pub datatype: String,
    pub value: RefCell<String>,
    pub attr: Option<&'a str>,
    pub hide: bool,
    pub include: Option<Regex>,
    pub exclude: Option<Regex>,
    pub find: Option<Regex>,
    pub replace: Option<&'a str>,
    pub trim: bool,
    pub convert: Option<&'a str>,
    pub aggr: Option<&'a str>,
    pub subtable: Option<Table<'a>>,
    pub domain: Option<RefCell<Domain<'a>>>,
    pub bbox: Option<BBox>,
    pub multitype: bool,
    pub used: RefCell<bool>,
}

#[derive(Debug)]
pub struct Geometry {
    pub gtype: u8,
    pub dims: u8,
    pub srid: u32,
    pub rings: Vec<Vec<f64>>,
}

impl Geometry {
    pub fn new(gtype: u8) -> Geometry {
        Geometry {
            gtype,
            dims: 2,
            srid: 4326,
            rings: Vec::new(),
        }
    }
}

pub struct BBox {
    pub minx: f64,
    pub miny: f64,
    pub maxx: f64,
    pub maxy: f64,
}

impl BBox {
    pub fn from(str: &str) -> Option<BBox> {
        lazy_static::lazy_static! {
            static ref RE: Regex = Regex::new(r"^([0-9.]+),([0-9.]+) ([0-9.]+),([0-9.]+)$").unwrap();
        }
        RE.captures(str).map(|caps| BBox {
            minx: caps[1].parse().unwrap(),
            miny: caps[2].parse().unwrap(),
            maxx: caps[3].parse().unwrap(),
            maxy: caps[4].parse().unwrap(),
        })
    }
}

#[derive(PartialEq, Debug)]
pub enum Step {
    Next,
    Repeat,
    Defer,
    Apply,
    Done,
}

pub struct State<'a, 'b> {
    pub settings: Settings,
    pub reader: Reader<Box<dyn BufRead>>,
    pub tables: Vec<&'b Table<'a>>,
    pub table: &'b Table<'a>,
    pub rowpath: String,
    pub path: String,
    pub parentcol: Option<&'b Column<'a>>,
    pub deferred: Option<String>,
    pub filtered: bool,
    pub skipped: bool,
    pub fullcount: u64,
    pub filtercount: u64,
    pub skipcount: u64,
    pub concattext: bool,
    pub xmltotext: bool,
    pub text: String,
    pub gmltoewkb: bool,
    pub gmltocoord: bool,
    pub gmlpos: bool,
    pub gmlcoll: Vec<Geometry>,
    pub trimre: Regex,
    pub step: Step,
}


