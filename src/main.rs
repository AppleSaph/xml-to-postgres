mod binary;
mod config;
mod geometry;
mod models;
mod output;
mod processing;

use crate::config::{add_table, emit_preamble};
use crate::models::{Cardinality, Settings, State, Step};
use git_version::git_version;
use processing::{check_columns_used, process_event};
use quick_xml::Reader;
use regex::Regex;
use std::env;
use std::fs::File;
use std::io::{stdin, stdout, BufRead, BufReader, IsTerminal as _, Read};
use std::time::Instant;
use yaml_rust2::YamlLoader;

#[macro_export]
macro_rules! fatalerr {
  () => ({
    eprintln!();
    std::process::exit(1);
  });
  ($($arg:tt)*) => ({
      eprintln!($($arg)*);
      std::process::exit(1);
  });
}
fn main() {
    let args: Vec<_> = env::args().collect();
    let bufread: Box<dyn BufRead>;
    if args.len() == 2 {
        bufread = Box::new(BufReader::new(stdin()));
    } else if args.len() == 3 {
        bufread = Box::new(BufReader::new(File::open(&args[2]).unwrap_or_else(|err| {
            fatalerr!("Error: failed to open input file '{}': {}", args[2], err)
        })));
    } else {
        eprintln!(
            "xml-to-postgres {}",
            git_version!(args = ["--always", "--tags", "--dirty=-modified"])
        );
        fatalerr!("Usage: {} <configfile> [xmlfile]", args[0]);
    }

    let config = {
        let mut config_str = String::new();
        let mut file = File::open(&args[1]).unwrap_or_else(|err| {
            fatalerr!(
                "Error: failed to open configuration file '{}': {}",
                args[1],
                err
            )
        });
        file.read_to_string(&mut config_str).unwrap_or_else(|err| {
            fatalerr!(
                "Error: failed to read configuration file '{}': {}",
                args[1],
                err
            )
        });
        &YamlLoader::load_from_str(&config_str)
            .unwrap_or_else(|err| fatalerr!("Error: invalid syntax in configuration file: {}", err))
            [0]
    };

    let name = config["name"]
        .as_str()
        .unwrap_or_else(|| fatalerr!("Error: no valid 'name' entry in configuration file"));
    let rowpath = config["path"]
        .as_str()
        .unwrap_or_else(|| fatalerr!("Error: no valid 'path' entry in configuration file"));
    let colspec = config["cols"]
        .as_vec()
        .unwrap_or_else(|| fatalerr!("Error: no valid 'cols' array in configuration file"));
    let outfile = config["file"].as_str();
    let emit = config["emit"].as_str().unwrap_or("");
    let hush = config["hush"].as_str().unwrap_or("");
    let binary_format = config["format"].as_str().unwrap_or("text") == "binary";
    let mut settings = Settings {
        filemode: config["mode"].as_str().unwrap_or("truncate").to_owned(),
        skip: config["skip"].as_str().unwrap_or("").to_owned(),
        // In binary mode no SQL preamble is written, so all emit flags are off.
        emit_copyfrom: !binary_format
            && (emit.contains("copy_from")
                || emit.contains("create_table")
                || emit.contains("start_trans")
                || emit.contains("truncate")
                || emit.contains("drop_table")),
        emit_createtable: !binary_format && emit.contains("create_table"),
        emit_starttransaction: !binary_format && emit.contains("start_trans"),
        emit_truncate: !binary_format && emit.contains("truncate"),
        emit_droptable: !binary_format && emit.contains("drop_table"),
        hush_version: hush.contains("version"),
        hush_info: hush.contains("info"),
        hush_notice: hush.contains("notice"),
        hush_warning: hush.contains("warn"),
        show_progress: config["prog"]
            .as_bool()
            .unwrap_or_else(|| stdout().is_terminal()),
        binary_format,
    };

    let maintable = add_table(
        name,
        rowpath,
        outfile,
        &settings,
        colspec,
        Cardinality::Default,
    );
    emit_preamble(&maintable, &settings, None);
    if !settings.skip.is_empty() {
        if !settings.skip.starts_with('/') {
            settings.skip.insert(0, '/');
        }
        settings.skip.insert_str(0, &maintable.path); // Maintable path is normalized in add_table()
    }

    let mut reader;
    reader = Reader::from_reader(bufread);
    reader.config_mut().trim_text(true);
    reader.config_mut().expand_empty_elements = true;
    let mut state = State {
        settings,
        reader,
        tables: Vec::new(),
        table: &maintable,
        rowpath: rowpath.to_string(),
        path: String::new(),
        parentcol: None,
        deferred: None,
        filtered: false,
        skipped: false,
        fullcount: 0,
        filtercount: 0,
        skipcount: 0,
        concattext: false,
        xmltotext: false,
        text: String::new(),
        gmltoewkb: false,
        gmltocoord: false,
        gmlpos: false,
        gmlcoll: vec![],
        step: Step::Next,
        trimre: Regex::new("[ \n\r\t]*\n[ \n\r\t]*").unwrap(),
    };

    let mut buf = Vec::new();
    let mut deferred = Vec::new();
    let mut events = 0;
    let mut report = 2;
    let start = Instant::now();
    'main: loop {
        // Main loop over the XML nodes
        let event = state.reader.read_event_into(&mut buf).unwrap_or_else(|e| {
            fatalerr!(
                "Error: failed to parse XML at position {}: {}",
                state.reader.buffer_position(),
                e
            )
        });
        if state.settings.show_progress && !state.settings.hush_info {
            events += 1;
            if events % 10000 == 0 && start.elapsed().as_secs() > report {
                report += 2;
                eprint!(
                    "\rInfo: [{}] {} rows processed{}{}",
                    state.tables.first().unwrap_or(&state.table).name,
                    state.fullcount - state.filtercount - state.skipcount,
                    match state.filtercount {
                        0 => "".to_owned(),
                        n => format!(" ({} excluded)", n),
                    },
                    match state.skipcount {
                        0 => "".to_owned(),
                        n => format!(" ({} skipped)", n),
                    }
                );
            }
        }
        loop {
            // Repeat loop to be able to process a node twice
            state.step = process_event(&event, &mut state);
            match state.step {
                Step::Next => break,
                Step::Repeat => {
                    // if !deferred.is_empty() { deferred.clear(); }
                    continue;
                }
                Step::Defer => {
                    // println!("Defer {:?}", event);
                    deferred.push(event.into_owned());
                    break;
                }
                Step::Apply => {
                    if state.table.lastid.borrow().is_empty() {
                        fatalerr!("Subtable defer failed to yield a key for parent table");
                    }
                    // println!("Applying {} deferred events", deferred.len());
                    state.step = Step::Repeat;
                    state.path = state.deferred.unwrap();
                    state.deferred = None;
                    deferred.reverse();
                    let mut event = deferred
                        .pop()
                        .expect("deferred array should never be empty at this stage");
                    loop {
                        // println!("Event: {:?}", event);
                        state.step = process_event(&event, &mut state);
                        match state.step {
              Step::Repeat => continue,
              Step::Defer => fatalerr!("Error: you have nested subtables that need non-linear processing; this is not currently supported"),
              Step::Done => break 'main,
              _ => ()
            }
                        let result = deferred.pop();
                        if result.is_none() {
                            break;
                        }
                        event = result.unwrap();
                    }
                    state.path.clear();
                    let i = state.table.path.rfind('/').unwrap();
                    state.path.push_str(&state.table.path[0..i]);
                    break;
                }
                Step::Done => break 'main,
            }
        }
        buf.clear();
    }
    if !state.settings.hush_warning {
        check_columns_used(&maintable);
    }
    if !state.settings.hush_info {
        let elapsed = start.elapsed().as_secs_f32();
        eprintln!(
            "{}Info: [{}] {} rows processed in {:.*} seconds{}{}",
            match state.settings.show_progress {
                true => "\r",
                false => "",
            },
            maintable.name,
            state.fullcount - state.filtercount - state.skipcount,
            if elapsed > 9.9 {
                0
            } else if elapsed > 0.99 {
                1
            } else if elapsed > 0.099 {
                2
            } else {
                3
            },
            elapsed,
            match state.filtercount {
                0 => "".to_owned(),
                n => format!(" ({} excluded)", n),
            },
            match state.skipcount {
                0 => "".to_owned(),
                n => format!(" ({} skipped)", n),
            }
        );
    }
}
