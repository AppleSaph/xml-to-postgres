use crate::fatalerr;
use crate::models::{BBox, Cardinality, Column, Domain, Settings, Table};
use crate::output::write_output;
use regex::Regex;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{stdout, Write};
use std::mem;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use yaml_rust2::yaml::Yaml;

pub fn add_table<'a>(
    name: &str,
    rowpath: &str,
    outfile: Option<&str>,
    settings: &Settings,
    colspec: &'a [Yaml],
    cardinality: Cardinality,
) -> Table<'a> {
    let mut table = create_table(name, rowpath, outfile, settings, cardinality);

    for col in colspec {
        let column = parse_column_config(col, &table, name, settings);
        table.columns.push(column);
    }

    table
}

fn create_table<'a>(
    name: &str,
    path: &str,
    file: Option<&str>,
    settings: &Settings,
    cardinality: Cardinality,
) -> Table<'a> {
    let out: RefCell<Box<dyn Write + Send>> = match file {
        None => RefCell::new(Box::new(stdout())),
        Some(ref file) => RefCell::new(Box::new(match settings.filemode.as_ref() {
            "truncate" => File::create(Path::new(file)).unwrap_or_else(|err| {
                fatalerr!("Error: failed to create output file '{}': {}", file, err)
            }),
            "append" => OpenOptions::new()
                .append(true)
                .create(true)
                .open(Path::new(file))
                .unwrap_or_else(|err| {
                    fatalerr!("Error: failed to open output file '{}': {}", file, err)
                }),
            mode => fatalerr!(
                "Error: invalid 'mode' setting in configuration file: {}",
                mode
            ),
        })),
    };

    let (writer_channel, rx) = mpsc::sync_channel(100);
    let writer_thread = thread::Builder::new()
        .name(format!("write {}", name))
        .spawn(move || write_output(out, rx))
        .unwrap_or_else(|err| fatalerr!("Error: failed to create writer thread: {}", err));

    let mut ownpath = String::from(path);
    if !ownpath.is_empty() && !ownpath.starts_with('/') {
        ownpath.insert(0, '/');
    }
    if ownpath.ends_with('/') {
        ownpath.pop();
    }

    Table {
        name: name.to_owned(),
        path: ownpath,
        buf: RefCell::new(Vec::new()),
        writer_channel,
        writer_thread: Some(writer_thread),
        columns: Vec::new(),
        lastid: RefCell::new(String::new()),
        domain: Box::new(None),
        cardinality,
        emit_copyfrom: if cardinality != Cardinality::None {
            settings.emit_copyfrom
        } else {
            false
        },
        emit_starttransaction: if cardinality != Cardinality::None {
            settings.emit_starttransaction
        } else {
            false
        },
        binary_format: settings.binary_format,
    }
}

fn parse_column_config<'a>(
    col: &'a Yaml,
    table: &Table,
    table_name: &str,
    settings: &Settings,
) -> Column<'a> {
    let colname = col["name"]
        .as_str()
        .unwrap_or_else(|| fatalerr!("Error: column has no 'name' entry in configuration file"));

    let fkey = col["fkey"]
        .as_str()
        .map(String::from)
        .map(|v| match v.split_once('.') {
            Some((left, right)) => (left.to_string(), right.to_string()),
            None => fatalerr!("Error: column {} option 'fkey' is invalid", colname),
        });

    let colpath = if let Some(true) = col["seri"].as_bool() {
        "/"
    } else if fkey.is_some() {
        "/"
    } else {
        col["path"].as_str().unwrap_or_else(|| {
            fatalerr!(
                "Error: table '{}' column '{}' has no 'path' entry in configuration file",
                table_name,
                colname
            )
        })
    };

    let mut path = String::from(&table.path);
    if !colpath.is_empty() && !colpath.starts_with('/') {
        path.push('/');
    }
    path.push_str(colpath);
    if path.ends_with('/') {
        path.pop();
    }

    let serial = match col["seri"].as_bool() {
        Some(true) => {
            if !table.columns.is_empty() && !settings.hush_warning {
                eprintln!(
                    "Warning: a 'seri' column usually needs to be the first column; {} in table {} is not",
                    colname, table_name
                );
            }
            Some(Cell::new(0))
        }
        _ => None,
    };

    let mut datatype = col["type"].as_str().unwrap_or("text").to_string();
    let mut include: Option<Regex> = col["incl"].as_str().map(|str| {
        Regex::new(str).unwrap_or_else(|err| {
            fatalerr!(
                "Error: invalid regex in 'incl' entry in configuration file: {}",
                err
            )
        })
    });
    let mut exclude: Option<Regex> = col["excl"].as_str().map(|str| {
        Regex::new(str).unwrap_or_else(|err| {
            fatalerr!(
                "Error: invalid regex in 'excl' entry in configuration file: {}",
                err
            )
        })
    });

    let norm = col["norm"].as_str();
    let file = col["file"].as_str();
    let cardinality = match (file, norm) {
        (None, None) => Cardinality::Default,
        (Some(_), None) => Cardinality::OneToMany,
        (None, Some(_)) => Cardinality::ManyToOne,
        (Some(_), Some(_)) => Cardinality::ManyToMany,
    };

    let mut subtable: Option<Table> = create_subtable_if_needed(
        col,
        colname,
        &path,
        table_name,
        table,
        cardinality,
        settings,
        &mut include,
        &mut exclude,
        &mut datatype,
    );

    let hide = col["hide"].as_bool().unwrap_or(false);
    let trim = col["trim"].as_bool().unwrap_or(false);
    let attr = col["attr"].as_str();
    let convert = col["conv"].as_str();
    let find = col["find"].as_str().map(|str| {
        Regex::new(str).unwrap_or_else(|err| {
            fatalerr!(
                "Error: invalid regex in 'find' entry in configuration file: {}",
                err
            )
        })
    });
    let replace = col["repl"].as_str();
    let aggr = col["aggr"].as_str();

    let domain =
        create_domain_if_needed(norm, col, colname, &mut subtable, &mut datatype, settings);

    let bbox = col["bbox"].as_str().and_then(BBox::from);
    let multitype = col["mult"].as_bool().unwrap_or(false);

    validate_column_options(
        col, convert, aggr, table_name, colname, &bbox, &include, &exclude, &find, settings,
    );

    Column {
        name: colname.to_string(),
        path,
        serial,
        fkey,
        datatype,
        attr,
        hide,
        include,
        exclude,
        trim,
        convert,
        find,
        replace,
        aggr,
        subtable,
        domain,
        bbox,
        multitype,
        ..Default::default()
    }
}

fn create_subtable_if_needed<'a>(
    col: &'a Yaml,
    colname: &str,
    path: &str,
    table_name: &str,
    table: &Table,
    cardinality: Cardinality,
    settings: &Settings,
    include: &mut Option<Regex>,
    exclude: &mut Option<Regex>,
    datatype: &mut String,
) -> Option<Table<'a>> {
    match col["cols"].is_badvalue() {
        true => match cardinality {
            Cardinality::OneToMany => {
                let filename = col["file"].as_str().unwrap();
                if table.columns.is_empty() {
                    fatalerr!(
                        "Error: table '{}' cannot have a subtable as first column",
                        table_name
                    );
                }
                let mut subtable =
                    add_table(colname, path, Some(filename), settings, &[], cardinality);
                subtable.columns.push(Column {
                    name: colname.to_string(),
                    path: path.to_string(),
                    datatype: datatype.to_string(),
                    include: mem::take(include),
                    exclude: mem::take(exclude),
                    ..Default::default()
                });
                emit_preamble(
                    &subtable,
                    settings,
                    Some(format!("{} {}", table_name, table.columns[0].datatype)),
                );
                Some(subtable)
            }
            Cardinality::ManyToMany => {
                let filename = col["file"].as_str().unwrap();
                if table.columns.is_empty() {
                    fatalerr!(
                        "Error: table '{}' cannot have a subtable as first column",
                        table_name
                    );
                }
                let mut subtable =
                    add_table(colname, path, Some(filename), settings, &[], cardinality);
                subtable.columns.push(Column {
                    name: colname.to_string(),
                    path: path.to_string(),
                    datatype: "integer".to_string(),
                    include: mem::take(include),
                    exclude: mem::take(exclude),
                    ..Default::default()
                });
                emit_preamble(
                    &subtable,
                    settings,
                    Some(format!("{} {}", table_name, table.columns[0].datatype)),
                );
                Some(subtable)
            }
            _ => None,
        },
        false => match cardinality {
            Cardinality::ManyToOne => {
                let subtable = add_table(
                    colname,
                    path,
                    col["norm"].as_str(),
                    settings,
                    col["cols"].as_vec().unwrap_or_else(|| {
                        fatalerr!("Error: subtable 'cols' entry is not an array")
                    }),
                    cardinality,
                );
                emit_preamble(&subtable, settings, None);
                Some(subtable)
            }
            Cardinality::ManyToMany => {
                let filename = col["file"].as_str().unwrap_or_else(|| {
                    fatalerr!("Error: subtable {} has no 'file' entry", colname)
                });
                if table.columns.is_empty() {
                    fatalerr!(
                        "Error: table '{}' cannot have a subtable as first column",
                        table_name
                    );
                }
                let subtable = add_table(
                    colname,
                    path,
                    Some(filename),
                    settings,
                    col["cols"].as_vec().unwrap_or_else(|| {
                        fatalerr!("Error: subtable 'cols' entry is not an array")
                    }),
                    cardinality,
                );
                emit_preamble(
                    &subtable,
                    settings,
                    Some(format!("{} {}", table_name, table.columns[0].datatype)),
                );
                Some(subtable)
            }
            _ => {
                let filename = col["file"].as_str().unwrap_or_else(|| {
                    fatalerr!("Error: subtable {} has no 'file' entry", colname)
                });
                if table.columns.is_empty() {
                    fatalerr!(
                        "Error: table '{}' cannot have a subtable as first column",
                        table_name
                    );
                }
                let subtable = add_table(
                    colname,
                    path,
                    Some(filename),
                    settings,
                    col["cols"].as_vec().unwrap_or_else(|| {
                        fatalerr!("Error: subtable 'cols' entry is not an array")
                    }),
                    cardinality,
                );
                emit_preamble(
                    &subtable,
                    settings,
                    Some(format!("{} {}", table_name, table.columns[0].datatype)),
                );
                Some(subtable)
            }
        },
    }
}

fn create_domain_if_needed<'a>(
    norm: Option<&str>,
    col: &'a Yaml,
    colname: &str,
    subtable: &mut Option<Table<'a>>,
    datatype: &mut String,
    settings: &Settings,
) -> Option<RefCell<Domain<'a>>> {
    match norm {
        Some(filename) => {
            if filename == "true" {
                fatalerr!("Error: 'norm' option now takes a file path instead of a boolean");
            }
            let file = match subtable {
                Some(_) if col["file"].is_badvalue() => None,
                Some(_) => Some(filename),
                None => Some(filename),
            };
            let mut domain = create_domain(colname, file, settings);
            if file.is_some() {
                populate_domain_columns(&mut domain, col, colname, datatype);
                emit_preamble(&domain.table, settings, None);
            }
            *datatype = String::from("integer");
            if let Some(ref mut table) = subtable {
                table.domain = Box::new(Some(RefCell::new(domain)));
                None
            } else {
                Some(RefCell::new(domain))
            }
        }
        None => None,
    }
}

fn create_domain<'a>(tabname: &str, filename: Option<&str>, settings: &Settings) -> Domain<'a> {
    Domain {
        lastid: 0,
        map: HashMap::new(),
        table: create_table(
            tabname,
            "_domain_",
            filename,
            settings,
            match filename {
                Some(_) => Cardinality::ManyToOne,
                None => Cardinality::None,
            },
        ),
    }
}

fn populate_domain_columns<'a>(
    domain: &mut Domain<'a>,
    col: &'a Yaml,
    colname: &str,
    datatype: &str,
) {
    if !col["cols"].is_badvalue() {
        for col in col["cols"].as_vec().unwrap() {
            let colname = col["name"].as_str().unwrap_or_else(|| {
                fatalerr!("Error: column has no 'name' entry in configuration file")
            });
            let datatype = col["type"].as_str().unwrap_or("text");
            domain.table.columns.push(Column {
                name: colname.to_string(),
                path: String::new(),
                datatype: datatype.to_string(),
                ..Default::default()
            });
        }
    } else {
        domain.table.columns.push(Column {
            name: String::from("id"),
            path: String::new(),
            datatype: String::from("integer"),
            ..Default::default()
        });
        domain.table.columns.push(Column {
            name: colname.to_string(),
            path: String::new(),
            datatype: datatype.to_string(),
            ..Default::default()
        });
    }
}

fn validate_column_options(
    col: &Yaml,
    convert: Option<&str>,
    aggr: Option<&str>,
    table_name: &str,
    _colname: &str,
    bbox: &Option<BBox>,
    include: &Option<Regex>,
    exclude: &Option<Regex>,
    find: &Option<Regex>,
    settings: &Settings,
) {
    if let Some(val) = convert {
        if !vec!["xml-to-text", "gml-to-ewkb", "gml-to-coord", "concat-text"].contains(&val) {
            fatalerr!(
                "Error: table '{}' option 'conv' contains invalid value: {}",
                table_name,
                val
            );
        }
        if val == "gml-to-ewkb" {
            if settings.binary_format {
                fatalerr!("Error: gml-to-ewkb conversion is not supported in binary output mode");
            }
            if !settings.hush_notice {
                eprintln!("Notice: gml-to-ewkb conversion is experimental and in no way complete or standards compliant; use at your own risk");
            }
        }
        if val == "gml-to-coord" && !settings.hush_notice {
            eprintln!("Notice: gml-to-coord conversion is experimental and in no way complete or standards compliant; use at your own risk");
        }
        if col["type"].is_badvalue() && val == "gml-to-ewkb" {
            // Note: datatype was already set in parse_column_config
        }
    }
    if let Some(val) = aggr {
        if !vec!["first", "last", "append"].contains(&val) {
            fatalerr!(
                "Error: table '{}' option 'aggr' contains invalid value: {}",
                table_name,
                val
            );
        }
    }
    if include.is_some() || exclude.is_some() {
        if convert.is_some() {
            fatalerr!("Error: filtering (incl/excl) and 'conv' cannot be used together on a single column");
        }
        if find.is_some() && !settings.hush_notice {
            eprintln!("Notice: when using filtering (incl/excl) and find/replace on a single column, the filter is checked after replacements");
        }
        if aggr.is_some() && !settings.hush_notice {
            eprintln!("Notice: when using filtering (incl/excl) and aggregation on a single column, the filter is checked after aggregation");
        }
    }
    if bbox.is_some()
        && (convert.is_none() || convert.unwrap() != "gml-to-ewkb")
        && !settings.hush_warning
    {
        eprintln!("Warning: the bbox option has no function without conversion type 'gml-to-ewkb'");
    }
}

pub fn emit_preamble(table: &Table, settings: &Settings, fkey: Option<String>) {
    if settings.binary_format {
        // Binary mode: no SQL preamble — write only the 19-byte PGCOPY file header.
        crate::binary::write_file_header(&mut table.buf.borrow_mut());
        table.flush();
        return;
    }
    if settings.emit_starttransaction {
        write!(table.buf.borrow_mut(), "START TRANSACTION;\n").unwrap();
    }
    if settings.emit_droptable {
        write!(
            table.buf.borrow_mut(),
            "DROP TABLE IF EXISTS {};\n",
            table.name
        )
        .unwrap();
    }
    if settings.emit_createtable {
        if table.cardinality == Cardinality::ManyToMany {
            let fkey = fkey.as_ref().unwrap();
            write!(
                table.buf.borrow_mut(),
                "CREATE TABLE IF NOT EXISTS {}_{} ({}, {} {});\n",
                fkey.split_once(' ').unwrap().0,
                table.name,
                fkey,
                table.name,
                if table.columns.is_empty() {
                    "integer"
                } else {
                    &table.columns[0].datatype
                }
            )
            .unwrap();
        } else {
            let mut cols = table
                .columns
                .iter()
                .filter_map(|c| {
                    if c.hide
                        || (c.subtable.is_some()
                            && c.subtable.as_ref().unwrap().cardinality != Cardinality::ManyToOne)
                    {
                        return None;
                    }
                    let mut spec = String::from(&c.name);
                    spec.push(' ');
                    spec.push_str(&c.datatype);
                    Some(spec)
                })
                .collect::<Vec<String>>()
                .join(", ");
            if fkey.is_some() {
                cols.insert_str(0, &format!("{}, ", fkey.as_ref().unwrap()));
            }
            write!(
                table.buf.borrow_mut(),
                "CREATE TABLE IF NOT EXISTS {} ({});\n",
                table.name,
                cols
            )
            .unwrap();
        }
    }
    if settings.emit_truncate {
        write!(table.buf.borrow_mut(), "TRUNCATE {};\n", table.name).unwrap();
    }
    if settings.emit_copyfrom {
        if table.cardinality == Cardinality::ManyToMany {
            let parent = fkey.as_ref().unwrap().split_once(' ').unwrap().0;
            write!(
                table.buf.borrow_mut(),
                "COPY {}_{} ({}, {}) FROM stdin;\n",
                parent,
                table.name,
                parent,
                table.name
            )
            .unwrap();
        } else {
            let cols = table
                .columns
                .iter()
                .filter_map(|c| {
                    if c.hide
                        || (c.subtable.is_some()
                            && c.subtable.as_ref().unwrap().cardinality != Cardinality::ManyToOne)
                    {
                        return None;
                    }
                    Some(String::from(&c.name))
                })
                .collect::<Vec<String>>()
                .join(", ");
            if fkey.is_some() {
                write!(
                    table.buf.borrow_mut(),
                    "COPY {} ({}, {}) FROM stdin;\n",
                    table.name,
                    fkey.unwrap().split(' ').next().unwrap(),
                    cols
                )
                .unwrap();
            } else {
                write!(
                    table.buf.borrow_mut(),
                    "COPY {} ({}) FROM stdin;\n",
                    table.name,
                    cols
                )
                .unwrap();
            }
        }
    }
    table.flush();
}
