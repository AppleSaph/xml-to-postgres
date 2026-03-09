use std::borrow::Cow;
use std::fmt::Write as _;
use cow_utils::CowUtils;
use quick_xml::events::Event;

use crate::models::{Column, Geometry, Settings, State, Step, Table};
use crate::geometry::{gml_to_coord, gml_to_ewkb};

pub fn check_columns_used(table: &Table) {
    for col in &table.columns {
        if col.subtable.is_some() {
            let sub = col.subtable.as_ref().unwrap();
            check_columns_used(sub);
        } else if !*col.used.borrow() {
            eprintln!(
                "Warning: table {} column {} was never found",
                table.name, col.name
            );
        }
    }
}

pub fn process_event(event: &Event, state: &mut State) -> Step {
    match event {
        Event::Decl(ref e) => {
            handle_declaration(e, state);
        }
        Event::Start(ref e) => {
            return handle_start_event(e, state);
        }
        Event::Text(ref e) => {
            return handle_text_event(e, state);
        }
        Event::End(_) => {
            return handle_end_event(state);
        }
        Event::Eof => return Step::Done,
        _ => (),
    }

    Step::Next
}

fn handle_declaration(e: &quick_xml::events::BytesDecl, state: &State) {
    if !state.settings.hush_version && !state.settings.hush_info {
        eprintln!(
            "Info: reading XML version {} with encoding {}",
            std::str::from_utf8(
                &e.version()
                    .unwrap_or_else(|_| panic!("Error: missing or invalid XML version attribute"))
            )
            .unwrap(),
            std::str::from_utf8(match e.encoding() {
                Some(Ok(Cow::Borrowed(encoding))) => encoding,
                _ => b"unknown",
            })
            .unwrap()
        );
    }
}

fn handle_start_event(e: &quick_xml::events::BytesStart, state: &mut State) -> Step {
    if state.step != Step::Repeat {
        state.path.push('/');
        state.path.push_str(
            &state
                .reader
                .decoder()
                .decode(e.name().as_ref())
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Error: failed to decode XML tag '{}': {}",
                        String::from_utf8_lossy(e.name().as_ref()),
                        err
                    );
                    std::process::exit(1);
                }),
        );
    }

    if let Some(path) = &state.deferred {
        if state.path.starts_with(path) {
            return Step::Defer;
        }
    }

    if state.filtered || state.skipped {
        return Step::Next;
    }

    // Check subtable conditions first before borrowing
    if !state.tables.is_empty() && path_match(&state.path, &state.table.path) {
        if state.table.cardinality != crate::models::Cardinality::ManyToOne {
            if state.tables.last().unwrap().lastid.borrow().is_empty() {
                if state.deferred.is_some() {
                    eprintln!("Error: you have multiple subtables that precede the parent table id column; this is not currently supported");
                    std::process::exit(1);
                }
                state.deferred = Some(state.path.clone());
                return Step::Defer;
            }
        }
    }

    if path_match(&state.path, &state.settings.skip) {
        state.skipped = true;
        return Step::Next;
    } else if state.concattext {
        return Step::Next;
    } else if state.xmltotext {
        state.text.push_str(&format!(
            "<{}>",
            state
                .reader
                .decoder()
                .decode(e.name().as_ref())
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Error: failed to decode XML tag '{}': {}",
                        String::from_utf8_lossy(e.name().as_ref()),
                        err
                    );
                    std::process::exit(1);
                })
        ));
        return Step::Next;
    } else if state.gmltoewkb || state.gmltocoord {
        handle_gml_start(e, state);
        return Step::Next;
    }

    if path_match(&state.path, &state.table.path) {
        state.table.lastid.borrow_mut().clear();
    }
    if path_match(&state.path, &state.rowpath) {
        state.fullcount += 1;
    }

    let subtable = process_columns_for_start(e, state);

    if let Some(i) = subtable {
        // Borrow table only when we need to push it
        let table = &state.table;
        state.tables.push(table);
        state.parentcol = Some(&table.columns[i]);
        state.table = table.columns[i].subtable.as_ref().unwrap();
        return Step::Repeat;
    }

    Step::Next
}

fn handle_gml_start(e: &quick_xml::events::BytesStart, state: &mut State) {
    match state.reader.decoder().decode(e.name().as_ref()) {
        Err(_) => (),
        Ok(tag) => match tag.as_ref() {
            "gml:Point" => {
                state.gmlcoll.push(Geometry::new(1));
                state.gmlcoll.last_mut().unwrap().rings.push(Vec::new());
            }
            "gml:LineString" => {
                state.gmlcoll.push(Geometry::new(2));
                state.gmlcoll.last_mut().unwrap().rings.push(Vec::new());
            }
            "gml:Polygon" => state.gmlcoll.push(Geometry::new(3)),
            "gml:MultiPolygon" => (),
            "gml:polygonMember" => (),
            "gml:exterior" => (),
            "gml:interior" => (),
            "gml:LinearRing" => state.gmlcoll.last_mut().unwrap().rings.push(Vec::new()),
            "gml:posList" => state.gmlpos = true,
            "gml:pos" => state.gmlpos = true,
            _ => {
                if !state.settings.hush_warning {
                    eprintln!("Warning: GML type {} not supported", tag);
                }
            }
        },
    }

    for res in e.attributes() {
        match res {
            Err(_) => (),
            Ok(attr) => {
                let key = state.reader.decoder().decode(attr.key.as_ref());
                if key.is_err() {
                    continue;
                }
                match key.unwrap().as_ref() {
                    "srsName" => {
                        let mut value = String::from(
                            state
                                .reader
                                .decoder()
                                .decode(&attr.value)
                                .unwrap_or_else(|err| {
                                    eprintln!(
                                        "Error: failed to decode XML attribute '{}': {}",
                                        String::from_utf8_lossy(&attr.value),
                                        err
                                    );
                                    std::process::exit(1);
                                }),
                        );
                        if let Some(i) = value.rfind("::") {
                            value = value.split_off(i + 2);
                        }
                        match value.parse::<u32>() {
                            Ok(int) => {
                                if let Some(geom) = state.gmlcoll.last_mut() {
                                    geom.srid = int
                                };
                            }
                            Err(_) => {
                                if !state.settings.hush_warning {
                                    eprintln!("Warning: invalid srsName {} in GML", value);
                                }
                            }
                        }
                    }
                    "srsDimension" => {
                        let value = state
                            .reader
                            .decoder()
                            .decode(&attr.value)
                            .unwrap_or_else(|err| {
                                eprintln!(
                                    "Error: failed to decode XML attribute '{}': {}",
                                    String::from_utf8_lossy(&attr.value),
                                    err
                                );
                                std::process::exit(1);
                            });
                        match value.parse::<u8>() {
                            Ok(int) => {
                                if let Some(geom) = state.gmlcoll.last_mut() {
                                    geom.dims = int
                                };
                            }
                            Err(_) => {
                                if !state.settings.hush_warning {
                                    eprintln!("Warning: invalid srsDimension {} in GML", value);
                                }
                            }
                        }
                    }
                    _ => (),
                }
            }
        }
    }
}

fn process_columns_for_start(e: &quick_xml::events::BytesStart, state: &mut State) -> Option<usize> {
    let table = &state.table;
    let mut subtable = None;

    for i in 0..table.columns.len() {
        if path_match(&state.path, &table.columns[i].path) {
            if let Some(ref serial) = table.columns[i].serial {
                if table.columns[i].value.borrow().is_empty() {
                    let id = serial.get() + 1;
                    let idstr = id.to_string();
                    table.columns[i].value.borrow_mut().push_str(&idstr);
                    table.lastid.borrow_mut().push_str(&idstr);
                    serial.set(id);
                    continue;
                }
            }

            if let Some(ref fkey) = table.columns[i].fkey {
                if table.columns[i].value.borrow().is_empty() {
                    for parent in &state.tables {
                        if parent.name != fkey.0 {
                            continue;
                        }
                        for col in &parent.columns {
                            if col.name == fkey.1 {
                                let mut column = table.columns[i].value.borrow_mut();
                                column.clear();
                                column.push_str(&col.value.borrow());
                            }
                        }
                    }
                }
            }

            if table.columns[i].subtable.is_some() {
                if subtable.is_some() {
                    eprintln!("Error: multiple subtables starting from the same element is not supported");
                    std::process::exit(1);
                }
                subtable = Some(i);
            }

            if let Some(request) = table.columns[i].attr {
                process_attribute(&table.columns[i], e, request, i, state);
                continue;
            }

            match table.columns[i].convert {
                None => (),
                Some("xml-to-text") => state.xmltotext = true,
                Some("gml-to-ewkb") => state.gmltoewkb = true,
                Some("gml-to-coord") => state.gmltocoord = true,
                Some("concat-text") => state.concattext = true,
                Some(_) => (),
            }
        }
    }

    subtable
}

fn process_attribute(
    _column: &Column,
    e: &quick_xml::events::BytesStart,
    request: &str,
    column_index: usize,
    state: &State,
) {
    let table = &state.table;
    for res in e.attributes() {
        if let Ok(attr) = res {
            if let Ok(key) = state.reader.decoder().decode(attr.key.as_ref()) {
                if key == request {
                    if let Ok(value) = state.reader.decoder().decode(&attr.value) {
                        if !table.columns[column_index].value.borrow().is_empty() {
                            if !allow_iteration(&table.columns[column_index], &state.settings) {
                                break;
                            }
                            if let Some("last") = table.columns[column_index].aggr {
                                table.columns[column_index].value.borrow_mut().clear();
                            }
                        }
                        if column_index == 0 {
                            table.lastid.borrow_mut().push_str(&value);
                        }
                        if let (Some(regex), Some(replacer)) = (
                            table.columns[column_index].find.as_ref(),
                            table.columns[column_index].replace,
                        ) {
                            table.columns[column_index]
                                .value
                                .borrow_mut()
                                .push_str(&regex.replace_all(&value, replacer));
                        } else {
                            table.columns[column_index]
                                .value
                                .borrow_mut()
                                .push_str(&value);
                        }
                    } else if !state.settings.hush_warning {
                        eprintln!(
                            "Warning: failed to decode attribute {} for column {}",
                            request, table.columns[column_index].name
                        );
                    }
                }
            } else if !state.settings.hush_warning {
                eprintln!(
                    "Warning: failed to decode an attribute for column {}",
                    table.columns[column_index].name
                );
            }
        } else if !state.settings.hush_warning {
            eprintln!(
                "Warning: failed to read attributes for column {}",
                table.columns[column_index].name
            );
        }
    }
    if table.columns[column_index].value.borrow().is_empty() && !state.settings.hush_warning {
        eprintln!(
            "Warning: column {} requested attribute {} not found",
            table.columns[column_index].name, request
        );
    }
}

fn handle_text_event(e: &quick_xml::events::BytesText, state: &mut State) -> Step {
    if let Some(path) = &state.deferred {
        if state.path.starts_with(path) {
            return Step::Defer;
        }
    }
    if state.filtered || state.skipped {
        return Step::Next;
    }

    if state.concattext {
        if !state.text.is_empty() {
            state.text.push(' ');
        }
        state.text.push_str(&e.unescape().unwrap_or_else(|err| {
            eprintln!(
                "Error: failed to decode XML text node '{}': {}",
                String::from_utf8_lossy(e),
                err
            );
            std::process::exit(1);
        }));
        return Step::Next;
    } else if state.xmltotext {
        state.text.push_str(&e.unescape().unwrap_or_else(|err| {
            eprintln!(
                "Error: failed to decode XML text node '{}': {}",
                String::from_utf8_lossy(e),
                err
            );
            std::process::exit(1);
        }));
        return Step::Next;
    } else if state.gmltoewkb || state.gmltocoord {
        if state.gmlpos {
            let value = String::from(e.unescape().unwrap_or_else(|err| {
                eprintln!(
                    "Error: failed to decode XML gmlpos '{}': {}",
                    String::from_utf8_lossy(e),
                    err
                );
                std::process::exit(1);
            }));
            for pos in value.split(' ') {
                state
                    .gmlcoll
                    .last_mut()
                    .unwrap()
                    .rings
                    .last_mut()
                    .unwrap()
                    .push(pos.parse::<f64>().unwrap_or_else(|err| {
                        eprintln!("Error: failed to parse GML pos '{}' into float: {}", pos, err);
                        std::process::exit(1);
                    }));
            }
        }
        return Step::Next;
    }

    let table = &state.table;
    for i in 0..table.columns.len() {
        if path_match(&state.path, &table.columns[i].path) {
            if table.columns[i].attr.is_some() || table.columns[i].serial.is_some() {
                continue;
            }
            if !table.columns[i].value.borrow().is_empty() {
                if !allow_iteration(&table.columns[i], &state.settings) {
                    return Step::Next;
                }
                if let Some("last") = table.columns[i].aggr {
                    table.columns[i].value.borrow_mut().clear();
                }
            }
            let decoded = e.unescape().unwrap_or_else(|err| {
                eprintln!(
                    "Error: failed to decode XML text node '{}': {}",
                    String::from_utf8_lossy(e),
                    err
                );
                std::process::exit(1);
            });
            if table.columns[i].trim {
                let trimmed = state.trimre.replace_all(&decoded, " ");
                table.columns[i]
                    .value
                    .borrow_mut()
                    .push_str(&trimmed.cow_replace("\\", "\\\\").cow_replace("\t", "\\t"));
            } else {
                table.columns[i].value.borrow_mut().push_str(
                    &decoded
                        .cow_replace("\\", "\\\\")
                        .cow_replace("\r", "\\r")
                        .cow_replace("\n", "\\n")
                        .cow_replace("\t", "\\t"),
                );
            }
            if let (Some(regex), Some(replacer)) =
                (table.columns[i].find.as_ref(), table.columns[i].replace)
            {
                let mut value = table.columns[i].value.borrow_mut();
                *value = regex.replace_all(&value, replacer).to_string();
            }
            if i == 0 {
                table
                    .lastid
                    .borrow_mut()
                    .push_str(&table.columns[0].value.borrow());
            }
            return Step::Next;
        }
    }

    Step::Next
}

fn handle_end_event(state: &mut State) -> Step {
    let table = &state.table;

    if let Some(path) = &state.deferred {
        if state.path.starts_with(path) {
            if path_match(&state.path, &table.path) && !state.tables.is_empty() {
                state.table = state.tables.pop().unwrap();
            }
            let i = state.path.rfind('/').unwrap();
            state.path.truncate(i);
            return Step::Defer;
        }
    }

    if state.concattext {
        for i in 0..table.columns.len() {
            if path_match(&state.path, &table.columns[i].path) {
                state.concattext = false;
                table.columns[i].value.borrow_mut().push_str(&state.text);
                state.text.clear();
            }
        }
    }

    if path_match(&state.path, &table.path) {
        return handle_row_end(state);
    } else if state.skipped && path_match(&state.path, &state.settings.skip) {
        state.skipped = false;
        state.skipcount += 1;
    }

    if let Some(path) = &state.deferred {
        if path_match(&state.path, &table.path) && state.path.len() < path.len() {
            return Step::Apply;
        }
    }

    let i = state
        .path
        .rfind('/')
        .expect("no slash in path; shouldn't happen");
    let tag = state.path.split_off(i);

    if state.xmltotext {
        handle_xmltotext_end(&tag, state);
    } else if state.gmltoewkb {
        handle_gmltoewkb_end(&tag, state);
    } else if state.gmltocoord {
        handle_gmltocoord_end(&tag, state);
    }

    Step::Next
}

fn handle_row_end(state: &mut State) -> Step {
    let table = &state.table;

    for i in 0..table.columns.len() {
        if !*table.columns[i].used.borrow() && !table.columns[i].value.borrow().is_empty() {
            *state.table.columns[i].used.borrow_mut() = true;
        }
        if let Some(re) = &table.columns[i].include {
            if !re.is_match(&table.columns[i].value.borrow()) {
                state.filtered = true;
            }
        }
        if let Some(re) = &table.columns[i].exclude {
            if re.is_match(&table.columns[i].value.borrow()) {
                state.filtered = true;
            }
        }
    }

    if state.filtered {
        state.filtered = false;
        table.clear_columns();
        if state.tables.is_empty() {
            state.filtercount += 1;
        } else {
            state.table = state.tables.pop().unwrap();
            return Step::Repeat;
        }
    } else {
        if !state.tables.is_empty() {
            return handle_subtable_row_end(state);
        }

        write_row_data(table);
    }

    if !state.tables.is_empty() {
        state.table = state.tables.pop().unwrap();
        return Step::Repeat;
    }

    Step::Next
}

fn handle_subtable_row_end(state: &mut State) -> Step {
    let cardinality = state.table.cardinality;
    let has_domain = state.table.domain.as_ref().is_some();

    if cardinality != crate::models::Cardinality::ManyToOne {
        let key = state.tables.last().unwrap().lastid.borrow();
        if key.is_empty() && !state.settings.hush_warning {
            println!(
                "Warning: subtable {} has no foreign key for parent (you may need to add a 'seri' column)",
                state.table.name
            );
        }
        write!(state.table.buf.borrow_mut(), "{}\t", key).unwrap();

        if has_domain {
            if let Some(ref domain) = *state.table.domain {
                handle_subtable_with_domain(state, domain);
                let table = &state.table;
                table.flush();
                table.clear_columns();
                state.table = state.tables.pop().unwrap();
                return Step::Repeat;
            }
        }
    } else {
        if has_domain {
            if let Some(ref domain) = *state.table.domain {
                return handle_manytone_with_domain(state, domain);
            }
        }
        if state.parentcol.unwrap().value.borrow().is_empty() {
            let lastid = state.table.lastid.borrow().to_string();
            state
                .parentcol
                .unwrap()
                .value
                .borrow_mut()
                .push_str(&lastid);
        } else if allow_iteration(state.parentcol.unwrap(), &state.settings) {
            // TODO: make it do something...
        }
    }

    write_row_data(&state.table);

    if !state.tables.is_empty() {
        state.table = state.tables.pop().unwrap();
        return Step::Repeat;
    }

    Step::Next
}

fn handle_subtable_with_domain(state: &mut State, domain: &std::cell::RefCell<crate::models::Domain>) {
    let table = &state.table;
    let mut domain = domain.borrow_mut();
    let key = match table.columns[0].serial {
        Some(_) => table.columns[1..]
            .iter()
            .map(|c| c.value.borrow().to_string())
            .collect::<String>(),
        None => table.lastid.borrow().to_string(),
    };

    let rowid;
    if !domain.map.contains_key(&key) {
        domain.lastid += 1;
        rowid = domain.lastid;
        domain.map.insert(key, rowid);
        if table.columns.len() == 1 {
            write!(domain.table.buf.borrow_mut(), "{}\t", rowid).unwrap();
        }
        for i in 0..table.columns.len() {
            if table.columns[i].subtable.is_some() {
                continue;
            }
            if table.columns[i].hide {
                continue;
            }
            if i > 0 {
                write!(domain.table.buf.borrow_mut(), "\t").unwrap();
            }
            if table.columns[i].value.borrow().is_empty() {
                write!(domain.table.buf.borrow_mut(), "\\N").unwrap();
            } else if let Some(domain) = table.columns[i].domain.as_ref() {
                let mut domain = domain.borrow_mut();
                let id = match domain.map.get(&table.columns[i].value.borrow().to_string()) {
                    Some(id) => *id,
                    None => {
                        domain.lastid += 1;
                        let id = domain.lastid;
                        domain
                            .map
                            .insert(table.columns[i].value.borrow().to_string(), id);
                        write!(
                            domain.table.buf.borrow_mut(),
                            "{}\t{}\n",
                            id,
                            *table.columns[i].value.borrow()
                        )
                        .unwrap();
                        domain.table.flush();
                        id
                    }
                };
                write!(domain.table.buf.borrow_mut(), "{}", id).unwrap();
            } else {
                write!(
                    domain.table.buf.borrow_mut(),
                    "{}",
                    &table.columns[i].value.borrow()
                )
                .unwrap();
            }
        }
        write!(domain.table.buf.borrow_mut(), "\n").unwrap();
        domain.table.flush();
    } else {
        rowid = *domain.map.get(&key).unwrap();
    }

    if table.columns.len() == 1 {
        write!(table.buf.borrow_mut(), "{}", rowid).unwrap();
    } else {
        if table.lastid.borrow().is_empty() && !state.settings.hush_warning {
            println!(
                "Warning: subtable {} has no primary key to normalize on",
                table.name
            );
        }
        write!(table.buf.borrow_mut(), "{}", table.lastid.borrow()).unwrap();
    }
    write!(table.buf.borrow_mut(), "\n").unwrap();
}

fn handle_manytone_with_domain(state: &mut State, domain: &std::cell::RefCell<crate::models::Domain>) -> Step {
    let table = &state.table;
    let mut domain = domain.borrow_mut();
    let key = match table.columns[0].serial {
        Some(_) => table.columns[1..]
            .iter()
            .map(|c| c.value.borrow().to_string())
            .collect::<String>(),
        None => table.lastid.borrow().to_string(),
    };

    if domain.map.contains_key(&key) {
        if table.columns[0].serial.is_some() {
            state
                .parentcol
                .unwrap()
                .value
                .borrow_mut()
                .push_str(&format!("{}", *domain.map.get(&key).unwrap()));
        } else {
            state
                .parentcol
                .unwrap()
                .value
                .borrow_mut()
                .push_str(&table.lastid.borrow());
        }
        table.clear_columns();
        state.table = state.tables.pop().unwrap();
        return Step::Repeat;
    }

    domain.lastid += 1;
    let id = domain.lastid;
    domain.map.insert(key, id);

    if state.parentcol.unwrap().value.borrow().is_empty() {
        state
            .parentcol
            .unwrap()
            .value
            .borrow_mut()
            .push_str(&table.lastid.borrow());
    } else if allow_iteration(state.parentcol.unwrap(), &state.settings) {
        // TODO: make it do something...
    }

    Step::Repeat
}

fn write_row_data(table: &Table) {
    for i in 0..table.columns.len() {
        if table.columns[i].subtable.is_some()
            && table.columns[i].subtable.as_ref().unwrap().cardinality
                != crate::models::Cardinality::ManyToOne
        {
            continue;
        }
        if table.columns[i].hide {
            table.columns[i].value.borrow_mut().clear();
            continue;
        }
        if i > 0 {
            write!(table.buf.borrow_mut(), "\t").unwrap();
        }
        if table.columns[i].value.borrow().is_empty() {
            write!(table.buf.borrow_mut(), "\\N").unwrap();
        } else if let Some(domain) = table.columns[i].domain.as_ref() {
            let mut domain = domain.borrow_mut();
            let id = match domain.map.get(&table.columns[i].value.borrow().to_string()) {
                Some(id) => *id,
                None => {
                    domain.lastid += 1;
                    let id = domain.lastid;
                    domain
                        .map
                        .insert(table.columns[i].value.borrow().to_string(), id);
                    write!(
                        domain.table.buf.borrow_mut(),
                        "{}\t{}\n",
                        id,
                        *table.columns[i].value.borrow()
                    )
                    .unwrap();
                    domain.table.flush();
                    id
                }
            };
            write!(table.buf.borrow_mut(), "{}", id).unwrap();
            table.columns[i].value.borrow_mut().clear();
        } else {
            write!(table.buf.borrow_mut(), "{}", &table.columns[i].value.borrow()).unwrap();
            table.columns[i].value.borrow_mut().clear();
        }
    }
    write!(table.buf.borrow_mut(), "\n").unwrap();
    table.flush();
}

fn handle_xmltotext_end(tag: &str, state: &mut State) {
    let table = &state.table;
    state.text.push_str(&format!("<{}>", tag));
    for i in 0..table.columns.len() {
        if path_match(&state.path, &table.columns[i].path) {
            state.xmltotext = false;
            if let (Some(regex), Some(replacer)) =
                (table.columns[i].find.as_ref(), table.columns[i].replace)
            {
                state.text = regex.replace_all(&state.text, replacer).to_string();
            }
            table.columns[i].value.borrow_mut().push_str(&state.text);
            state.text.clear();
            return;
        }
    }
}

fn handle_gmltoewkb_end(tag: &str, state: &mut State) {
    let table = &state.table;
    if state.gmlpos && ((tag == "/gml:pos") || (tag == "/gml:posList")) {
        state.gmlpos = false;
    }
    for i in 0..table.columns.len() {
        if path_match(&state.path, &table.columns[i].path) {
            state.gmltoewkb = false;
            if !gml_to_ewkb(
                &table.columns[i].value,
                &state.gmlcoll,
                table.columns[i].bbox.as_ref(),
                table.columns[i].multitype,
                &state.settings,
            ) {
                state.filtered = true;
            }
            state.gmlcoll.clear();
            return;
        }
    }
}

fn handle_gmltocoord_end(tag: &str, state: &mut State) {
    let table = &state.table;
    if state.gmlpos && ((tag == "/gml:pos") || (tag == "/gml:posList")) {
        state.gmlpos = false;
    }
    for i in 0..table.columns.len() {
        if path_match(&state.path, &table.columns[i].path) {
            state.gmltocoord = false;
            if !gml_to_coord(&table.columns[i].value, &state.gmlcoll, &state.settings) {
                state.filtered = true;
            }
            state.gmlcoll.clear();
            return;
        }
    }
}

pub fn path_match(path: &String, mask: &String) -> bool {
    if !mask.contains("*") && !mask.contains("{") {
        return path == mask;
    }
    glob_match::glob_match(mask, path)
}

pub fn allow_iteration(column: &Column, settings: &Settings) -> bool {
    match column.aggr {
        None if settings.hush_warning => false,
        None => {
            eprintln!(
                "Warning: column '{}' has multiple occurrences without an aggregation method; using 'first'",
                column.name
            );
            false
        }
        Some("first") => false,
        Some("last") => true,
        Some("append") => {
            if !column.value.borrow().is_empty() {
                column.value.borrow_mut().push(',');
            }
            true
        }
        _ => true,
    }
}

