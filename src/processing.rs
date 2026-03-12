use crate::geometry::{gml_to_coord, gml_to_ewkb};
use crate::models::{Cardinality, Column, Domain, Geometry, Settings, State, Step, Table};
use cow_utils::CowUtils;
use glob_match::glob_match;
use quick_xml::events::Event;
use std::borrow::Cow;
use std::cell::RefMut;
use std::io::Write;

pub(crate) fn check_columns_used(table: &Table) {
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

pub(crate) fn process_event(event: &Event, state: &mut State) -> Step {
    let table = &state.table;
    match event {
        Event::Decl(ref e) => {
            if !state.settings.hush_version && !state.settings.hush_info {
                eprintln!(
                    "Info: reading XML version {} with encoding {}",
                    str::from_utf8(&e.version().unwrap_or_else(|_| crate::fatalerr!(
                        "Error: missing or invalid XML version attribute: {:#?}",
                        e.as_ref()
                    )))
                    .unwrap(),
                    str::from_utf8(match e.encoding() {
                        Some(Ok(Cow::Borrowed(encoding))) => encoding,
                        _ => b"unknown",
                    })
                    .unwrap()
                );
            }
        }
        Event::Start(ref e) => {
            if state.step != Step::Repeat {
                state.path.push('/');
                state.path.push_str(
                    &state
                        .reader
                        .decoder()
                        .decode(e.name().as_ref())
                        .unwrap_or_else(|err| {
                            crate::fatalerr!(
                                "Error: failed to decode XML tag '{}': {}",
                                String::from_utf8_lossy(e.name().as_ref()),
                                err
                            )
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
            if !state.tables.is_empty() && path_match(&state.path, &table.path) {
                // Start of a subtable
                if table.cardinality != Cardinality::ManyToOne {
                    // Subtable needs a foreign key from parent
                    if state.tables.last().unwrap().lastid.borrow().is_empty() {
                        if state.deferred.is_some() {
                            crate::fatalerr!("Error: you have multiple subtables that precede the parent table id column; this is not currently supported");
                        }
                        // println!("Defer subtable {}", table.name);
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
                        .unwrap_or_else(|err| crate::fatalerr!(
                            "Error: failed to decode XML tag '{}': {}",
                            String::from_utf8_lossy(e.name().as_ref()),
                            err
                        ))
                ));
                return Step::Next;
            } else if state.gmltoewkb || state.gmltocoord {
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
                        "gml:LinearRing" => {
                            state.gmlcoll.last_mut().unwrap().rings.push(Vec::new())
                        }
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
                                    let mut value = String::from(state.reader.decoder().decode(&attr.value).unwrap_or_else(|err| crate::fatalerr!("Error: failed to decode XML attribute '{}': {}", String::from_utf8_lossy(&attr.value), err)));
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
                                                eprintln!(
                                                    "Warning: invalid srsName {} in GML",
                                                    value
                                                );
                                            }
                                        }
                                    }
                                }
                                "srsDimension" => {
                                    let value =
                                        state.reader.decoder().decode(&attr.value).unwrap_or_else(
                                            |err| {
                                                crate::fatalerr!(
                                                "Error: failed to decode XML attribute '{}': {}",
                                                String::from_utf8_lossy(&attr.value),
                                                err
                                            )
                                            },
                                        );
                                    match value.parse::<u8>() {
                                        Ok(int) => {
                                            if let Some(geom) = state.gmlcoll.last_mut() {
                                                geom.dims = int
                                            };
                                        }
                                        Err(_) => {
                                            if !state.settings.hush_warning {
                                                eprintln!(
                                                    "Warning: invalid srsDimension {} in GML",
                                                    value
                                                );
                                            }
                                        }
                                    }
                                }
                                _ => (),
                            }
                        }
                    }
                }
                return Step::Next;
            }

            if path_match(&state.path, &table.path) {
                state.table.lastid.borrow_mut().clear();
            }
            if path_match(&state.path, &state.rowpath) {
                state.fullcount += 1;
            }
            let mut subtable = None;

            for i in 0..table.columns.len() {
                if path_match(&state.path, &table.columns[i].path) {
                    // This start tag matches one of the defined columns
                    // Handle the 'seri' case where this column is a virtual auto-incrementing serial
                    if let Some(ref serial) = table.columns[i].serial {
                        // if table.cardinality == Cardinality::ManyToOne { continue; }
                        if table.columns[i].value.borrow().is_empty() {
                            let id = serial.get() + 1;
                            let idstr = id.to_string();
                            table.columns[i].value.borrow_mut().push_str(&idstr);
                            table.lastid.borrow_mut().push_str(&idstr);
                            serial.set(id);
                            continue;
                        }
                    }
                    // Handle the 'fkey' case where this column contains a prior value
                    if let Some(ref fkey) = table.columns[i].fkey {
                        if table.columns[i].value.borrow().is_empty() {
                            for parent in &state.tables {
                                if parent.name != fkey.0 {
                                    continue;
                                }
                                for col in &parent.columns {
                                    if col.name == fkey.1 {
                                        // println!("Found fkey {}.{} with value {}", parent.name, col.name, col.value.borrow());
                                        let mut column = table.columns[i].value.borrow_mut();
                                        column.clear();
                                        column.push_str(&col.value.borrow());
                                    }
                                }
                            }
                        }
                    }
                    // Handle 'subtable' case (the 'cols' entry has 'cols' of its own)
                    if table.columns[i].subtable.is_some() {
                        if subtable.is_some() {
                            crate::fatalerr!("Error: multiple subtables starting from the same element is not supported");
                        }
                        subtable = Some(i);
                    }
                    // Handle the 'attr' case where the content is read from an attribute of this tag
                    if let Some(request) = table.columns[i].attr {
                        for res in e.attributes() {
                            if let Ok(attr) = res {
                                if let Ok(key) = state.reader.decoder().decode(attr.key.as_ref()) {
                                    if key == request {
                                        if let Ok(value) =
                                            state.reader.decoder().decode(&attr.value)
                                        {
                                            if !table.columns[i].value.borrow().is_empty() {
                                                if !allow_iteration(
                                                    &table.columns[i],
                                                    &state.settings,
                                                ) {
                                                    break;
                                                }
                                                if let Some("last") = table.columns[i].aggr {
                                                    table.columns[i].value.borrow_mut().clear();
                                                }
                                            }
                                            if i == 0 {
                                                table.lastid.borrow_mut().push_str(&value);
                                            }
                                            if let (Some(regex), Some(replacer)) = (
                                                table.columns[i].find.as_ref(),
                                                table.columns[i].replace,
                                            ) {
                                                table.columns[i]
                                                    .value
                                                    .borrow_mut()
                                                    .push_str(&regex.replace_all(&value, replacer));
                                            } else {
                                                table.columns[i]
                                                    .value
                                                    .borrow_mut()
                                                    .push_str(&value);
                                            }
                                        } else if !state.settings.hush_warning {
                                            eprintln!("Warning: failed to decode attribute {} for column {}", request, table.columns[i].name);
                                        }
                                    }
                                } else if !state.settings.hush_warning {
                                    eprintln!(
                                        "Warning: failed to decode an attribute for column {}",
                                        table.columns[i].name
                                    );
                                }
                            } else if !state.settings.hush_warning {
                                eprintln!(
                                    "Warning: failed to read attributes for column {}",
                                    table.columns[i].name
                                );
                            }
                        }
                        if table.columns[i].value.borrow().is_empty()
                            && !state.settings.hush_warning
                        {
                            eprintln!(
                                "Warning: column {} requested attribute {} not found",
                                table.columns[i].name, request
                            );
                        }
                        continue;
                    }
                    // Set the appropriate convert flag for the following data in case the 'conv' option is present
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
            if let Some(i) = subtable {
                state.tables.push(table);
                state.parentcol = Some(&table.columns[i]);
                state.table = table.columns[i].subtable.as_ref().unwrap();
                return Step::Repeat; // Continue the repeat loop because a subtable column may also match the current path
            }
        }
        Event::Text(ref e) => {
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
                    crate::fatalerr!(
                        "Error: failed to decode XML text node '{}': {}",
                        String::from_utf8_lossy(e),
                        err
                    )
                }));
                return Step::Next;
            } else if state.xmltotext {
                state.text.push_str(&e.unescape().unwrap_or_else(|err| {
                    crate::fatalerr!(
                        "Error: failed to decode XML text node '{}': {}",
                        String::from_utf8_lossy(e),
                        err
                    )
                }));
                return Step::Next;
            } else if state.gmltoewkb || state.gmltocoord {
                if state.gmlpos {
                    let value = String::from(e.unescape().unwrap_or_else(|err| {
                        crate::fatalerr!(
                            "Error: failed to decode XML gmlpos '{}': {}",
                            String::from_utf8_lossy(e),
                            err
                        )
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
                                crate::fatalerr!(
                                    "Error: failed to parse GML pos '{}' into float: {}",
                                    pos,
                                    err
                                )
                            }));
                    }
                }
                return Step::Next;
            }
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
                        crate::fatalerr!(
                            "Error: failed to decode XML text node '{}': {}",
                            String::from_utf8_lossy(e),
                            err
                        )
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
                    // println!("Table {} column {} value {}", table.name, table.columns[i].name, &table.columns[i].value.borrow());
                    if i == 0 {
                        table
                            .lastid
                            .borrow_mut()
                            .push_str(&table.columns[0].value.borrow());
                    }
                    return Step::Next;
                }
            }
        }
        Event::End(_) => {
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
                // This is an end tag of the row path
                for i in 0..table.columns.len() {
                    if !*table.columns[i].used.borrow()
                        && !table.columns[i].value.borrow().is_empty()
                    {
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
                    }
                    // Only count filtered for the main table
                    else {
                        // Subtable; nothing more to do in this case
                        state.table = state.tables.pop().unwrap();
                        return Step::Repeat;
                    }
                } else {
                    if !state.tables.is_empty() {
                        // This is a subtable
                        if table.cardinality != Cardinality::ManyToOne {
                            // Write the first column value of the parent table as the first column of the subtable (for use as a foreign key)
                            let key = state.tables.last().unwrap().lastid.borrow();
                            if key.is_empty() && !state.settings.hush_warning {
                                eprintln!("Warning: subtable {} has no foreign key for parent (you may need to add a 'seri' column)", table.name);
                            }
                            write!(table.buf.borrow_mut(), "{}\t", key).unwrap();
                            let rowid;
                            if let Some(domain) = table.domain.as_ref() {
                                let mut domain = domain.borrow_mut();
                                let key = get_key(table);
                                if !domain.map.contains_key(&key) {
                                    domain.lastid += 1;
                                    rowid = domain.lastid;
                                    domain.map.insert(key, rowid);
                                    if table.columns.len() == 1 {
                                        write!(domain.table.buf.borrow_mut(), "{}\t", rowid)
                                            .unwrap();
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
                                        } else if let Some(domain) =
                                            table.columns[i].domain.as_ref()
                                        {
                                            let mut domain = domain.borrow_mut();
                                            let id = get_id(&table, &i, &mut domain);
                                            write!(domain.table.buf.borrow_mut(), "{}", id)
                                                .unwrap();
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
                                    // Single column many-to-many subtable; needs the id from the domain map
                                    write!(table.buf.borrow_mut(), "{}", rowid).unwrap();
                                } else {
                                    if table.lastid.borrow().is_empty()
                                        && !state.settings.hush_warning
                                    {
                                        eprintln!("Warning: subtable {} has no primary key to normalize on", table.name);
                                    }
                                    write!(table.buf.borrow_mut(), "{}", table.lastid.borrow())
                                        .unwrap(); // This is a many-to-many relation; write the two keys into the link table
                                }
                                write!(table.buf.borrow_mut(), "\n").unwrap();
                                table.flush();
                                table.clear_columns();
                                state.table = state.tables.pop().unwrap();
                                return Step::Repeat;
                            }
                        } else {
                            // Many-to-one relation; write the id of this subtable into the parent table
                            if let Some(domain) = table.domain.as_ref() {
                                let mut domain = domain.borrow_mut();
                                let key = get_key(table);
                                if domain.map.contains_key(&key) {
                                    if table.columns[0].serial.is_some() {
                                        state.parentcol.unwrap().value.borrow_mut().push_str(
                                            &format!("{}", *domain.map.get(&key).unwrap()),
                                        );
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
                                // The for loop below will now write out the new row
                            }
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
                        }
                    }
                    // Now write out the other column values
                    for i in 0..table.columns.len() {
                        if table.columns[i].subtable.is_some()
                            && table.columns[i].subtable.as_ref().unwrap().cardinality
                                != Cardinality::ManyToOne
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
                            let id = get_id(&table, &i, &mut domain);
                            write!(table.buf.borrow_mut(), "{}", id).unwrap();
                            table.columns[i].value.borrow_mut().clear();
                        } else {
                            write!(
                                table.buf.borrow_mut(),
                                "{}",
                                &table.columns[i].value.borrow()
                            )
                            .unwrap();
                            table.columns[i].value.borrow_mut().clear();
                        }
                    }
                    write!(table.buf.borrow_mut(), "\n").unwrap();
                    table.flush();
                }
                if !state.tables.is_empty() {
                    state.table = state.tables.pop().unwrap();
                    return Step::Repeat;
                }
            } else if state.skipped && path_match(&state.path, &state.settings.skip) {
                state.skipped = false;
                state.skipcount += 1;
            }

            if let Some(path) = &state.deferred {
                if path_match(&state.path, &table.path) && state.path.len() < path.len() {
                    // We've just processed the deferred subtable's parent; apply the deferred events
                    return Step::Apply;
                }
            }

            let i = state
                .path
                .rfind('/')
                .expect("no slash in path; shouldn't happen");
            let tag = state.path.split_off(i);

            if state.xmltotext {
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
                        return Step::Next;
                    }
                }
            } else if state.gmltoewkb {
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
                        return Step::Next;
                    }
                }
            } else if state.gmltocoord {
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
                        return Step::Next;
                    }
                }
            }
        }
        Event::Eof => return Step::Done,
        _ => (),
    }

    Step::Next
}

fn get_key(table: &&Table) -> String {
    let key = match table.columns[0].serial {
        Some(_) => table.columns[1..]
            .iter()
            .map(|c| c.value.borrow().to_string())
            .collect::<String>(),
        None => table.lastid.borrow().to_string(),
    };
    key
}

fn get_id(table: &&&Table, i: &usize, domain: &mut RefMut<Domain>) -> u32 {
    let id = match domain
        .map
        .get(&table.columns[*i].value.borrow().to_string())
    {
        Some(id) => *id,
        None => {
            domain.lastid += 1;
            let id = domain.lastid;
            domain
                .map
                .insert(table.columns[*i].value.borrow().to_string(), id);
            write!(
                domain.table.buf.borrow_mut(),
                "{}\t{}\n",
                id,
                *table.columns[*i].value.borrow()
            )
            .unwrap();
            domain.table.flush();
            id
        }
    };
    id
}

fn path_match(path: &String, mask: &String) -> bool {
    if !mask.contains("*") && !mask.contains("{") {
        return path == mask;
    }
    glob_match(mask, path)
}

fn allow_iteration(column: &Column, settings: &Settings) -> bool {
    match column.aggr {
        None if settings.hush_warning => false,
        None => {
            eprintln!("Warning: column '{}' has multiple occurrences without an aggregation method; using 'first'", column.name);
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
