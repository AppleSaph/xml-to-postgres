use std::cell::RefCell;
use std::io::Write;
use std::sync::mpsc;

pub fn write_output(file: RefCell<Box<dyn Write>>, rx: mpsc::Receiver<String>) {
    while let Ok(buf) = rx.recv() {
        if buf.len() == 0 {
            break;
        }
        file.borrow_mut()
            .write_all(buf.as_bytes())
            .unwrap_or_else(|err| {
                eprintln!("Error: IO error encountered while writing table: {}", err);
                std::process::exit(1);
            })
    }
}

