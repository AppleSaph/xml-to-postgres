use std::cell::RefCell;
use std::io::Write;
use std::sync::mpsc;

pub fn write_output(file: RefCell<Box<dyn Write + Send>>, rx: mpsc::Receiver<Vec<u8>>) {
    while let Ok(buf) = rx.recv() {
        if buf.is_empty() {
            break;
        }
        file.borrow_mut()
            .write_all(&buf)
            .unwrap_or_else(|err| {
                eprintln!("Error: IO error encountered while writing table: {}", err);
                std::process::exit(1);
            })
    }
}

