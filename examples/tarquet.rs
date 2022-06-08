use fileslice::slice_tarball;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let usage = "Usage: tarquet <file> <table>";
    let path = std::env::args().nth(1).ok_or(usage)?;
    let table = PathBuf::from(std::env::args().nth(2).ok_or(usage)?);
    let file = std::fs::File::open(path)?;
    let archive = tar::Archive::new(file);
    for (header, slice) in slice_tarball(archive)? {
        if header.path()? == table {
            let rdr = SerializedFileReader::new(slice).unwrap();
            for row in rdr.get_row_iter(None).unwrap() {
                println!("{:?}", row);
            }
        }
    }
    Ok(())
}
