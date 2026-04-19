use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/gtfs-realtime.proto");
    prost_build::compile_protos(&["proto/gtfs-realtime.proto"], &["proto/"])?;
    Ok(())
}
