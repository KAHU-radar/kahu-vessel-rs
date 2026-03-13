fn main() {
    // Generate Rust types from RadarMessage.proto at compile time.
    // The output lands in $OUT_DIR/protos/mod.rs and is included via src/protos.rs.
    protobuf_codegen::Codegen::new()
        .pure()
        .includes(&["."])
        .input("RadarMessage.proto")
        .cargo_out_dir("protos")
        .run_from_script();

    println!("cargo::rerun-if-changed=RadarMessage.proto");
    println!("cargo::rerun-if-changed=build.rs");
}
