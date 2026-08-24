fn main() {
    let icon_dir = std::path::Path::new("icons");
    let icon_path = icon_dir.join("icon.png");
    if !icon_path.is_file() {
        std::fs::create_dir_all(icon_dir).expect("create Tauri icons directory");
        let png = rgba_1x1_png();
        assert_eq!(png[25], 6, "placeholder icon must be RGBA (color type 6)");
        std::fs::write(&icon_path, png).expect("write placeholder Tauri icon");
    } else {
        let existing = std::fs::read(&icon_path).expect("read existing Tauri icon");
        if existing.len() <= 25 || existing[25] != 6 {
            let png = rgba_1x1_png();
            std::fs::write(&icon_path, png).expect("rewrite invalid Tauri icon as RGBA");
        }
    }
    let ico_path = icon_dir.join("icon.ico");
    if !ico_path.is_file() {
        let png = std::fs::read(&icon_path).expect("read placeholder Tauri icon");
        write_minimal_ico(&ico_path, &png);
    }
    tauri_build::build()
}

/// Minimal valid 1x1 RGBA PNG (color type 6).
fn rgba_1x1_png() -> Vec<u8> {
    vec![
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // signature
        0x00, 0x00, 0x00, 0x0D, // IHDR length
        0x49, 0x48, 0x44, 0x52, // IHDR
        0x00, 0x00, 0x00, 0x01, // width
        0x00, 0x00, 0x00, 0x01, // height
        0x08, 0x06, 0x00, 0x00, 0x00, // 8-bit RGBA
        0x1F, 0x15, 0xC4, 0x89, // IHDR CRC
        0x00, 0x00, 0x00, 0x0A, // IDAT length
        0x49, 0x44, 0x41, 0x54, // IDAT
        0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01, // zlib payload
        0x0D, 0x0A, 0x2D, 0xB4, // IDAT CRC
        0x00, 0x00, 0x00, 0x00, // IEND length
        0x49, 0x45, 0x4E, 0x44, // IEND
        0xAE, 0x42, 0x60, 0x82, // IEND CRC
    ]
}

fn write_minimal_ico(path: &std::path::Path, png: &[u8]) {
    let mut ico = vec![0, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 0, 32, 0];
    ico.extend_from_slice(&(png.len() as u32).to_le_bytes());
    ico.extend_from_slice(&22u32.to_le_bytes());
    ico.extend_from_slice(png);
    std::fs::write(path, ico).expect("write placeholder Tauri Windows icon");
}
