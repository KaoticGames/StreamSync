fn main() {
    let icon_dir = std::path::Path::new("icons");
    let icon_path = icon_dir.join("icon.png");
    if !icon_path.is_file() {
        std::fs::create_dir_all(icon_dir).expect("create Tauri icons directory");
        // Deterministic non-secret 1x1 PNG used only when a clean checkout has no release icon.
        let png = decode_base64(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
        );
        std::fs::write(&icon_path, png).expect("write placeholder Tauri icon");
    }
    let ico_path = icon_dir.join("icon.ico");
    if !ico_path.is_file() {
        let png = std::fs::read(&icon_path).expect("read placeholder Tauri icon");
        let mut ico = vec![
            0, 0, 1, 0, 1, 0, // ICO header: one image
            1, 1, 0, 0, 1, 0, 32, 0, // 1x1, RGBA
        ];
        ico.extend_from_slice(&(png.len() as u32).to_le_bytes());
        ico.extend_from_slice(&22u32.to_le_bytes());
        ico.extend_from_slice(&png);
        std::fs::write(ico_path, ico).expect("write placeholder Tauri Windows icon");
    }
    tauri_build::build()
}

fn decode_base64(input: &str) -> Vec<u8> {
    let mut output = Vec::new();
    let mut bits = 0u32;
    let mut count = 0u8;
    for byte in input.bytes().filter(|byte| *byte != b'=') {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => continue,
        };
        bits = (bits << 6) | u32::from(value);
        count += 6;
        if count >= 8 {
            count -= 8;
            output.push((bits >> count) as u8);
            bits &= (1 << count) - 1;
        }
    }
    output
}
