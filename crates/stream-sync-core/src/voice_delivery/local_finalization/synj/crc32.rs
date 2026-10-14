//! IEEE CRC32 (zlib polynomial) for `.synj` record integrity.

pub fn crc32_buffer(data: &[u8]) -> u32 {
    static TABLE: [u32; 256] = {
        let mut t = [0u32; 256];
        let mut i = 0usize;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    0xedb8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            t[i] = c;
            i += 1;
        }
        t
    };
    let mut crc = 0xffff_ffffu32;
    for byte in data {
        let idx = ((crc ^ u32::from(*byte)) & 0xff) as usize;
        crc = TABLE[idx] ^ (crc >> 8);
    }
    crc ^ 0xffff_ffff
}
