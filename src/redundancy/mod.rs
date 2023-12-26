mod redun;

pub use self::redun::{generate_key, EncKey, Nonce, PartialIndex, PartialIndexKind, Redundancy};

pub type Hash = [u8; 20];

pub fn redundancy(data1: &[u8], data2: &[u8], out: &mut [u8]) {
    assert_eq!(data1.len(), data2.len());
    assert_eq!(data1.len(), out.len());
    for i in 0..data1.len() {
        out[i] = data1[i] ^ data2[i];
    }
}

pub fn redundancy_copy(data1: &[u8], data2: &[u8], out: &mut [u8]) {
    let (short, long, short_len) = if data1.len() < data2.len() {
        (data1, data2, data1.len())
    } else {
        (data2, data1, data2.len())
    };
    let (long_xor, long_copy) = long.split_at(short_len);
    let (out_xor, out_copy) = out.split_at_mut(short_len);
    redundancy_copy_impl(short, long_xor, long_copy, out_xor, out_copy);
}

fn redundancy_copy_impl(
    short: &[u8],
    long_xor: &[u8],
    long_copy: &[u8],
    out_xor: &mut [u8],
    out_copy: &mut [u8],
) {
    redundancy(short, long_xor, out_xor);

    /*
     * Copy over the rest of the longer input file.
     */
    out_copy.copy_from_slice(long_copy);
}

#[cfg(test)]
mod tests {
    use redundancy::redundancy;
    use redundancy::redundancy_copy;

    #[test]
    fn test_redundancy() {
        let data1 = [0u8, 30, 128, 10, 84];
        let data2 = [90u8, 1, 74, 121, 3];
        let mut out = [0u8; 5];
        redundancy(&data1, &data2, &mut out);
        assert_eq!(out, [90u8, 31, 202, 115, 87]);
    }

    #[test]
    fn test_redundancy_copy() {
        let data1 = [0u8, 30, 128, 10, 84, b'a', b'b', b'c', b'd', b'e', b'f'];
        let data2 = [90u8, 1, 74, 121, 3];
        let mut out = [0u8; 11];
        redundancy_copy(&data1, &data2, &mut out);
        assert_eq!(
            out,
            [90u8, 31, 202, 115, 87, b'a', b'b', b'c', b'd', b'e', b'f']
        );
    }
}
