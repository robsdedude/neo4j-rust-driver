// Copyright Rouven Bauer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{Error as IoError, Read, Write};
use std::mem;

use thiserror::Error;

pub(super) fn read_var_int(mut read: impl Read) -> Result<u64, ReadVarIntError> {
    let mut buf = [0u8; varint_buffer_size::<u64>()];
    let read = read_var_int_buffer(&mut read, &mut buf)?;
    decode_var_uint(&buf[..read]).map_err(|err| match err {
        VarIntError::RemainingBytes { .. }
        | VarIntError::Incomplete(_)
        | VarIntError::BufferTooBig => {
            panic!("Should not have passed incorrect buffer: {err:?}");
        }
        VarIntError::TooBig => ReadVarIntError::TooBig,
    })
}

pub(super) fn write_var_int<W: Write>(mut write: W, mut val: u64) -> Result<(), IoError> {
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        write.write_all(&[byte])?;
        if val == 0 {
            break;
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub(super) enum ReadVarIntError {
    #[error("IO error: {0}")]
    Io(IoError),
    #[error("VarInt overflow")]
    TooBig,
}

fn read_var_int_buffer(mut read: impl Read, buf: &mut [u8]) -> Result<usize, ReadVarIntError> {
    let mut current_byte = [0u8; 1];
    let mut i = 0;
    let mut ignored_padding = false;
    loop {
        read.read_exact(&mut current_byte)
            .map_err(ReadVarIntError::Io)?;
        if i >= buf.len() {
            // ignore if only 0 padding
            if current_byte[0] & 0x7F != 0 {
                return Err(ReadVarIntError::TooBig);
            }
            ignored_padding = true;
        } else {
            buf[i] = current_byte[0];
            i += 1;
        }
        if current_byte[0] & 0x80 == 0 {
            break;
        }
    }
    if ignored_padding {
        // truncate padding by setting continuation bit of last byte to 0
        buf[i - 1] &= 0x7F;
    }
    Ok(i)
}

pub(super) fn decode_var_uint(bytes: &[u8]) -> Result<u64, VarIntError> {
    bytes
        .len()
        .checked_mul(7)
        .ok_or(VarIntError::BufferTooBig)?;
    let mut res = 0u64;
    let max_bits = mem::size_of::<u64>() * 8;
    for (i, byte) in bytes.iter().enumerate() {
        let part_u8 = byte & 0x7F;
        let part = (part_u8) as u64;
        let continue_flag = byte & 0x80;

        let shift = i * 7;
        if (shift + 7) > max_bits {
            let allowed_bits = max_bits.saturating_sub(shift);
            let forbidden_bit_mask = 0xFFu8
                .checked_shl(allowed_bits.try_into().unwrap_or(u32::MAX))
                .unwrap_or_default();
            if part_u8 & forbidden_bit_mask != 0 {
                return Err(VarIntError::TooBig);
            }
            if shift < max_bits {
                res |= part << shift;
            }
        } else {
            res |= part << shift;
        }

        if continue_flag == 0 {
            if i == bytes.len() - 1 {
                return Ok(res);
            }
            return Err(VarIntError::RemainingBytes {
                result: res,
                read: i + 1,
            });
        }
    }
    Err(VarIntError::Incomplete(bytes.to_vec()))
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub(super) enum VarIntError {
    #[error("VarInt too big")]
    TooBig,
    #[error("VarInt incomplete {0:02X?}")]
    Incomplete(Vec<u8>),
    #[error("VarInt buffer length must be <= usize::MAX / 7")]
    BufferTooBig,
    #[error("VarInt ended after {read} bytes, before end of buffer")]
    RemainingBytes { result: u64, read: usize },
}

const fn varint_buffer_size<T>() -> usize {
    let max_bits_t = mem::size_of::<T>() * 8;
    let (mut max_bytes_buf, rem) = (max_bits_t / 7, max_bits_t % 7);
    if rem > 0 {
        max_bytes_buf += 1;
    }
    max_bytes_buf
}

#[cfg(test)]
mod test {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case(&[0x00], 0)]
    #[case(&[0x80, 0x00], 0)] // some 0 padding
    // 0 padding that exceeds the max bits
    #[case(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], 0)]
    #[case(&[0x01], 1)]
    #[case(&[0x81, 0x00], 1)] // some 0 padding
    // 0 padding that exceeds the max bits
    #[case(&[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], 1)]
    #[case(&[0xFF, 0x01], 0xFF)]
    #[case(&[0xFF, 0x7F], 0x3FFF)]
    #[case(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F], i64::MAX as u64)]
    #[case(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01], u64::MAX)]
    fn test_decode_varint(#[case] input: &[u8], #[case] expected: u64) {
        let res = decode_var_uint(input).unwrap();
        assert_eq!(res, expected);
    }

    #[rstest]
    #[case(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02], VarIntError::TooBig)]
    #[case(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x82], VarIntError::TooBig)]
    #[case(
        &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80],
        VarIntError::Incomplete(vec![
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80
        ])
    )]
    #[case(&[0x80], VarIntError::Incomplete(vec![0x80]))]
    #[case(&[0x01, 0x00], VarIntError::RemainingBytes { result: 1, read: 1 })]
    #[case(&[0x01, 0xFF], VarIntError::RemainingBytes { result: 1, read: 1 })]
    #[case(&[0xFF, 0x7F, 0x00], VarIntError::RemainingBytes { result: 0x3FFF, read: 2 })]
    fn test_decode_varint_err(#[case] input: &[u8], #[case] expected: VarIntError) {
        let res = decode_var_uint(input).unwrap_err();
        assert_eq!(res, expected);
    }

    #[rstest]
    #[case(&[0x00], 0, 0)]
    #[case(&[0x80, 0x00], 0, 0)] // some 0 padding
    // 0 padding that exceeds the max bits
    #[case(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], 0, 0)]
    #[case(&[0x01], 1, 0)]
    #[case(&[0x81, 0x00], 1, 0)] // some 0 padding
    // 0 padding that exceeds the max bits
    #[case(&[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], 1, 0)]
    #[case(&[0xFF, 0x01], 0xFF, 0)]
    #[case(&[0xFF, 0x7F], 0x3FFF, 0)]
    #[case(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F], i64::MAX as u64, 0)]
    #[case(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01], u64::MAX, 0)]
    #[case(&[0x01, 0x00], 1, 1)]
    #[case(&[0x01, 0xFF], 1, 1)]
    #[case(&[0xFF, 0x7F, 0x00], 0x3FFF, 1)]
    #[case(&[0xFF, 0x7F, 0xFF, 0xFF], 0x3FFF, 2)]
    fn test_read_var_int(#[case] input: &[u8], #[case] expected: u64, #[case] remaining: usize) {
        let mut read = make_read(input);
        let res = read_var_int(&mut read).unwrap();
        assert_eq!(res, expected);

        let mut remaining_read = Vec::with_capacity(remaining);
        read.read_to_end(&mut remaining_read).unwrap();
        assert_eq!(remaining_read.len(), remaining);
    }

    #[rstest]
    #[case(
        make_read_err(IoError::new(std::io::ErrorKind::Other, "Some error")),
        ReadVarIntError::Io(IoError::new(std::io::ErrorKind::Other, "Some error"))
    )]
    #[case(
        make_read(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02]),
        ReadVarIntError::TooBig
    )]
    fn test_read_var_int_err(#[case] input: impl Read, #[case] expected: ReadVarIntError) {
        let res = read_var_int(input).unwrap_err();
        assert_eq!(res.to_string(), expected.to_string());
    }

    #[rstest]
    #[case(0, &[0x00])]
    #[case(1, &[0x01])]
    #[case(0xFF, &[0xFF, 0x01])]
    #[case(0x3FFF, &[0xFF, 0x7F])]
    #[case(i64::MAX as u64, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F])]
    #[case(u64::MAX, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01])]
    fn test_write_var_int(#[case] input: u64, #[case] expected: &[u8]) {
        let mut buf = Vec::new();
        write_var_int(&mut buf, input).unwrap();
        assert_eq!(buf, expected);
    }

    // Helper functions

    fn make_read(input: &[u8]) -> impl Read {
        std::io::Cursor::new(Vec::from(input))
    }

    fn make_read_err(err: IoError) -> impl Read {
        struct ErrRead {
            err: Option<IoError>,
        }

        impl Read for ErrRead {
            fn read(&mut self, _: &mut [u8]) -> Result<usize, IoError> {
                Err(self.err.take().expect("Should not be called again"))
            }
        }

        ErrRead { err: Some(err) }
    }
}
