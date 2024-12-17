use bytes::{BufMut, BytesMut};

pub trait BoundedReader {
    fn read_u8(&self, start: usize) -> Result<u8, String>;
    fn read_u32(&self, start: usize) -> Result<u32, &'static str>;
    /// write_u8 adds given value to the buffer and moves the cursor to 1 byte ahead.
    fn write_u8(&mut self, value: u8);
    /// write_u32 adds given value to the buffer and moves the cursor to 4 bytes ahead.
    fn write_u32(&mut self, value: u32);
}

impl BoundedReader for BytesMut {
    fn write_u32(&mut self, value: u32) {
        self.put_u32_le(value);
    }

    fn write_u8(&mut self, value: u8) {
        self.put_u8(value);
    }

    fn read_u8(&self, start: usize) -> Result<u8, String> {
        let end = start + 1;

        // Check if we have enough bytes
        if self.len() < end {
            return Err("Not enough bytes in buffer".to_string());
        }

        let bytes = &self[start..end];
        match bytes.try_into() {
            Ok(array) => Ok(u8::from_le_bytes(array)),
            Err(err) => Err(format!(
                "Failed to convert bytes to array, err: {:?}",
                err.to_string()
            )),
        }
    }

    fn read_u32(&self, start: usize) -> Result<u32, &'static str> {
        let end = start + 4;

        // Check if we have enough bytes
        if self.len() < end {
            return Err("Not enough bytes in buffer");
        }

        let bytes = &self[start..end];
        match bytes.try_into() {
            Ok(array) => Ok(u32::from_le_bytes(array)),
            Err(_) => Err("Failed to convert bytes to array"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_u8() {
        let mut buffer = BytesMut::new();

        buffer.write_u8(42);
        assert_eq!(buffer.read_u8(0).unwrap(), 42);

        buffer.write_u8(255);
        buffer.write_u8(0);
        assert_eq!(buffer.read_u8(1).unwrap(), 255);
        assert_eq!(buffer.read_u8(2).unwrap(), 0);
    }

    #[test]
    fn test_write_and_read_u32() {
        let mut buffer = BytesMut::new();

        buffer.write_u32(0x12345678);
        assert_eq!(buffer.read_u32(0).unwrap(), 0x12345678);

        buffer.write_u32(0xFFFFFFFF);
        buffer.write_u32(0);
        assert_eq!(buffer.read_u32(4).unwrap(), 0xFFFFFFFF);
        assert_eq!(buffer.read_u32(8).unwrap(), 0);
    }

    #[test]
    fn test_read_u8_out_of_bounds() {
        let buffer = BytesMut::new();

        assert!(buffer.read_u8(0).is_err());

        let mut buffer = BytesMut::new();
        buffer.write_u8(42);
        assert!(buffer.read_u8(1).is_err());
        assert!(buffer.read_u8(100).is_err());
    }

    #[test]
    fn test_read_u32_out_of_bounds() {
        let buffer = BytesMut::new();

        assert!(buffer.read_u32(0).is_err());

        let mut buffer = BytesMut::new();
        buffer.write_u8(42);
        assert!(buffer.read_u32(0).is_err());

        let mut buffer = BytesMut::new();
        buffer.write_u32(0x12345678);
        assert!(buffer.read_u32(1).is_err());
        assert!(buffer.read_u32(100).is_err());
    }

    #[test]
    fn test_mixed_operations() {
        let mut buffer = BytesMut::new();

        buffer.write_u8(0xFF);
        buffer.write_u32(0x12345678);
        buffer.write_u8(0x42);

        assert_eq!(buffer.read_u8(0).unwrap(), 0xFF);
        assert_eq!(buffer.read_u32(1).unwrap(), 0x12345678);
        assert_eq!(buffer.read_u8(5).unwrap(), 0x42);
    }

    #[test]
    fn test_boundary_values() {
        let mut buffer = BytesMut::new();

        buffer.write_u8(u8::MIN);
        buffer.write_u8(u8::MAX);
        assert_eq!(buffer.read_u8(0).unwrap(), u8::MIN);
        assert_eq!(buffer.read_u8(1).unwrap(), u8::MAX);

        buffer.write_u32(u32::MIN);
        buffer.write_u32(u32::MAX);
        assert_eq!(buffer.read_u32(2).unwrap(), u32::MIN);
        assert_eq!(buffer.read_u32(6).unwrap(), u32::MAX);
    }
}
