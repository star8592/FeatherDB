#![forbid(unsafe_code)]

use feather_transport_api::{MessageClass, MessageEnvelope};

pub const MESSAGE_MAGIC: [u8; 4] = *b"FMSG";
pub const MESSAGE_VERSION: u8 = 1;
pub const MAX_MESSAGE_PAYLOAD: usize = 16 * 1024 * 1024;
pub const MESSAGE_HEADER_BYTES: usize = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireError {
    TooShort,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidClass(u8),
    InvalidFlags(u16),
    PayloadTooLarge(usize),
    LengthMismatch { declared: usize, actual: usize },
}

pub fn encode_message(message: &MessageEnvelope) -> Result<Vec<u8>, WireError> {
    if message.payload.len() > MAX_MESSAGE_PAYLOAD {
        return Err(WireError::PayloadTooLarge(message.payload.len()));
    }
    let payload_len = u32::try_from(message.payload.len())
        .map_err(|_| WireError::PayloadTooLarge(message.payload.len()))?;
    let mut frame = Vec::with_capacity(MESSAGE_HEADER_BYTES + message.payload.len());
    frame.extend_from_slice(&MESSAGE_MAGIC);
    frame.push(MESSAGE_VERSION);
    frame.push(encode_class(message.class));
    frame.extend_from_slice(&0_u16.to_le_bytes());
    frame.extend_from_slice(&message.message_id.to_le_bytes());
    frame.extend_from_slice(&message.from.to_le_bytes());
    frame.extend_from_slice(&message.to.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&message.payload);
    Ok(frame)
}

pub fn decode_message(frame: &[u8]) -> Result<MessageEnvelope, WireError> {
    if frame.len() < MESSAGE_HEADER_BYTES {
        return Err(WireError::TooShort);
    }
    if frame[..4] != MESSAGE_MAGIC {
        return Err(WireError::InvalidMagic);
    }
    let version = frame[4];
    if version != MESSAGE_VERSION {
        return Err(WireError::UnsupportedVersion(version));
    }
    let class = decode_class(frame[5])?;
    let flags = u16::from_le_bytes(frame[6..8].try_into().expect("fixed header slice"));
    if flags != 0 {
        return Err(WireError::InvalidFlags(flags));
    }
    let message_id = u64::from_le_bytes(frame[8..16].try_into().expect("fixed header slice"));
    let from = u64::from_le_bytes(frame[16..24].try_into().expect("fixed header slice"));
    let to = u64::from_le_bytes(frame[24..32].try_into().expect("fixed header slice"));
    let declared =
        u32::from_le_bytes(frame[32..36].try_into().expect("fixed header slice")) as usize;
    if declared > MAX_MESSAGE_PAYLOAD {
        return Err(WireError::PayloadTooLarge(declared));
    }
    let actual = frame.len() - MESSAGE_HEADER_BYTES;
    if declared != actual {
        return Err(WireError::LengthMismatch { declared, actual });
    }
    Ok(MessageEnvelope {
        message_id,
        from,
        to,
        class,
        payload: frame[MESSAGE_HEADER_BYTES..].to_vec(),
    })
}

fn encode_class(class: MessageClass) -> u8 {
    match class {
        MessageClass::Membership => 0,
        MessageClass::Gossip => 1,
        MessageClass::Control => 2,
        MessageClass::Data => 3,
        MessageClass::Repair => 4,
        MessageClass::Client => 5,
    }
}

fn decode_class(value: u8) -> Result<MessageClass, WireError> {
    match value {
        0 => Ok(MessageClass::Membership),
        1 => Ok(MessageClass::Gossip),
        2 => Ok(MessageClass::Control),
        3 => Ok(MessageClass::Data),
        4 => Ok(MessageClass::Repair),
        5 => Ok(MessageClass::Client),
        other => Err(WireError::InvalidClass(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(class: MessageClass, payload: Vec<u8>) -> MessageEnvelope {
        MessageEnvelope {
            message_id: 8592,
            from: 7,
            to: 11,
            class,
            payload,
        }
    }

    #[test]
    fn all_message_classes_round_trip_exactly() {
        for class in [
            MessageClass::Membership,
            MessageClass::Gossip,
            MessageClass::Control,
            MessageClass::Data,
            MessageClass::Repair,
            MessageClass::Client,
        ] {
            let original = message(class, vec![0, 1, 2, 0xff]);
            assert_eq!(
                decode_message(&encode_message(&original).unwrap()),
                Ok(original)
            );
        }
    }

    #[test]
    fn encoding_is_stable_and_little_endian() {
        let frame = encode_message(&message(MessageClass::Control, b"abc".to_vec())).unwrap();
        assert_eq!(&frame[..4], b"FMSG");
        assert_eq!(frame[4], 1);
        assert_eq!(frame[5], 2);
        assert_eq!(&frame[8..16], &8592_u64.to_le_bytes());
        assert_eq!(&frame[16..24], &7_u64.to_le_bytes());
        assert_eq!(&frame[24..32], &11_u64.to_le_bytes());
        assert_eq!(&frame[32..36], &3_u32.to_le_bytes());
        assert_eq!(&frame[36..], b"abc");
    }

    #[test]
    fn malformed_or_oversized_frames_are_rejected() {
        assert_eq!(decode_message(b"short"), Err(WireError::TooShort));

        let mut invalid_magic = encode_message(&message(MessageClass::Data, vec![])).unwrap();
        invalid_magic[0] ^= 0xff;
        assert_eq!(decode_message(&invalid_magic), Err(WireError::InvalidMagic));

        let mut future = encode_message(&message(MessageClass::Data, vec![])).unwrap();
        future[4] = 2;
        assert_eq!(
            decode_message(&future),
            Err(WireError::UnsupportedVersion(2))
        );

        let mut bad_len = encode_message(&message(MessageClass::Data, vec![1, 2, 3])).unwrap();
        bad_len[32..36].copy_from_slice(&4_u32.to_le_bytes());
        assert_eq!(
            decode_message(&bad_len),
            Err(WireError::LengthMismatch {
                declared: 4,
                actual: 3,
            })
        );

        let huge = message(MessageClass::Data, vec![0; MAX_MESSAGE_PAYLOAD + 1]);
        assert_eq!(
            encode_message(&huge),
            Err(WireError::PayloadTooLarge(MAX_MESSAGE_PAYLOAD + 1))
        );
    }
}
