use serde::{Deserialize, Serialize};

pub mod adapter;
pub mod iroh;

#[derive(Serialize, Deserialize, Debug)]
pub enum SyncMessage {
    Sync(Vec<u8>),
    Done,
}

impl From<Option<Vec<u8>>> for SyncMessage {
    fn from(value: Option<Vec<u8>>) -> Self {
        match value {
            Some(v) => SyncMessage::Sync(v),
            None => SyncMessage::Done,
        }
    }
}
