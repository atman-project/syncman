use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod automerge;

pub trait Syncman {
    type Handle: SyncHandle;

    fn initiate_sync(&self) -> Self::Handle;
    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &SyncMessage);
    // TODO: use generic type
    fn dump(&self) -> HashMap<String, String>;
}

pub trait SyncHandle {
    fn generate_message(&mut self) -> SyncMessage;
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SyncMessage {
    Sync(Vec<u8>),
    Done,
}
