use std::collections::HashMap;

pub mod automerge;

pub trait Syncman {
    type Handle: SyncHandle;

    fn initiate_sync(&self) -> Self::Handle;
    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &[u8]);
    // TODO: use generic type
    fn dump(&self) -> HashMap<String, String>;
}

pub trait SyncHandle {
    fn generate_message(&mut self) -> Option<Vec<u8>>;
}
