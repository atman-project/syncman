pub mod automerge;

pub trait Syncman {
    type Handle: SyncHandle;

    fn initiate_sync(&self) -> Self::Handle;
    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &SyncMessage);
}

pub trait SyncHandle {
    fn generate_message(&mut self) -> SyncMessage;
}

pub enum SyncMessage {
    Sync(Vec<u8>),
    Done,
}
