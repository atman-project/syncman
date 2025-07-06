pub mod automerge;

pub trait Syncman<Model> {
    type Handle: SyncHandle;

    fn update(&mut self, data: &Model);
    fn get(&self) -> Model;
    fn initiate_sync(&mut self) -> Self::Handle;
    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &[u8]);
    fn dump(&mut self) -> Vec<u8>;
}

pub trait SyncHandle {
    fn generate_message(&mut self) -> Option<Vec<u8>>;
}
