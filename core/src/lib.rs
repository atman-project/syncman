pub mod automerge;

pub trait Syncman {
    fn initiate_sync();
    fn apply_sync();
}
