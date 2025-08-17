use std::collections::HashMap;

use autosurgeon::{Hydrate, Reconcile};

pub mod automerge;

pub trait Syncman {
    type Handle: SyncHandle;
    type ObjectId;
    type Property;

    fn update<Model>(&mut self, data: &Model)
    where
        Model: Reconcile;

    fn root(&self) -> Self::ObjectId;

    fn update_prop<Obj, Prop, Model>(&mut self, obj: Obj, prop: Prop, value: &Model)
    where
        Obj: AsRef<Self::ObjectId>,
        Prop: Into<Self::Property>,
        Model: Reconcile;

    fn insert<Model>(&mut self, obj: Self::ObjectId, index: usize, value: &Model)
    where
        Model: Reconcile;

    fn get<Model>(&self) -> Model
    where
        Model: Hydrate;

    fn get_prop<Obj, Prop, Model>(&self, obj: Obj, prop: Prop) -> (Model, Self::ObjectId)
    where
        Obj: AsRef<Self::ObjectId>,
        Prop: Into<Self::Property>,
        Model: Hydrate;

    fn initiate_sync(&mut self) -> Self::Handle;

    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &[u8]);

    // TODO: use generic type
    fn dump(&self) -> HashMap<String, String>;
}

pub trait SyncHandle {
    fn generate_message(&mut self) -> Option<Vec<u8>>;
}
