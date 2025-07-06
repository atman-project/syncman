use std::marker::PhantomData;

use automerge::{
    sync::{self, SyncDoc},
    AutoCommit,
};
use autosurgeon::{hydrate, reconcile, Hydrate, Reconcile};

use crate::{SyncHandle, Syncman};

#[derive(Debug)]
pub struct AutomergeSyncman<Model> {
    doc: AutoCommit,
    _marker: PhantomData<Model>,
}

impl<Model> AutomergeSyncman<Model> {
    pub fn new() -> Self {
        Self {
            doc: AutoCommit::new(),
            _marker: PhantomData,
        }
    }
}

impl<Model> Default for AutomergeSyncman<Model> {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: Handle AutomergeError in all methods
impl<Model> Syncman<Model> for AutomergeSyncman<Model>
where
    Model: Reconcile + Hydrate,
{
    type Handle = AutomergeSyncHandle;

    fn update(&mut self, data: &Model) {
        reconcile(&mut self.doc, data).unwrap();
    }

    fn get(&self) -> Model {
        hydrate(&self.doc).unwrap()
    }

    fn initiate_sync(&mut self) -> Self::Handle {
        AutomergeSyncHandle {
            doc: self.doc.fork(),
            sync_state: sync::State::new(),
        }
    }

    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &[u8]) {
        let msg = sync::Message::decode(msg).unwrap();
        handle
            .doc
            .sync()
            .receive_sync_message(&mut handle.sync_state, msg)
            .unwrap();
        self.doc.merge(&mut handle.doc).unwrap();
    }

    fn dump(&mut self) -> Vec<u8> {
        self.doc.save()
    }
}

pub struct AutomergeSyncHandle {
    doc: AutoCommit,
    sync_state: sync::State,
}

impl SyncHandle for AutomergeSyncHandle {
    fn generate_message(&mut self) -> Option<Vec<u8>> {
        self.doc
            .sync()
            .generate_sync_message(&mut self.sync_state)
            .map(|msg| msg.encode())
    }
}

#[cfg(test)]
mod tests {
    use automerge_test::{assert_doc, map};
    use autosurgeon::{Hydrate, Reconcile};

    use super::*;

    #[test]
    fn empty_docs() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo(u64);

        let mut peer1 = AutomergeSyncman::<Foo>::new();
        let mut peer2 = AutomergeSyncman::<Foo>::new();

        sync(&mut peer1, &mut peer2);

        assert!(peer1.doc.document().is_empty());
        assert!(peer2.doc.document().is_empty());
    }

    #[test]
    fn sync_from_empty() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            k0: String,
            k1: String,
        }

        let mut peer1 = AutomergeSyncman::<Foo>::new();
        let mut peer2 = AutomergeSyncman::<Foo>::new();

        let foo = Foo {
            k0: "v-0".to_string(),
            k1: "v-1".to_string(),
        };
        peer2.update(&foo);

        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get(), foo);
        assert_eq!(peer2.get(), foo);

        [&peer1.doc, &peer2.doc].iter().for_each(|&doc| {
            assert_doc!(
                doc,
                map! {
                    "k0" => {"v-0"},
                    "k1" => {"v-1"},
                }
            );
        });
    }

    #[test]
    fn mutual_sync() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            k0: Option<String>,
            k1: Option<String>,
            k2: Option<String>,
            k3: Option<String>,
            kc: Option<String>,
        }

        let mut peer1 = AutomergeSyncman::<Foo>::new();
        let mut peer2 = AutomergeSyncman::<Foo>::new();

        // Populate a doc of each peer: Two unique values for each and one overlapping value
        peer1.update(&Foo {
            k0: Some("v-0".to_string()),
            k1: Some("v-1".to_string()),
            k2: None,
            k3: None,
            kc: Some("v-c".to_string()),
        });
        peer2.update(&Foo {
            k0: Some("v-0".to_string()),
            k1: Some("v-11".to_string()),
            k2: Some("v-2".to_string()),
            k3: None,
            kc: Some("v-c".to_string()),
        });

        sync(&mut peer1, &mut peer2);

        // RULE: If both A and B set the key $x$ to some value then randomly choose one value
        let expected = Foo {
            k0: Some("v-0".to_string()),
            k1: Some("v-1".to_string()),
            k2: Some("v-2".to_string()),
            k3: None,
            kc: Some("v-c".to_string()),
        };
        assert_eq!(peer1.get(), expected);
        assert_eq!(peer2.get(), expected);
    }

    #[test]
    fn three_way_sync() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            k0: Option<String>,
            k1: Option<String>,
            k2: Option<String>,
            k3: Option<String>,
            k4: Option<String>,
            k5: Option<String>,
            kc: Option<String>,
        }
        let mut peer1 = AutomergeSyncman::<Foo>::new();
        let mut peer2 = AutomergeSyncman::<Foo>::new();
        let mut peer3 = AutomergeSyncman::<Foo>::new();

        // Populate a doc of each peer: Two unique values for each and one overlapping value
        peer1.update(&Foo {
            k0: Some("v-0".to_string()),
            k1: Some("v-1".to_string()),
            k2: None,
            k3: None,
            k4: None,
            k5: None,
            kc: Some("v-c".to_string()),
        });
        peer2.update(&Foo {
            k0: None,
            k1: None,
            k2: Some("v-2".to_string()),
            k3: Some("v-3".to_string()),
            k4: None,
            k5: None,
            kc: Some("v-c".to_string()),
        });
        peer3.update(&Foo {
            k0: None,
            k1: None,
            k2: None,
            k3: None,
            k4: Some("v-4".to_string()),
            k5: Some("v-5".to_string()),
            kc: Some("v-c".to_string()),
        });

        // peer1 is connected to peer2 and peer3.
        // peer2 is not connected to peer3.
        //
        // Round 1: peer1 syncs with peer2 and peer3
        three_way_sync_single_round(&mut peer1, &mut peer2, &mut peer3);
        // Round 2: peer1 syncs with peer2 and peer3 again
        // to propagate the changes received from the round 1.
        three_way_sync_single_round(&mut peer1, &mut peer2, &mut peer3);

        // RULE: If both A and B set the key $x$ to some value then randomly choose one value
        let expected = Foo {
            k0: Some("v-0".to_string()),
            k1: Some("v-1".to_string()),
            k2: None,
            k3: None,
            k4: None,
            k5: None,
            kc: Some("v-c".to_string()),
        };
        assert_eq!(peer1.get(), expected);
        assert_eq!(peer2.get(), expected);
        assert_eq!(peer3.get(), expected);
    }

    #[test]
    fn update_after_sync() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            k0: String,
        }

        let mut peer1 = AutomergeSyncman::<Foo>::new();
        let mut peer2 = AutomergeSyncman::<Foo>::new();

        let foo = Foo {
            k0: "v-0".to_string(),
        };
        peer2.update(&foo);

        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get(), foo);
        assert_eq!(peer2.get(), foo);

        let foo = Foo {
            k0: "v-1".to_string(),
        };
        peer1.update(&foo);
        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get(), foo);
        assert_eq!(peer2.get(), foo);

        let foo = Foo {
            k0: "v-2".to_string(),
        };
        peer2.update(&foo);
        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get(), foo);
        assert_eq!(peer2.get(), foo);
    }

    fn sync<Model>(peer1: &mut AutomergeSyncman<Model>, peer2: &mut AutomergeSyncman<Model>)
    where
        Model: Reconcile + Hydrate,
    {
        let mut handle1 = peer1.initiate_sync();
        let mut handle2 = peer2.initiate_sync();

        let mut done1 = false;
        let mut done2 = false;
        while !(done1 && done2) {
            if !done1 {
                match handle1.generate_message() {
                    Some(msg) => {
                        peer2.apply_sync(&mut handle2, &msg);
                    }
                    None => {
                        done1 = true;
                    }
                }
            }

            if !done2 {
                match handle2.generate_message() {
                    Some(msg) => {
                        peer1.apply_sync(&mut handle1, &msg);
                    }
                    None => {
                        done2 = true;
                    }
                }
            }
        }
    }

    fn three_way_sync_single_round<Model>(
        peer1: &mut AutomergeSyncman<Model>,
        peer2: &mut AutomergeSyncman<Model>,
        peer3: &mut AutomergeSyncman<Model>,
    ) where
        Model: Reconcile + Hydrate,
    {
        // peer1 is connected to peer2 and peer3.
        // peer2 is not connected to peer3.
        // peer1 uses a different sync handle for each connection.
        //        peer2
        //       /
        // peer1
        //       \
        //        peer3
        let mut handle1_2 = peer1.initiate_sync();
        let mut handle1_3 = peer1.initiate_sync();
        let mut handle2 = peer2.initiate_sync();
        let mut handle3 = peer3.initiate_sync();

        let mut done1_2 = false;
        let mut done1_3 = false;
        let mut done2 = false;
        let mut done3 = false;
        while !(done1_2 && done1_3 && done2 && done3) {
            if !done1_2 {
                match handle1_2.generate_message() {
                    Some(msg) => {
                        peer2.apply_sync(&mut handle2, &msg);
                    }
                    None => {
                        done1_2 = true;
                    }
                }
            }

            if !done1_3 {
                match handle1_3.generate_message() {
                    Some(msg) => {
                        peer3.apply_sync(&mut handle3, &msg);
                    }
                    None => {
                        done1_3 = true;
                    }
                }
            }

            if !done2 {
                match handle2.generate_message() {
                    Some(msg) => {
                        peer1.apply_sync(&mut handle1_2, &msg);
                    }
                    None => {
                        done2 = true;
                    }
                }
            }

            if !done3 {
                match handle3.generate_message() {
                    Some(msg) => {
                        peer1.apply_sync(&mut handle1_3, &msg);
                    }
                    None => {
                        done3 = true;
                    }
                }
            }
        }
    }
}
