use std::collections::HashMap;

use automerge::{
    sync::{self, SyncDoc},
    AutoCommit, ReadDoc as _,
};
use autosurgeon::{
    hydrate, hydrate_prop, reconcile, reconcile_insert, reconcile_prop, Hydrate, Reconcile,
};

use crate::{SyncHandle, Syncman};

#[derive(Debug)]
pub struct AutomergeSyncman {
    doc: AutoCommit,
}

impl AutomergeSyncman {
    pub fn new() -> Self {
        Self {
            doc: AutoCommit::new(),
        }
    }
}

impl Default for AutomergeSyncman {
    fn default() -> Self {
        Self::new()
    }
}

impl From<AutoCommit> for AutomergeSyncman {
    fn from(doc: AutoCommit) -> Self {
        Self { doc }
    }
}

// TODO: Handle AutomergeError in all methods
impl Syncman for AutomergeSyncman {
    type Handle = AutomergeSyncHandle;
    type ObjectId = automerge::ObjId;
    type Property = automerge::Prop;

    fn update<Model>(&mut self, data: &Model)
    where
        Model: Reconcile,
    {
        reconcile(&mut self.doc, data).unwrap();
    }

    fn root(&self) -> Self::ObjectId {
        automerge::ROOT
    }

    fn update_prop<Obj, Prop, Model>(&mut self, obj: Obj, prop: Prop, value: &Model)
    where
        Obj: AsRef<Self::ObjectId>,
        Prop: Into<Self::Property>,
        Model: Reconcile,
    {
        reconcile_prop(&mut self.doc, obj, prop.into(), value).unwrap();
    }

    fn insert<Model>(&mut self, obj: Self::ObjectId, index: usize, value: &Model)
    where
        Model: Reconcile,
    {
        reconcile_insert(&mut self.doc, obj, index, value).unwrap();
    }

    fn get<Model>(&self) -> Model
    where
        Model: Hydrate,
    {
        hydrate(&self.doc).unwrap()
    }

    fn get_prop<Obj, Prop, Model>(&self, obj: Obj, prop: Prop) -> (Model, Self::ObjectId)
    where
        Obj: AsRef<Self::ObjectId>,
        Prop: Into<Self::Property>,
        Model: Hydrate,
    {
        let prop = prop.into();
        let (_, obj_id) = self
            .doc
            .get(obj.as_ref(), prop.clone())
            .unwrap()
            .expect("Property not found");
        (hydrate_prop(&self.doc, obj.as_ref(), prop).unwrap(), obj_id)
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

    fn dump(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for key in self.doc.keys(automerge::ROOT) {
            let (value, _) = self.doc.get(automerge::ROOT, &key).unwrap().unwrap();
            map.insert(key, value.to_string());
        }
        map
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
    fn update_and_get() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            b0: Bar,
            b1: Bar,
        }

        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Bar {
            k0: String,
            k1: String,
        }

        let mut syncman = AutomergeSyncman::new();
        let mut foo = Foo {
            b0: Bar {
                k0: "v-0".to_string(),
                k1: "v-1".to_string(),
            },
            b1: Bar {
                k0: "v-2".to_string(),
                k1: "v-3".to_string(),
            },
        };
        syncman.update(&foo);
        assert_eq!(syncman.get::<Foo>(), foo);

        foo.b1 = Bar {
            k0: "v-4".to_string(),
            k1: "v-5".to_string(),
        };
        syncman.update_prop(syncman.root(), "b1", &foo.b1);
        assert_eq!(syncman.get::<Foo>(), foo);
    }

    #[test]
    fn update_prop() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            k0: String,
            k1: String,
        }

        let mut syncman = AutomergeSyncman::new();
        let foo1 = Foo {
            k0: "v-0".to_string(),
            k1: "v-1".to_string(),
        };
        syncman.update_prop(syncman.root(), "f1", &foo1);
        let mut foo2 = Foo {
            k0: "v-2".to_string(),
            k1: "v-3".to_string(),
        };
        syncman.update_prop(syncman.root(), "f2", &foo2);

        assert_eq!(
            syncman.get::<HashMap<String, Foo>>(),
            HashMap::from([
                ("f1".to_string(), foo1.clone()),
                ("f2".to_string(), foo2.clone())
            ])
        );

        foo2.k0 = "v-4".to_string();
        syncman.update_prop(syncman.root(), "f2", &foo2);
        assert_eq!(
            syncman.get::<HashMap<String, Foo>>(),
            HashMap::from([
                ("f1".to_string(), foo1.clone()),
                ("f2".to_string(), foo2.clone())
            ])
        );
    }

    #[test]
    fn insert() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            numbers: Vec<u64>,
        }

        let mut syncman = AutomergeSyncman::new();
        let foo = Foo {
            numbers: vec![0, 1, 2],
        };
        syncman.update(&foo);

        let (numbers, obj) = syncman.get_prop::<_, _, Vec<u64>>(&syncman.root(), "numbers");
        assert_eq!(numbers, foo.numbers);

        syncman.insert(obj.clone(), 0, &10u64);
        syncman.insert(obj, 2, &20u64);

        assert_eq!(
            syncman.get::<Foo>(),
            Foo {
                numbers: vec![10, 0, 20, 1, 2],
            }
        );
    }

    #[test]
    fn empty_docs() {
        let mut peer1 = AutomergeSyncman::new();
        let mut peer2 = AutomergeSyncman::new();

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

        let mut peer1 = AutomergeSyncman::new();
        let mut peer2 = AutomergeSyncman::new();

        let foo = Foo {
            k0: "v-0".to_string(),
            k1: "v-1".to_string(),
        };
        peer2.update(&foo);

        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get::<Foo>(), foo);
        assert_eq!(peer2.get::<Foo>(), foo);

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

        let mut peer1 = AutomergeSyncman::new();
        let mut peer2 = AutomergeSyncman::new();

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
        //
        // So, here we just check if all peers have the same view,
        // instead of comparing the view with the expected `Foo`.
        assert_eq!(peer1.get::<Foo>(), peer2.get::<Foo>());
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
        let mut peer1 = AutomergeSyncman::new();
        let mut peer2 = AutomergeSyncman::new();
        let mut peer3 = AutomergeSyncman::new();

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
        //
        // So, here we just check if all peers have the same view,
        // instead of comparing the view with the expected `Foo`.
        assert_eq!(peer1.get::<Foo>(), peer2.get::<Foo>());
        assert_eq!(peer1.get::<Foo>(), peer3.get::<Foo>());
    }

    #[test]
    fn update_after_sync() {
        #[derive(Reconcile, Hydrate, Clone, Debug, Eq, PartialEq)]
        struct Foo {
            k0: String,
        }

        let mut peer1 = AutomergeSyncman::new();
        let mut peer2 = AutomergeSyncman::new();

        let foo = Foo {
            k0: "v-0".to_string(),
        };
        peer2.update(&foo);

        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get::<Foo>(), foo);
        assert_eq!(peer2.get::<Foo>(), foo);

        let foo = Foo {
            k0: "v-1".to_string(),
        };
        peer1.update(&foo);
        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get::<Foo>(), foo);
        assert_eq!(peer2.get::<Foo>(), foo);

        let foo = Foo {
            k0: "v-2".to_string(),
        };
        peer2.update(&foo);
        sync(&mut peer1, &mut peer2);
        assert_eq!(peer1.get::<Foo>(), foo);
        assert_eq!(peer2.get::<Foo>(), foo);
    }

    fn sync(peer1: &mut AutomergeSyncman, peer2: &mut AutomergeSyncman) {
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

    fn three_way_sync_single_round(
        peer1: &mut AutomergeSyncman,
        peer2: &mut AutomergeSyncman,
        peer3: &mut AutomergeSyncman,
    ) {
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
