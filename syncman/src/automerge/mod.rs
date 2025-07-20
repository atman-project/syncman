use std::collections::HashMap;

use automerge::{
    sync::{self, SyncDoc},
    Automerge, ReadDoc,
};

use crate::{SyncHandle, Syncman};

#[derive(Debug)]
pub struct AutomergeSyncman {
    doc: Automerge,
}

impl AutomergeSyncman {
    pub fn new(doc: Automerge) -> Self {
        Self { doc }
    }
}

impl Syncman for AutomergeSyncman {
    type Handle = AutomergeSyncHandle;

    fn initiate_sync(&self) -> Self::Handle {
        AutomergeSyncHandle {
            doc: self.doc.fork(),
            sync_state: sync::State::new(),
        }
    }

    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &[u8]) {
        let msg = sync::Message::decode(msg).unwrap();
        handle
            .doc
            .receive_sync_message(&mut handle.sync_state, msg)
            .unwrap();
        self.doc.merge(&mut handle.doc).unwrap();
    }

    fn dump(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for key in self.doc.keys(automerge::ROOT).collect::<Vec<_>>() {
            let (value, _) = self.doc.get(automerge::ROOT, &key).unwrap().unwrap();
            map.insert(key, value.to_string());
        }
        map
    }
}

pub struct AutomergeSyncHandle {
    doc: Automerge,
    sync_state: sync::State,
}

impl SyncHandle for AutomergeSyncHandle {
    fn generate_message(&mut self) -> Option<Vec<u8>> {
        self.doc
            .generate_sync_message(&mut self.sync_state)
            .map(|msg| msg.encode())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use automerge::{transaction::Transactable, Automerge, ReadDoc};

    use super::*;

    #[test]
    fn empty_docs() {
        let mut peer1 = AutomergeSyncman::new(Automerge::new());
        let mut peer2 = AutomergeSyncman::new(Automerge::new());

        sync(&mut peer1, &mut peer2);

        assert!(peer1.doc.is_empty());
        assert!(peer2.doc.is_empty());
    }

    #[test]
    fn sync_from_empty() {
        let mut peer1 = AutomergeSyncman::new(Automerge::new());
        let mut peer2 = AutomergeSyncman::new(Automerge::new());
        let data = populate_doc(
            &mut peer2.doc,
            vec![
                ("k-0".to_string(), "v-0".to_string()),
                ("k-1".to_string(), "v-1".to_string()),
            ],
        );

        sync(&mut peer1, &mut peer2);

        assert_eq!(peer1.dump(), data);
        assert_eq!(peer2.dump(), data);
    }

    #[test]
    fn mutual_sync() {
        let mut peer1 = AutomergeSyncman::new(Automerge::new());
        let mut peer2 = AutomergeSyncman::new(Automerge::new());

        // Populate a doc of each peer: Two unique values for each and one overlapping value
        let mut data = populate_doc(
            &mut peer1.doc,
            vec![
                ("k-0".to_string(), "v-0".to_string()),
                ("k-2".to_string(), "v-2".to_string()),
                ("k-c".to_string(), "v-c".to_string()),
            ],
        );
        data.extend(populate_doc(
            &mut peer2.doc,
            vec![
                ("k-1".to_string(), "v-1".to_string()),
                ("k-3".to_string(), "v-3".to_string()),
                ("k-c".to_string(), "v-c".to_string()),
            ],
        ));
        assert_eq!(data.len(), 5);

        sync(&mut peer1, &mut peer2);

        assert_eq!(peer1.dump(), data);
        assert_eq!(peer2.dump(), data);
    }

    #[test]
    fn three_way_sync() {
        let mut peer1 = AutomergeSyncman::new(Automerge::new());
        let mut peer2 = AutomergeSyncman::new(Automerge::new());
        let mut peer3 = AutomergeSyncman::new(Automerge::new());

        // Populate a doc of each peer: Two unique values for each and one overlapping value
        let data1 = populate_doc(
            &mut peer1.doc,
            vec![
                ("k-0".to_string(), "v-0".to_string()),
                ("k-1".to_string(), "v-1".to_string()),
                ("k-c".to_string(), "v-c".to_string()),
            ],
        );
        let data2 = populate_doc(
            &mut peer2.doc,
            vec![
                ("k-2".to_string(), "v-2".to_string()),
                ("k-3".to_string(), "v-3".to_string()),
                ("k-c".to_string(), "v-c".to_string()),
            ],
        );
        let data3 = populate_doc(
            &mut peer3.doc,
            vec![
                ("k-4".to_string(), "v-4".to_string()),
                ("k-5".to_string(), "v-5".to_string()),
                ("k-c".to_string(), "v-c".to_string()),
            ],
        );
        let all_data = merge_hashmaps([&data1, &data2, &data3]);
        assert_eq!(all_data.len(), 7);

        // peer1 is connected to peer2 and peer3.
        // peer2 is not connected to peer3.
        three_way_sync_single_round(&mut peer1, &mut peer2, &mut peer3);
        // Round 1: peer1 syncs with peer2 and peer3
        assert_eq!(peer1.dump(), all_data);
        assert_eq!(peer2.dump(), merge_hashmaps([&data1, &data2]));
        assert_eq!(peer3.dump(), merge_hashmaps([&data1, &data3]));
        // Round 2: peer1 syncs with peer2 and peer3 again
        // to propagate the changes received from the round 1.
        three_way_sync_single_round(&mut peer1, &mut peer2, &mut peer3);
        assert_eq!(peer1.dump(), all_data);
        assert_eq!(peer2.dump(), all_data);
        assert_eq!(peer3.dump(), all_data);
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

    fn populate_doc(
        doc: &mut Automerge,
        keyvalues: Vec<(String, String)>,
    ) -> HashMap<String, String> {
        let mut fork = doc.fork();
        let mut tx = doc.transaction();
        for (key, value) in keyvalues {
            tx.put(automerge::ROOT, key, value).unwrap();
        }
        tx.commit();
        doc.merge(&mut fork).unwrap();

        // Return the key-value pairs as a hashmap
        let mut map = HashMap::new();
        for key in doc.keys(automerge::ROOT).collect::<Vec<_>>() {
            let (value, _) = doc.get(automerge::ROOT, &key).unwrap().unwrap();
            map.insert(key, value.to_string());
        }
        map
    }

    fn merge_hashmaps<'a, I>(maps: I) -> HashMap<String, String>
    where
        I: IntoIterator<Item = &'a HashMap<String, String>>,
    {
        maps.into_iter().fold(HashMap::new(), |mut acc, map| {
            acc.extend(map.clone());
            acc
        })
    }
}
