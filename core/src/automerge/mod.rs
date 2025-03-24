use automerge::{
    sync::{self, SyncDoc},
    Automerge,
};

use crate::{SyncHandle, SyncMessage, Syncman};

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

    fn apply_sync(&mut self, handle: &mut Self::Handle, msg: &SyncMessage) {
        if let SyncMessage::Sync(msg) = msg {
            let msg = sync::Message::decode(msg).unwrap();
            handle
                .doc
                .receive_sync_message(&mut handle.sync_state, msg)
                .unwrap();
            self.doc.merge(&mut handle.doc).unwrap();
        }
    }
}

pub struct AutomergeSyncHandle {
    doc: Automerge,
    sync_state: sync::State,
}

impl SyncHandle for AutomergeSyncHandle {
    fn generate_message(&mut self) -> SyncMessage {
        match self.doc.generate_sync_message(&mut self.sync_state) {
            Some(msg) => SyncMessage::Sync(msg.encode()),
            None => SyncMessage::Done,
        }
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
        let mut handle1 = peer1.initiate_sync();
        let mut handle2 = peer2.initiate_sync();

        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer2.apply_sync(&mut handle2, &msg);
        let msg = handle2.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer1.apply_sync(&mut handle1, &msg);
        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Done));
        peer2.apply_sync(&mut handle2, &msg);
        let msg = handle2.generate_message();
        assert!(matches!(msg, SyncMessage::Done));

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

        let mut handle1 = peer1.initiate_sync();
        let mut handle2 = peer2.initiate_sync();

        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer2.apply_sync(&mut handle2, &msg);
        let msg = handle2.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer1.apply_sync(&mut handle1, &msg);
        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer2.apply_sync(&mut handle2, &msg);
        let msg = handle2.generate_message();
        assert!(matches!(msg, SyncMessage::Done));
        peer1.apply_sync(&mut handle1, &msg);
        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Done));

        assert_eq!(doc_to_hashmap(&peer1.doc), data);
        assert_eq!(doc_to_hashmap(&peer2.doc), data);
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

        let mut handle1 = peer1.initiate_sync();
        let mut handle2 = peer2.initiate_sync();

        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer2.apply_sync(&mut handle2, &msg);
        let msg = handle2.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer1.apply_sync(&mut handle1, &msg);
        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer2.apply_sync(&mut handle2, &msg);
        let msg = handle2.generate_message();
        assert!(matches!(msg, SyncMessage::Sync(_)));
        peer1.apply_sync(&mut handle1, &msg);
        let msg = handle1.generate_message();
        assert!(matches!(msg, SyncMessage::Done));
        peer2.apply_sync(&mut handle2, &msg);
        let msg = handle2.generate_message();
        assert!(matches!(msg, SyncMessage::Done));

        assert_eq!(doc_to_hashmap(&peer1.doc), data);
        assert_eq!(doc_to_hashmap(&peer2.doc), data);
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
        doc_to_hashmap(doc)
    }

    fn doc_to_hashmap(doc: &Automerge) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for key in doc.keys(automerge::ROOT).collect::<Vec<_>>() {
            let (value, _) = doc.get(automerge::ROOT, &key).unwrap().unwrap();
            map.insert(key, value.to_string());
        }
        map
    }
}
