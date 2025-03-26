use std::sync::{Arc, Mutex};

use syncman::Syncman;

use super::SyncmanAdapter;

pub struct EmbeddedSyncmanAdapter<S: Syncman> {
    syncman: Arc<Mutex<S>>,
}

impl<S: Syncman> Clone for EmbeddedSyncmanAdapter<S> {
    fn clone(&self) -> Self {
        Self {
            syncman: self.syncman.clone(),
        }
    }
}

impl<S: Syncman> EmbeddedSyncmanAdapter<S> {
    pub fn new(syncman: S) -> Self {
        Self {
            syncman: Arc::new(Mutex::new(syncman)),
        }
    }
}

#[async_trait::async_trait]
impl<S> SyncmanAdapter<S> for EmbeddedSyncmanAdapter<S>
where
    S: Syncman + Send + Sync,
    S::Handle: Send,
{
    async fn open_sync_handle(&self) -> <S as Syncman>::Handle {
        self.syncman.lock().unwrap().initiate_sync()
    }

    async fn apply_sync(&self, handle: &mut <S as Syncman>::Handle, msg: &syncman::SyncMessage) {
        self.syncman.lock().unwrap().apply_sync(handle, msg)
    }

    async fn dump(&self) -> std::collections::HashMap<String, String> {
        self.syncman.lock().unwrap().dump()
    }
}
