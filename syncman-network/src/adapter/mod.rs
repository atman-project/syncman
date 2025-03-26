pub mod embedded;

use std::collections::HashMap;

use syncman::Syncman;

#[async_trait::async_trait]
pub trait SyncmanAdapter<S: Syncman>: Clone {
    async fn open_sync_handle(&self) -> <S as Syncman>::Handle;
    async fn apply_sync(&self, handle: &mut <S as Syncman>::Handle, msg: &[u8]);
    // TODO: use generic type
    async fn dump(&self) -> HashMap<String, String>;
}
