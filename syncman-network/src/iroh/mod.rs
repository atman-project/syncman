use std::{fmt::Formatter, future::Future, pin::Pin};

use anyhow::Ok;
use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    protocol::ProtocolHandler,
};
use syncman::{SyncHandle, Syncman};
use tokio::sync::mpsc;

use crate::{adapter::SyncmanAdapter, SyncMessage};

pub struct IrohSyncmanProtocol<S, A>
where
    S: Syncman,
    A: SyncmanAdapter<S>,
{
    adapter: A,
    sync_finished: mpsc::Sender<()>,
    _syncman: std::marker::PhantomData<S>,
}

impl<S, A> Clone for IrohSyncmanProtocol<S, A>
where
    S: Syncman,
    A: SyncmanAdapter<S>,
{
    fn clone(&self) -> Self {
        Self {
            adapter: self.adapter.clone(),
            sync_finished: self.sync_finished.clone(),
            _syncman: std::marker::PhantomData,
        }
    }
}

impl<S, A> std::fmt::Debug for IrohSyncmanProtocol<S, A>
where
    S: Syncman,
    A: SyncmanAdapter<S>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrohSyncmanProtocol").finish()
    }
}

impl<S, A> ProtocolHandler for IrohSyncmanProtocol<S, A>
where
    S: Syncman + Send + Sync + 'static,
    S::Handle: Send + Sync + 'static,
    A: SyncmanAdapter<S> + Clone + Send + Sync + 'static,
{
    fn accept(
        &self,
        conn: Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'static>> {
        let protocol = self.clone();
        Box::pin(async move {
            protocol.respond_sync(conn).await?;
            protocol.sync_finished.send(()).await?;
            Ok(())
        })
    }
}

pub const IROH_SYNCMAN_ALPN: &[u8] = b"atman/syncman/1";

impl<S, A> IrohSyncmanProtocol<S, A>
where
    S: Syncman,
    A: SyncmanAdapter<S>,
{
    pub fn new(adapter: A, sync_finished: mpsc::Sender<()>) -> Self {
        Self {
            adapter,
            sync_finished,
            _syncman: std::marker::PhantomData,
        }
    }

    pub async fn initiate_sync(&self, conn: Connection) -> anyhow::Result<()> {
        let (mut conn_send, mut conn_recv) = conn.open_bi().await?;
        let mut handle = self.adapter.open_sync_handle().await;
        let mut is_local_done = false;
        let mut is_remote_done = false;
        while !is_local_done || !is_remote_done {
            if !is_local_done {
                let msg: SyncMessage = handle.generate_message().into();
                Self::send_msg(&msg, &mut conn_send).await?;
                is_local_done = matches!(msg, SyncMessage::Done);
            }

            if !is_remote_done {
                match Self::recv_msg(&mut conn_recv).await? {
                    SyncMessage::Sync(msg) => {
                        self.adapter.apply_sync(&mut handle, &msg).await;
                    }
                    SyncMessage::Done => {
                        is_remote_done = true;
                    }
                };
            }
        }
        conn_send.finish()?;
        let _ = conn_send.stopped().await;
        Ok(())
    }

    async fn respond_sync(&self, conn: Connection) -> anyhow::Result<()> {
        let (mut conn_send, mut conn_recv) = conn.accept_bi().await?;

        let mut handle = self.adapter.open_sync_handle().await;
        let mut is_local_done = false;
        let mut is_remote_done = false;
        while !is_local_done || !is_remote_done {
            if !is_remote_done {
                match Self::recv_msg(&mut conn_recv).await? {
                    SyncMessage::Sync(msg) => {
                        self.adapter.apply_sync(&mut handle, &msg).await;
                    }
                    SyncMessage::Done => {
                        is_remote_done = true;
                    }
                };
            }

            if !is_local_done {
                let msg: SyncMessage = handle.generate_message().into();
                Self::send_msg(&msg, &mut conn_send).await?;
                is_local_done = matches!(msg, SyncMessage::Done);
            }
        }
        conn_send.finish()?;
        let _ = conn_send.stopped().await;
        Ok(())
    }

    async fn send_msg(msg: &SyncMessage, send: &mut SendStream) -> anyhow::Result<()> {
        let encoded = bincode::serialize(msg)?;
        send.write_all(&(encoded.len() as u64).to_le_bytes())
            .await?;
        send.write_all(&encoded).await?;
        Ok(())
    }

    async fn recv_msg(recv: &mut RecvStream) -> anyhow::Result<SyncMessage> {
        let mut len = [0u8; 8];
        recv.read_exact(&mut len).await?;
        let len = u64::from_le_bytes(len);

        let mut buffer = vec![0u8; len as usize];
        recv.read_exact(&mut buffer).await?;
        Ok(bincode::deserialize(&buffer)?)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{Ipv4Addr, SocketAddrV4},
    };

    use automerge::{transaction::Transactable, Automerge, ReadDoc};
    use iroh::{protocol::Router, Endpoint};
    use syncman::automerge::AutomergeSyncman;

    use crate::adapter::embedded::EmbeddedSyncmanAdapter;

    use super::*;

    #[tokio::test]
    async fn sync_empty_docs() {
        let (tx_sync_finished1, mut rx_sync_finished1) = tokio::sync::mpsc::channel(1);
        let protocol1 = create_protocol(Automerge::new(), tx_sync_finished1);
        let iroh1 = spawn_router(protocol1.clone()).await;
        let addr1 = iroh1.endpoint().node_addr().await.unwrap();
        println!("sync_empty_docs: addr1: {addr1:?}");

        let (tx_sync_finished2, _) = tokio::sync::mpsc::channel(1);
        let protocol2 = create_protocol(Automerge::new(), tx_sync_finished2);
        let iroh2 = spawn_router(protocol2.clone()).await;
        let conn = iroh2
            .endpoint()
            .connect(addr1, IROH_SYNCMAN_ALPN)
            .await
            .unwrap();

        protocol2.initiate_sync(conn).await.unwrap();
        rx_sync_finished1.recv().await.unwrap();

        iroh1.shutdown().await.unwrap();
        iroh2.shutdown().await.unwrap();

        assert!(protocol1.adapter.dump().await.is_empty());
        assert!(protocol2.adapter.dump().await.is_empty());
    }

    #[tokio::test]
    async fn sync_from_empty() {
        let doc1 = Automerge::new();
        let mut doc2 = Automerge::new();
        let data = populate_doc(
            &mut doc2,
            vec![
                ("k-0".to_string(), "v-0".to_string()),
                ("k-1".to_string(), "v-1".to_string()),
            ],
        );

        let (tx_sync_finished1, mut rx_sync_finished1) = tokio::sync::mpsc::channel(1);
        let protocol1 = create_protocol(doc1, tx_sync_finished1);
        let iroh1 = spawn_router(protocol1.clone()).await;
        let addr1 = iroh1.endpoint().node_addr().await.unwrap();
        println!("sync_from_empty: addr1: {addr1:?}");

        let (tx_sync_finished2, _) = tokio::sync::mpsc::channel(1);
        let protocol2 = create_protocol(doc2, tx_sync_finished2);
        let iroh2 = spawn_router(protocol2.clone()).await;
        let conn = iroh2
            .endpoint()
            .connect(addr1, IROH_SYNCMAN_ALPN)
            .await
            .unwrap();

        protocol2.initiate_sync(conn).await.unwrap();
        rx_sync_finished1.recv().await.unwrap();

        iroh1.shutdown().await.unwrap();
        iroh2.shutdown().await.unwrap();

        assert_eq!(protocol1.adapter.dump().await, data);
        assert_eq!(protocol2.adapter.dump().await, data);
    }

    #[tokio::test]
    async fn mutual_sync() {
        let mut doc1 = Automerge::new();
        let mut data = populate_doc(
            &mut doc1,
            vec![
                ("k-0".to_string(), "v-0".to_string()),
                ("k-2".to_string(), "v-2".to_string()),
                ("k-c".to_string(), "v-c".to_string()),
            ],
        );
        let mut doc2 = Automerge::new();
        data.extend(populate_doc(
            &mut doc2,
            vec![
                ("k-1".to_string(), "v-1".to_string()),
                ("k-3".to_string(), "v-3".to_string()),
                ("k-c".to_string(), "v-c".to_string()),
            ],
        ));

        let (tx_sync_finished1, mut rx_sync_finished1) = tokio::sync::mpsc::channel(1);
        let protocol1 = create_protocol(doc1, tx_sync_finished1);
        let iroh1 = spawn_router(protocol1.clone()).await;
        let addr1 = iroh1.endpoint().node_addr().await.unwrap();
        println!("addr1: {addr1:?}");

        let (tx_sync_finished2, _) = tokio::sync::mpsc::channel(1);
        let protocol2 = create_protocol(doc2, tx_sync_finished2);
        let iroh2 = spawn_router(protocol2.clone()).await;
        let conn = iroh2
            .endpoint()
            .connect(addr1, IROH_SYNCMAN_ALPN)
            .await
            .unwrap();

        protocol2.initiate_sync(conn).await.unwrap();
        rx_sync_finished1.recv().await.unwrap();

        iroh1.shutdown().await.unwrap();
        iroh2.shutdown().await.unwrap();

        assert_eq!(protocol1.adapter.dump().await, data);
        assert_eq!(protocol2.adapter.dump().await, data);
    }

    fn create_protocol(
        doc: Automerge,
        sync_finished: mpsc::Sender<()>,
    ) -> IrohSyncmanProtocol<AutomergeSyncman, EmbeddedSyncmanAdapter<AutomergeSyncman>> {
        IrohSyncmanProtocol::new(
            EmbeddedSyncmanAdapter::new(AutomergeSyncman::new(doc)),
            sync_finished,
        )
    }

    async fn spawn_router<S, A>(protocol: IrohSyncmanProtocol<S, A>) -> Router
    where
        S: Syncman + Send + Sync + 'static,
        S::Handle: Send + Sync,
        A: SyncmanAdapter<S> + Send + Sync + 'static,
    {
        Router::builder(
            Endpoint::builder()
                .relay_mode(iroh::RelayMode::Disabled)
                .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
                .bind()
                .await
                .unwrap(),
        )
        .accept(IROH_SYNCMAN_ALPN, protocol)
        .spawn()
        .await
        .unwrap()
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
