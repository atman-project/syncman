use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use anyhow::Ok;
use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    protocol::ProtocolHandler,
};
use syncman::{automerge::AutomergeSyncman, SyncHandle, SyncMessage, Syncman};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct IrohSyncmanProtocol {
    // TODO: do not embed this here. get it from somewhere
    // TODO: use a generic instead of AutomergeSyncman
    syncman: Arc<Mutex<AutomergeSyncman>>,
    sync_finished: mpsc::Sender<()>,
}

impl ProtocolHandler for IrohSyncmanProtocol {
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

impl IrohSyncmanProtocol {
    pub const ALPN: &'static [u8] = b"atman/syncman/1";

    pub fn new(syncman: AutomergeSyncman, sync_finished: mpsc::Sender<()>) -> Self {
        Self {
            syncman: Arc::new(Mutex::new(syncman)),
            sync_finished,
        }
    }

    pub async fn initiate_sync(&self, conn: Connection) -> anyhow::Result<()> {
        let (mut conn_send, mut conn_recv) = conn.open_bi().await?;
        let mut handle = self.open_sync_handle();
        let mut is_local_done = false;
        let mut is_remote_done = false;
        while !is_local_done || !is_remote_done {
            if !is_local_done {
                let msg = handle.generate_message();
                Self::send_msg(&msg, &mut conn_send).await?;
                is_local_done = matches!(msg, SyncMessage::Done);
            }

            if !is_remote_done {
                let msg = Self::recv_msg(&mut conn_recv).await?;
                self.apply_sync(&mut handle, &msg);
                is_remote_done = matches!(msg, SyncMessage::Done);
            }
        }
        conn_send.finish()?;
        conn_send.stopped().await?;
        Ok(())
    }

    async fn respond_sync(&self, conn: Connection) -> anyhow::Result<()> {
        let (mut conn_send, mut conn_recv) = conn.accept_bi().await?;

        let mut handle = self.open_sync_handle();
        let mut is_local_done = false;
        let mut is_remote_done = false;
        while !is_local_done || !is_remote_done {
            if !is_remote_done {
                let msg = Self::recv_msg(&mut conn_recv).await?;
                self.apply_sync(&mut handle, &msg);
                is_remote_done = matches!(msg, SyncMessage::Done);
            }

            if !is_local_done {
                let msg = handle.generate_message();
                Self::send_msg(&msg, &mut conn_send).await?;
                is_local_done = matches!(msg, SyncMessage::Done);
            }
        }
        conn_send.finish()?;
        conn_send.stopped().await?;
        Ok(())
    }

    fn open_sync_handle(&self) -> <AutomergeSyncman as Syncman>::Handle {
        let syncman = self.syncman.lock().unwrap();
        syncman.initiate_sync()
    }

    fn apply_sync(&self, handle: &mut <AutomergeSyncman as Syncman>::Handle, msg: &SyncMessage) {
        let mut syncman = self.syncman.lock().unwrap();
        syncman.apply_sync(handle, msg);
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

    use super::*;

    #[tokio::test]
    async fn sync_empty_docs() {
        let (tx_sync_finished1, mut rx_sync_finished1) = tokio::sync::mpsc::channel(1);
        let protocol1 =
            IrohSyncmanProtocol::new(AutomergeSyncman::new(Automerge::new()), tx_sync_finished1);
        let iroh1 = Router::builder(endpoint().await)
            .accept(IrohSyncmanProtocol::ALPN, protocol1.clone())
            .spawn()
            .await
            .unwrap();
        let addr1 = iroh1.endpoint().node_addr().await.unwrap();
        println!("sync_empty_docs: addr1: {addr1:?}");

        let (tx_sync_finished2, _) = tokio::sync::mpsc::channel(1);
        let protocol2 =
            IrohSyncmanProtocol::new(AutomergeSyncman::new(Automerge::new()), tx_sync_finished2);
        let iroh2 = Router::builder(endpoint().await)
            .accept(IrohSyncmanProtocol::ALPN, protocol2.clone())
            .spawn()
            .await
            .unwrap();
        let conn = iroh2
            .endpoint()
            .connect(addr1, IrohSyncmanProtocol::ALPN)
            .await
            .unwrap();

        protocol2.initiate_sync(conn).await.unwrap();
        rx_sync_finished1.recv().await.unwrap();

        iroh1.shutdown().await.unwrap();
        iroh2.shutdown().await.unwrap();

        assert!(doc_to_hashmap(&protocol1.syncman.lock().unwrap().doc()).is_empty());
        assert!(doc_to_hashmap(&protocol2.syncman.lock().unwrap().doc()).is_empty());
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
        let protocol1 = IrohSyncmanProtocol::new(AutomergeSyncman::new(doc1), tx_sync_finished1);
        let iroh1 = Router::builder(endpoint().await)
            .accept(IrohSyncmanProtocol::ALPN, protocol1.clone())
            .spawn()
            .await
            .unwrap();
        let addr1 = iroh1.endpoint().node_addr().await.unwrap();
        println!("sync_from_empty: addr1: {addr1:?}");

        let (tx_sync_finished2, _) = tokio::sync::mpsc::channel(1);
        let protocol2 = IrohSyncmanProtocol::new(AutomergeSyncman::new(doc2), tx_sync_finished2);
        let iroh2 = Router::builder(endpoint().await)
            .accept(IrohSyncmanProtocol::ALPN, protocol2.clone())
            .spawn()
            .await
            .unwrap();
        let conn = iroh2
            .endpoint()
            .connect(addr1, IrohSyncmanProtocol::ALPN)
            .await
            .unwrap();

        protocol2.initiate_sync(conn).await.unwrap();
        rx_sync_finished1.recv().await.unwrap();

        iroh1.shutdown().await.unwrap();
        iroh2.shutdown().await.unwrap();

        assert_eq!(
            doc_to_hashmap(&protocol1.syncman.lock().unwrap().doc()),
            data
        );
        assert_eq!(
            doc_to_hashmap(&protocol2.syncman.lock().unwrap().doc()),
            data
        );
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
        let protocol1 = IrohSyncmanProtocol::new(AutomergeSyncman::new(doc1), tx_sync_finished1);
        let iroh1 = Router::builder(endpoint().await)
            .accept(IrohSyncmanProtocol::ALPN, protocol1.clone())
            .spawn()
            .await
            .unwrap();
        let addr1 = iroh1.endpoint().node_addr().await.unwrap();
        println!("addr1: {addr1:?}");

        let (tx_sync_finished2, _) = tokio::sync::mpsc::channel(1);
        let protocol2 = IrohSyncmanProtocol::new(AutomergeSyncman::new(doc2), tx_sync_finished2);
        let iroh2 = Router::builder(endpoint().await)
            .accept(IrohSyncmanProtocol::ALPN, protocol2.clone())
            .spawn()
            .await
            .unwrap();
        let conn = iroh2
            .endpoint()
            .connect(addr1, IrohSyncmanProtocol::ALPN)
            .await
            .unwrap();

        protocol2.initiate_sync(conn).await.unwrap();
        rx_sync_finished1.recv().await.unwrap();

        iroh1.shutdown().await.unwrap();
        iroh2.shutdown().await.unwrap();

        assert_eq!(
            doc_to_hashmap(&protocol1.syncman.lock().unwrap().doc()),
            data
        );
        assert_eq!(
            doc_to_hashmap(&protocol2.syncman.lock().unwrap().doc()),
            data
        );
    }

    async fn endpoint() -> Endpoint {
        Endpoint::builder()
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .bind()
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
