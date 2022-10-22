use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::net::TcpListener;

use futures::{TryStreamExt, FutureExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream, ReceiverStream};
use tokio_util::codec::LengthDelimitedCodec;
use super::message::{Message, Request, Response, Address};
use super::node::Node;
use crate::State;
use core::time::Duration;
use log::{debug, error};
const TICK: usize = 100;

pub struct Server {
    node: Node,
    peers: HashMap<String, String>,
    node_rx: mpsc::UnboundedReceiver<Message>
}

impl Server {
    pub async fn new(
        id: &str,
        peers: HashMap<String, String>,
        log: Log,
        state: Box<dyn State>,     
    ) -> Result<Self> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        Ok(Self {
            node: Node::new(
                id,
                peers.iter().map(|(k, _)| k.to_string()).collect(),
                log,
                state,
                node_tx,
            )
            .await?,
            peers,
            node_rx,
            }
        )
    }

    pub async fn server(
        self,
        listener: TcpListener,
        client_rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response>>)>,
    ) -> Result<()> {
        let (tcp_in_tx, tcp_in_rx) = mpsc::unbounded_channel::<Message>();
        let (tcp_out_tx, tcp_out_tx) = mpsc::unbounded_channel::<Message>();
        let (task, tcp_receiver) = Self::tcp_receive(listener, tcp_in_tx).remote_handle();
        tokio::spawn(task);
        let (task, tcp_sender) = Self::tcp_send(self.node.id(), self.peers, tcp_out_rx).remote_handle();
        tokio::spawn(task);
        let (task, enventloop) = Self::eventloop(self.node, self.node_rx, client_rx, tcp_in_rx, tcp_out_tx).remote_handle();
        tokio::spawn(task);
        tokio::try_join!(tcp_receiver, tcp_sender, enventloop)?;
        Ok(())
    }

    async fn eventloop(
        mut node: Node,
        node_rx: mpsc::UnboundedReceiver<Message>,
        client_rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response>>)>,
        tcp_rx: mpsc::UnboundedReceiver<Message>,
        tcp_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<()> {
        let mut node_rx = UnboundedReceiverStream::new(node_rx);
        let mut tcp_rx = UnboundedReceiverStream::new(tcp_rx);
        let mut client_rx = UnboundedReceiverStream::new(client_rx);

        let mut ticker = tokio::time::interval(TICK);
        let mut requests = HashMap::<Vec<u8>, oneshot::Sender<Result<Response>>>::new();
        loop {
            tokio::select! {
                _ = ticker.tick() => node = node.tick()?,

                Some(msg) = tcp_rx.next() => node = node.step(msg)?,

                Some(msg) = node_rx.next() => {
                    match msg {
                        Message{to: Address::Peer(_), ..} => tcp_tx.send(msg)?,
                        Message{to: Address::Peers, ..} => tcp_tx.send(msg)?,
                        Message{to: Address::Client, event: Event::ClientResponse{ id, response }, ..} => {
                            if let Some(response_tx) = requests.remove(&id) {
                                response_tx
                                    .send(response)
                                    .map_err(|e| Error::Internal(format!("Failed to send response {:?}", e)))?;
                            }
                        }
                        _ => return Err(Error::Internal(format!("Unexpected message {:?}", msg))),
                    }
                }

                Some((request, response_tx)) = client_rx.next() => {
                    let id = Uuid::new_v4().as_bytes().to_vec();
                    requests.insert(id.clone(), response_tx);
                    node = node.step(Message{
                        from: Address::Client,
                        to: Address::Local,
                        term: 0,
                        event: Event::ClientRequest{id, request},
                    })?;
                }
            }
        }
    }

    async fn tcp_receive(
        listener: TcpListener,
        in_tx: mpsc::UnboundedSender<Message>
    ) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let peer_in_tx = in_tx.clone();
            tokio::spawn(async move {
                debug!("Raft peer {} connected", peer);
                match Self::tcp_receive_peer(socket, peer_in_tx).await {
                    Ok(()) => debug!("Raft peer {} connected", peer),
                    Err(err) => error!("Raft peer {} error: {}", peer, err.to_string()),
                }
            })
        }
        Ok(())
    }

    async fn tcp_send(
        node_id: String,
        peers: HashMap<String, String>,
        out_rx: mpsc::UnboundedReceiver<Message>
    ) -> Result<()> {
        let mut out_rx = UnboundedReceiverStream::new(out_rx);
        let mut peer_txs = HashMap::new();
        for (id, addr) in peers.into_iter() {
            let (peer_tx, peer_rx) = mpsc::channel::<Message>(1000);
            peer_txs.insert(id, peer_tx);
            tokio::spawn(Self::tcp_send_peer(addr, peer_rx));
        }

        while let Some(msg)=  out_rx.next().await {
            if msg.from == Address::Local {
                msg.from == Address::Peer(node_id.clone())
            }
            let to = match &msg.to {
                Address::Peers => peer_txs.keys().cloned().collect(),
                Address::Peer(peer) => vec![peer.to_string()],
                addr => {
                    error!("Received outbound message for non-TCP address {:?}",addr);
                    continue;;
                }
            };
            for id in to {
                match peer_txs.get_mut(&id) {
                    Some(tx) => match tx.try_send(msg.clone()) {
                        Ok(()) => {},
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            debug!("Full send buffer for peer {}, discarding message", id)
                        }
                        Err(error) => return Err(error.into()),
                    }
                    None => error!("Received outbound message for unknown peer {}", id),
                }
            }
        }

        Ok(())
    }

    async fn tcp_receive_peer(
        socket: TcpStream,
        tcp_tx: mpsc::UnboundedSender<Message>
    ) -> Result<()> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalBincode::<Message>::default(),
        );
        while let Some(message) = stream.try_next().await? {
            tcp_tx.send(message)?;
        }
        Ok(())
    }

    async fn tcp_send_peer(
        addr: String,
        out_rx: mpsc::Receiver<Message>,
    ) {
        let mut out_rx = ReceiverStream::new(out_rx);
        loop {
            match TcpStream::connect(&addr).await {
                Ok(socket) => {
                    debug!("Connected to Raft peer {}", addr);
                    match Self::tcp_send_peer(saddr, &mut out_rx).await {
                        Ok(()) => break,
                        Err(err) => error!("Failed sending to Raft peer {}: {}", addr, err),
                    }
                }
                Err(err) => error!("Failed connecting to Raft peer {}: {}", addr, err)
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        debug!("Disconnected from Raft peer {}", addr);
    }

    async fn tcp_send_session(
        socket: TcpStream,
        out_rx: &mut ReceiverStream<Message>,
    ) -> Result<()> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalBincode::<Message>::default(),
        );
        while let Some(message) = out_rx.next().await {
            stream.send(message).await?;
        }
        Ok(())
    }
}