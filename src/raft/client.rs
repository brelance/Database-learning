use tokio::sync::{mpsc, oneshot};
use crate::error::{Error, Result};
use super::message::{Request, Response};
use super::node::Status;


pub struct Client {
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>
}

impl Client {
    pub fn new(request_tx:
        mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>
        ) -> Self {
            Client { request_tx, }
        }

    async fn request(&self, request: Request) -> Result<Response> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx.send((request, response_tx))?;
        response_rx.await?
    }

    pub async fn mutate(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Mutate(request)).await? {
            Response::State(response) => Ok(response),
            response => Err(Error::Internal(format!("Unexpeted Raft mutate response {:?}", response)))
        }
    }

    pub async fn query(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Query(request)).await? {
            Response::State(response) => Ok(response),
            response => Err(Error::Internal(format!("Unexpected Raft query response {:?}", response))),
        }
    }

    pub async fn status(&self) -> Result<Status> {
        match self.request(Request::Status).await? {
            Response::Status(status) => Ok(status),
            status => Err(Error::Internal(format!("Unexpected Raft status response {:?}", resp))),
        }
    }

    
}