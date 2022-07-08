use std::sync::Arc;

use async_trait::async_trait;
use poem::{Endpoint, IntoResponse, Middleware, Request, Response};

use crate::{RegistryStore, OPT_SEQ_HEADER_NAME};

pub struct RaftSequencer {
    store: Arc<RegistryStore>,
}

impl RaftSequencer {
    pub fn new(store: Arc<RegistryStore>) -> Self {
        Self { store }
    }
}

impl<E: Endpoint> Middleware<E> for RaftSequencer {
    type Output = RaftSequencerImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        RaftSequencerImpl {
            ep,
            store: self.store.clone(),
        }
    }
}

pub struct RaftSequencerImpl<E> {
    ep: E,
    store: Arc<RegistryStore>,
}

#[async_trait]
impl<E: Endpoint> Endpoint for RaftSequencerImpl<E> {
    type Output = Response;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        let res = self.ep.call(req).await;
        let opt_seq = self
            .store
            .state_machine
            .read()
            .await
            .last_applied_log
            .map(|l| l.index);

        match res {
            Ok(resp) => {
                let resp = match opt_seq {
                    Some(v) => resp.with_header(OPT_SEQ_HEADER_NAME, v).into_response(),
                    None => resp.into_response(),
                };
                Ok(resp)
            }
            Err(err) => Err(err),
        }
    }
}
