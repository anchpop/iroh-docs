//! [`ProtocolHandler`] implementation for the docs [`Engine`].

use std::sync::Arc;

use anyhow::Result;
use iroh::{endpoint::Connection, protocol::ProtocolHandler, Endpoint};
use iroh_blobs::api::Store as BlobsStore;
use iroh_gossip::net::Gossip;

use crate::{
    api::DocsApi,
    engine::{Engine, ProtectCallbackHandler},
    store::Store,
};

/// Docs protocol.
#[derive(Debug, Clone)]
pub struct Docs {
    engine: Arc<Engine>,
    api: DocsApi,
}

impl Docs {
    /// Create a new [`Builder`] for the docs protocol, using an in-memory replica store.
    pub fn memory() -> Builder {
        Self::with_store(Store::memory())
    }

    /// Create a new [`Builder`] for the docs protocol, using a persistent replica store
    /// in the given directory.
    ///
    /// Returns an error if the store cannot be opened.
    #[cfg(feature = "fs-store")]
    pub fn persistent(path: std::path::PathBuf) -> anyhow::Result<Builder> {
        Ok(Self::with_store(Store::persistent(path.join("docs.redb"))?))
    }

    /// Create a new [`Builder`] for the docs protocol, using a replica store
    /// constructed by the caller.
    ///
    /// Use this when the store is backed by storage the builder cannot open
    /// itself, e.g. a custom redb backend via [`Store::from_database`].
    pub fn with_store(store: Store) -> Builder {
        Builder {
            store,
            protect_cb: None,
        }
    }

    /// Creates a new [`Docs`] from an [`Engine`].
    pub fn new(engine: Engine) -> Self {
        let engine = Arc::new(engine);
        let api = DocsApi::spawn(engine.clone());
        Self { engine, api }
    }

    /// Returns the API for this docs instance.
    pub fn api(&self) -> &DocsApi {
        &self.api
    }
}

impl std::ops::Deref for Docs {
    type Target = DocsApi;

    fn deref(&self) -> &Self::Target {
        &self.api
    }
}

impl ProtocolHandler for Docs {
    async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
        self.engine
            .handle_connection(connection)
            .await
            .map_err(|err| iroh::protocol::AcceptError::from_err(n0_error::anyerr!(err)))?;
        Ok(())
    }

    async fn shutdown(&self) {
        if let Err(err) = self.engine.shutdown().await {
            tracing::warn!("shutdown error: {:?}", err);
        }
    }
}

/// Builder for the docs protocol.
#[derive(Debug)]
pub struct Builder {
    store: Store,
    protect_cb: Option<ProtectCallbackHandler>,
}

impl Builder {
    /// Set the garbage collection protection handler for blobs.
    ///
    /// See [`ProtectCallbackHandler::new`] for details.
    pub fn protect_handler(mut self, protect_handler: ProtectCallbackHandler) -> Self {
        self.protect_cb = Some(protect_handler);
        self
    }

    /// Build a [`Docs`] protocol given a [`BlobsStore`] and [`Gossip`] protocol.
    pub async fn spawn(
        self,
        endpoint: Endpoint,
        blobs: BlobsStore,
        gossip: Gossip,
    ) -> anyhow::Result<Docs> {
        let replica_store = self.store;
        let downloader = blobs.downloader(&endpoint);
        let engine = Engine::spawn(
            endpoint,
            gossip,
            replica_store,
            blobs,
            downloader,
            self.protect_cb,
        )
        .await?;
        Ok(Docs::new(engine))
    }
}
