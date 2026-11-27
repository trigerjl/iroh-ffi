use std::sync::Arc;

use bytes::Bytes;
use futures_lite::StreamExt;
use iroh_gossip::{Event as GossipEvent, Gossip, GossipReceiver, GossipSender};
use iroh_topic_tracker::{TopicDiscoveryConfig, TopicDiscoveryExt};
use tokio::sync::Mutex;

use crate::{Endpoint, IrohError};

#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct TopicDiscoveryOptions {
    #[uniffi(default = None)]
    pub max_peers_per_round: Option<u64>,
    #[uniffi(default = None)]
    pub connection_timeout_ms: Option<u64>,
    #[uniffi(default = None)]
    pub announce_interval_ms: Option<u64>,
    #[uniffi(default = None)]
    pub discovery_interval_ms: Option<u64>,
    #[uniffi(default = None)]
    pub discovery_interval_no_peers_ms: Option<u64>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct TopicMessage {
    pub content: Vec<u8>,
    pub delivered_from: String,
    pub is_direct: bool,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum TopicEvent {
    NeighborUp { endpoint_id: String },
    NeighborDown { endpoint_id: String },
    Received { message: TopicMessage },
    Lagged,
    Unknown,
}

#[derive(uniffi::Object)]
pub struct TopicSubscription {
    sender: GossipSender,
    receiver: Arc<Mutex<GossipReceiver>>,
    discovery: iroh_topic_tracker::TopicDiscoveryHandle,
}

impl TopicSubscription {
    fn new(
        sender: GossipSender,
        receiver: GossipReceiver,
        discovery: iroh_topic_tracker::TopicDiscoveryHandle,
    ) -> Self {
        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            discovery,
        }
    }
}

#[uniffi::export]
impl TopicSubscription {
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn broadcast(&self, message: Vec<u8>) -> Result<(), IrohError> {
        self.sender
            .broadcast(Bytes::from(message))
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(())
    }

    #[uniffi::method(async_runtime = "tokio")]
    pub async fn broadcast_neighbors(&self, message: Vec<u8>) -> Result<(), IrohError> {
        self.sender
            .broadcast_neighbors(Bytes::from(message))
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(())
    }

    #[uniffi::method(async_runtime = "tokio")]
    pub async fn join_peers(&self, peers: Vec<String>) -> Result<(), IrohError> {
        let mut ids = Vec::with_capacity(peers.len());
        for p in peers {
            let id: iroh::EndpointId = p
                .parse()
                .map_err(|e| IrohError::invalid_input(format!("invalid endpoint id: {e:?}")))?;
            ids.push(id);
        }
        self.sender
            .join_peers(ids)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(())
    }

    #[uniffi::method(async_runtime = "tokio")]
    pub async fn next_event(&self) -> Result<Option<TopicEvent>, IrohError> {
        let mut rx = self.receiver.lock().await;
        let Some(ev) = rx.next().await else {
            return Ok(None);
        };
        let ev = ev.map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(Some(match ev {
            GossipEvent::NeighborUp(id) => TopicEvent::NeighborUp {
                endpoint_id: id.to_string(),
            },
            GossipEvent::NeighborDown(id) => TopicEvent::NeighborDown {
                endpoint_id: id.to_string(),
            },
            GossipEvent::Received(msg) => TopicEvent::Received {
                message: TopicMessage {
                    content: msg.content.to_vec(),
                    delivered_from: msg.delivered_from.to_string(),
                    is_direct: msg.scope.is_direct(),
                },
            },
            GossipEvent::Lagged => TopicEvent::Lagged,
            _ => TopicEvent::Unknown,
        }))
    }

    pub fn stop_discovery(&self) {
        self.discovery.stop();
    }

    pub fn is_discovery_running(&self) -> bool {
        self.discovery.is_running()
    }

    #[uniffi::method(async_runtime = "tokio")]
    pub async fn discovered_peer_count(&self) -> u64 {
        self.discovery.added_connection_count().await as u64
    }
}

#[derive(uniffi::Object)]
pub struct GossipNode {
    gossip: Gossip,
    endpoint: Arc<Endpoint>,
}

#[uniffi::export]
impl GossipNode {
    #[uniffi::constructor(async_runtime = "tokio")]
    pub async fn spawn(endpoint: Arc<Endpoint>) -> Result<Arc<Self>, IrohError> {
        let gossip = Gossip::builder().spawn(endpoint.raw().clone());
        Ok(Arc::new(Self { gossip, endpoint }))
    }

    #[uniffi::method(async_runtime = "tokio")]
    pub async fn subscribe_with_discovery(
        &self,
        topic_id: Vec<u8>,
        bootstrap_peers: Vec<String>,
        options: Option<TopicDiscoveryOptions>,
    ) -> Result<Arc<TopicSubscription>, IrohError> {
        let mut bootstrap = Vec::with_capacity(bootstrap_peers.len());
        for p in bootstrap_peers {
            let id: iroh::EndpointId = p
                .parse()
                .map_err(|e| IrohError::invalid_input(format!("invalid bootstrap peer: {e:?}")))?;
            bootstrap.push(id);
        }

        let mut builder = TopicDiscoveryConfig::builder(self.endpoint.raw().clone());
        if let Some(opts) = options {
            if let Some(v) = opts.max_peers_per_round {
                builder = builder.max_peers_per_round(Some(v as usize));
            }
            if let Some(v) = opts.connection_timeout_ms {
                builder = builder.connection_timeout(std::time::Duration::from_millis(v));
            }
            if let Some(v) = opts.announce_interval_ms {
                builder = builder.announce_interval(std::time::Duration::from_millis(v));
            }
            if let Some(v) = opts.discovery_interval_ms {
                builder = builder.discovery_interval(std::time::Duration::from_millis(v));
            }
            if let Some(v) = opts.discovery_interval_no_peers_ms {
                builder = builder.discovery_interval_no_peers(std::time::Duration::from_millis(v));
            }
        }
        let config = builder.build();

        let (sender, receiver, discovery) = self
            .gossip
            .subscribe_with_discovery(topic_id, bootstrap, config)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        Ok(Arc::new(TopicSubscription::new(sender, receiver, discovery)))
    }

    #[uniffi::method(async_runtime = "tokio")]
    pub async fn subscribe_with_discovery_joined(
        &self,
        topic_id: Vec<u8>,
        bootstrap_peers: Vec<String>,
        options: Option<TopicDiscoveryOptions>,
    ) -> Result<Arc<TopicSubscription>, IrohError> {
        let mut bootstrap = Vec::with_capacity(bootstrap_peers.len());
        for p in bootstrap_peers {
            let id: iroh::EndpointId = p
                .parse()
                .map_err(|e| IrohError::invalid_input(format!("invalid bootstrap peer: {e:?}")))?;
            bootstrap.push(id);
        }

        let mut builder = TopicDiscoveryConfig::builder(self.endpoint.raw().clone());
        if let Some(opts) = options {
            if let Some(v) = opts.max_peers_per_round {
                builder = builder.max_peers_per_round(Some(v as usize));
            }
            if let Some(v) = opts.connection_timeout_ms {
                builder = builder.connection_timeout(std::time::Duration::from_millis(v));
            }
            if let Some(v) = opts.announce_interval_ms {
                builder = builder.announce_interval(std::time::Duration::from_millis(v));
            }
            if let Some(v) = opts.discovery_interval_ms {
                builder = builder.discovery_interval(std::time::Duration::from_millis(v));
            }
            if let Some(v) = opts.discovery_interval_no_peers_ms {
                builder = builder.discovery_interval_no_peers(std::time::Duration::from_millis(v));
            }
        }
        let config = builder.build();

        let (sender, receiver, discovery) = self
            .gossip
            .subscribe_with_discovery_joined(topic_id, bootstrap, config)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        Ok(Arc::new(TopicSubscription::new(sender, receiver, discovery)))
    }
}
