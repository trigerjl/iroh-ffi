//! FFI bindings for `iroh-gossip` + `iroh-topic-tracker`.
//!
//! Exposes a small, UniFFI-friendly API for subscribing to a gossip topic
//! with DHT-based peer discovery via `iroh-topic-tracker`.

use std::sync::Arc;

use bytes::Bytes;
use futures_lite::StreamExt;
use iroh_gossip::api::{Event as GossipEvent, GossipReceiver, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_topic_tracker::{TopicDiscoveryConfig, TopicDiscoveryExt};
use tokio::sync::Mutex;

use crate::{Endpoint, IrohError};

/// Options for topic discovery.
#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct TopicDiscoveryOptions {
    /// Max peers to attempt to connect to per discovery round.
    #[uniffi(default = None)]
    pub max_peers_per_round: Option<u64>,
    /// Connection timeout in milliseconds.
    #[uniffi(default = None)]
    pub connection_timeout_ms: Option<u64>,
    /// DHT announce interval in milliseconds.
    #[uniffi(default = None)]
    pub announce_interval_ms: Option<u64>,
    /// Discovery interval when connected in milliseconds.
    #[uniffi(default = None)]
    pub discovery_interval_ms: Option<u64>,
    /// Discovery interval when no peers in milliseconds.
    #[uniffi(default = None)]
    pub discovery_interval_no_peers_ms: Option<u64>,
}

/// A gossip message received from a topic.
#[derive(Debug, Clone, uniffi::Record)]
pub struct TopicMessage {
    /// Message content bytes.
    pub content: Vec<u8>,
    /// Endpoint id of the peer that delivered the message.
    pub delivered_from: String,
    /// Whether the message came directly from its broadcaster.
    pub is_direct: bool,
}

/// Event emitted by topic subscription.
#[derive(Debug, Clone, uniffi::Enum)]
pub enum TopicEvent {
    /// A neighbor joined the topic swarm.
    NeighborUp { endpoint_id: String },
    /// A neighbor left the topic swarm.
    NeighborDown { endpoint_id: String },
    /// A message was received.
    Received { message: TopicMessage },
    /// Subscription lagged; some events were dropped.
    Lagged,
}

/// Handle to a gossip topic subscription with DHT discovery.
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
    /// Broadcast a message to all peers in the topic.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn broadcast(&self, message: Vec<u8>) -> Result<(), IrohError> {
        self.sender
            .broadcast(Bytes::from(message))
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(())
    }

    /// Broadcast a message to direct neighbors only.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn broadcast_neighbors(&self, message: Vec<u8>) -> Result<(), IrohError> {
        self.sender
            .broadcast_neighbors(Bytes::from(message))
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(())
    }

    /// Join specific peers by endpoint id strings.
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

    /// Receive the next event from the topic. Returns None if closed.
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
        }))
    }

    /// Stop topic discovery. The gossip subscription remains until dropped.
    pub fn stop_discovery(&self) {
        self.discovery.stop();
    }

    /// Whether discovery task is still running.
    pub fn is_discovery_running(&self) -> bool {
        self.discovery.is_running()
    }

    /// Number of peers discovered/connected by discovery.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn discovered_peer_count(&self) -> u64 {
        self.discovery.added_connection_count().await as u64
    }
}

/// Gossip topic/discovery node bound to an iroh endpoint.
#[derive(uniffi::Object)]
pub struct GossipNode {
    gossip: Gossip,
    endpoint: Arc<Endpoint>,
}

#[uniffi::export]
impl GossipNode {
    /// Create a gossip node on an existing endpoint.
    #[uniffi::constructor(async_runtime = "tokio")]
    pub async fn spawn(endpoint: Arc<Endpoint>) -> Result<Arc<Self>, IrohError> {
        let gossip = Gossip::builder().spawn(endpoint.raw().clone());
        Ok(Arc::new(Self { gossip, endpoint }))
    }

    /// Subscribe to a topic with DHT discovery. `topic_id` is arbitrary bytes.
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

    /// Subscribe and wait until discovery has joined.
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
