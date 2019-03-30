// Copyright © 2019 Felix Obenhuber
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::codec;
use log::{error, info, trace, warn};
use mqtt3::{Connack, ConnectReturnCode, Packet, PacketIdentifier, Publish, QoS, Suback, Subscribe, SubscribeReturnCodes, SubscribeTopic};
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    codec::Framed,
    net::TcpStream,
    prelude::{Stream, *},
    sync::mpsc,
};
// use tokio::timer::Interval;
// use tokio::timer::Delay;
// use std::time::Duration;
// use std::time::Instant;
use crate::{client::Client, subscriptions::Subscriptions};
use std::{
    cmp,
    collections::hash_map::DefaultHasher,
    fmt,
    hash::{Hash, Hasher},
};

#[derive(Clone, Debug)]
pub struct ClientId {
    id: String,
    // Cache hash for id
    hash: u64,
}

impl ClientId {
    pub fn new(id: &str) -> ClientId {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        ClientId {
            id: id.to_string(),
            hash: hasher.finish(),
        }
    }
}

impl Hash for ClientId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl cmp::PartialEq for ClientId {
    fn eq(&self, other: &ClientId) -> bool {
        self.hash == other.hash
    }
}
impl Eq for ClientId {}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

pub enum Event {
    Packet(SocketAddr, Packet),
    Connected(TcpStream),
    Disconnected(SocketAddr),
}

pub struct Broker {
    tx: mpsc::Sender<Event>,
    clients: HashMap<ClientId, Client>,
    connections: HashMap<SocketAddr, ClientId>,
    subscriptions: Subscriptions,
    retains: HashMap<SubscribeTopic, Publish>,
}

impl Broker {
    pub fn start() -> mpsc::Sender<Event> {
        let (tx, mut rx) = mpsc::channel::<Event>(100);

        let mut broker = Broker {
            tx: tx.clone(),
            clients: HashMap::new(),
            connections: HashMap::new(),
            subscriptions: Subscriptions::default(),
            retains: HashMap::new(),
        };

        tokio::spawn_async(async move {
            while let Some(event) = await!(rx.next()) {
                await!(broker.handle_event(event.unwrap()));
            }
        });
        tx
    }

    pub async fn handle_event(&mut self, event: Event) {
        match event {
            Event::Packet(sender, packet) => {
                await!(self.handle_packet(sender, packet));
            }
            Event::Connected(stream) => {
                await!(self.handle_connect(stream));
            }
            Event::Disconnected(addr) => {
                self.handle_disconnect(addr);
            }
        };
    }

    pub async fn handle_packet(&mut self, sender: SocketAddr, packet: Packet) {
        trace!("Inbound packet from {}: {:#?} ", sender, packet);
        let client_id = if let Some(client) = self.connections.get(&sender) {
            client.clone()
        } else {
            warn!("Discarding packet from unknwon sender!");
            return;
        };

        match packet {
            Packet::Subscribe(subscribe) => {
                await!(self.handle_subscribe(&client_id, &subscribe));
            }
            Packet::Publish(mut publish) => {
                await!(self.handle_publish(&client_id, &mut publish));
            }
            Packet::Puback(pkid) => self.handle_puback(&client_id, pkid),
            Packet::Pingreq => {
                await!(self.handle_pingreq(&client_id));
            }
            _ => unimplemented!(),
        };
    }

    fn handle_puback(&mut self, client_id: &ClientId, pkid: PacketIdentifier) {
        if let Some(ref mut client) = self.clients.get_mut(client_id) {
            client.handle_puback(pkid);
        }
    }

    async fn handle_subscribe<'a>(&'a mut self, client_id: &'a ClientId, subscribe: &'a Subscribe) {
        if let Some(ref mut client) = self.clients.get_mut(client_id) {
            let pkid = subscribe.pid;
            let mut return_codes = Vec::new();
            let mut successful_subscriptions = Vec::new();
            for topic in subscribe.topics.clone() {
                return_codes.push(SubscribeReturnCodes::Success(topic.qos));
                successful_subscriptions.push(topic.clone());
                self.subscriptions.add_subscription(topic, client_id.clone()).unwrap(); // TODO: handle error
            }

            let suback = Suback { pid: pkid, return_codes };
            let packet = Packet::Suback(suback);

            await!(client.send(packet));
        } else {
            warn!("Cannot subscribe unknown client {:?}", client_id);
        }
    }

    /// Forward a publication to all subscribed clients
    async fn forward<'a>(&'a mut self, publish: &'a Publish) {
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce].iter() {
            let subscribe_topic = SubscribeTopic {
                topic_path: publish.topic_name.clone(),
                qos: *qos,
            };

            if let Ok(clients) = self.subscriptions.get_subscribed_clients(subscribe_topic) {
                for client_id in clients {
                    if let Some(ref mut client) = self.clients.get_mut(&client_id) {
                        await!(client.publish(publish.clone()));
                    }
                }
            }
        }
    }

    pub async fn handle_publish<'a>(&'a mut self, client_id: &'a ClientId, publish: &'a mut Publish) {
        info!("Publish: {:?}", publish);
        let pkid = publish.pid;
        let qos = publish.qos;

        if publish.retain {
            self.store_retain(publish.clone());
            publish.retain = false;
        }

        match qos {
            QoS::AtMostOnce => (),
            QoS::AtLeastOnce => {
                if let Some(pkid) = pkid {
                    if let Some(ref mut client) = self.clients.get_mut(client_id) {
                        await!(client.send(Packet::Puback(pkid)));
                    }
                } else {
                    error!("Ignoring publish packet. No pkid for QoS1 packet");
                }
            }
            QoS::ExactlyOnce => unimplemented!(),
        };

        await!(self.forward(publish));
    }

    pub fn handle_disconnect(&mut self, addr: SocketAddr) {
        if let Some(client_id) = self.connections.remove(&addr) {
            info!("Client disconnnected: {} ({:?})", client_id, addr);
            self.clients.remove(&client_id);
            self.subscriptions.remove_client(&client_id);
        }
    }

    pub async fn handle_pingreq<'a>(&'a mut self, client_id: &'a ClientId) {
        if let Some(ref mut client) = self.clients.get_mut(client_id) {
            trace!("Sending Pingresponse to {:?}", client_id);
            await!(client.send(Packet::Pingresp));
        }
    }

    pub async fn handle_connect(&mut self, stream: TcpStream) {
        let addr = stream.peer_addr().unwrap();
        let (mut sink, mut stream) = Framed::new(stream, codec::MqttCodec).split();
        if let Some(Ok(Packet::Connect(connect))) = await!(stream.next()) {
            assert!(connect.clean_session);
            trace!("Connect from {} {:#?}", addr, connect);

            let (client_tx, mut client_rx) = mpsc::channel(100);

            // From client
            let mut broker_tx = self.tx.clone();
            tokio::spawn_async(async move {
                while let Some(packet) = await!(stream.next()) {
                    match packet {
                        Ok(packet) => await!(broker_tx.send_async(Event::Packet(addr, packet))).unwrap(),
                        Err(e) => {
                            warn!("Error on connection to {}: {}", addr, e);
                            break; // io::Error
                        }
                    }
                }
                await!(broker_tx.send_async(Event::Disconnected(addr))).unwrap();
            });

            // To client
            let mut broker_tx = self.tx.clone();
            tokio::spawn_async(async move {
                while let Some(packet) = await!(client_rx.next()) {
                    if let Ok(packet) = packet {
                        if let Err(e) = await!(sink.send_async(packet)) {
                            trace!("Failed to send to {}: {}", addr, e);
                            break;
                        }
                    } else {
                        break;
                    }
                }
                await!(broker_tx.send_async(Event::Disconnected(addr))).unwrap();
            });

            let connack = Packet::Connack(Connack {
                session_present: false,
                code: ConnectReturnCode::Accepted,
            });

            let id = ClientId::new(&connect.client_id);
            let mut client = Client::new(self.tx.clone(), client_tx, addr);
            await!(client.send(connack));
            self.clients.insert(id.clone(), client);
            self.connections.insert(addr, id);
;
        // Pings
        // let duration = Duration::from_secs(u64::from(connect.keep_alive));
        // let id = connect.client_id;
        // tokio::spawn_async(async move {
        //     loop {
        //         await!(Delay::new(Instant::now() + duration));
        //         debug!("Sending ping to {}", id)
        //         //await!(broker_tx.send_async(Event::Packet(addr, packet))).unwrap();
        //     }
        // });
        } else {
            warn!("Invalid connection attempt from {}", addr);
        }
    }

    fn store_retain(&mut self, publish: Publish) {
        let retain_subscription = SubscribeTopic {
            topic_path: publish.topic_name.clone(),
            qos: publish.qos,
        };
        if publish.payload.is_empty() {
            // Remove existing retains if new retain publish's payload len = 0
            self.retains.remove(&retain_subscription);
        } else {
            self.retains.insert(retain_subscription, publish);
        }
    }
}
