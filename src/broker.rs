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

use crate::{client::Client, client_id::ClientId, subscriptions::Subscriptions};
use bytes::Bytes;
use hashbrown::HashMap;
use log::{info, trace, warn};
use mqtt_codec::{QoS, *};
use std::{net::SocketAddr, str::FromStr};
use tokio::{
    codec::Framed,
    net::TcpStream,
    prelude::{Stream, *},
    runtime::Runtime,
    sync::mpsc,
};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    Packet(SocketAddr, Packet),
    Connection((SocketAddr, TcpStream)),
    ClientConnected(SocketAddr, Connect, mpsc::Sender<Packet>),
    Disconnected(SocketAddr),
}

#[derive(Debug)]
pub struct Broker {
    tx: mpsc::Sender<Event>,
    clients: HashMap<ClientId, Client>,
    connections: HashMap<SocketAddr, ClientId>,
    subscriptions: Subscriptions,
    retains: HashMap<string::String<Bytes>, Publish>,
}

impl Broker {
    pub fn start(runtime: &mut Runtime) -> mpsc::Sender<Event> {
        let (tx, rx) = mpsc::channel::<Event>(100);

        let mut broker = Broker {
            tx: tx.clone(),
            clients: HashMap::new(),
            connections: HashMap::new(),
            subscriptions: Subscriptions::default(),
            retains: HashMap::new(),
        };

        runtime.spawn(
            rx.for_each(move |event| {
                broker.handle_event(event);
                Ok(())
            })
            .map_err(drop)
            .map(drop),
        );
        tx
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::ClientConnected(addr, connect, tx) => {
                self.handle_client_connected(addr, connect, tx);
            }
            Event::Packet(sender, packet) => {
                self.handle_packet(sender, packet);
            }
            Event::Disconnected(addr) => {
                self.handle_disconnect(addr);
            }
            Event::Connection((addr, stream)) => self.handle_connect(addr, stream),
        };
    }

    fn handle_packet(&mut self, sender: SocketAddr, packet: Packet) {
        let client_id = if let Some(client) = self.connections.get(&sender) {
            client.clone()
        } else {
            warn!("Discarding packet from unknwon sender!");
            return;
        };

        match packet {
            Packet::Subscribe {
                packet_id,
                mut topic_filters,
            } => self.handle_subscribe(&client_id, packet_id, &mut topic_filters),
            Packet::Unsubscribe {
                packet_id,
                mut topic_filters,
            } => self.handle_unsubscribe(&client_id, packet_id, &mut topic_filters),
            Packet::Publish(mut publish) => self.handle_publish(&client_id, &mut publish),
            Packet::PublishAck { packet_id } => self.handle_puback(&client_id, packet_id),
            Packet::PublishRelease { packet_id } => self.handle_pubrel(&client_id, packet_id),
            Packet::PublishComplete { packet_id } => self.handle_pubcomp(&client_id, packet_id),
            Packet::PublishReceived { packet_id } => self.handle_pubrec(&client_id, packet_id),
            Packet::PingRequest => self.handle_pingreq(&client_id),
            // The connect packet is handled before. This is a Connect received on a
            // established connection
            Packet::Connect(_) | Packet::Disconnect => self.handle_disconnect(sender),
            p => panic!("Unimplemented packet: {:?}", p),
        };
    }

    fn handle_puback(&mut self, client_id: &ClientId, packet_id: u16) {
        if let Some(ref mut client) = self.clients.get_mut(client_id) {
            client.handle_puback(packet_id);
        } else {
            warn!("Cannot handle PublishAck({}) from unknown client {}", packet_id, client_id);
        }
    }

    fn handle_pubrel(&mut self, client_id: &ClientId, packet_id: u16) {
        if let Some(ref mut client) = self.clients.get_mut(client_id) {
            let publish = client.handle_pubrel(packet_id).expect("internal error");
            let qos = publish.qos;

            match std::str::from_utf8(publish.topic.get_ref())
                .map_err(drop)
                .and_then(|s| Topic::from_str(s).map_err(drop))
            {
                Ok(topic) => {
                    let subscriptions = self.subscriptions.get_subscriptions(&topic);
                    for s in subscriptions {
                        if let Some(client) = self.clients.get_mut(&s.client_id) {
                            match qos {
                                QoS::AtLeastOnce => client.outgoing_pub.push_back(publish.clone()),
                                QoS::ExactlyOnce => client.outgoing_rec.push_back(publish.clone()),
                                _ => (),
                            }
                            client.send(Packet::Publish(publish.clone()));
                        }
                    }
                }
                Err(_) => {
                    warn!("Invalid topic in pubrel");
                    return;
                }
            }
        } else {
            warn!("Cannot handle PublishRelease({}) from unknown client {}", packet_id, client_id);
        }
    }

    fn handle_pubcomp(&mut self, client_id: &ClientId, packet_id: u16) {
        if let Some(ref mut client) = self.clients.get_mut(client_id) {
            client.handle_pubcomp(packet_id);
        } else {
            warn!("Cannot handle PublishComplete({}) from unknown client {}", packet_id, client_id);
        }
    }

    fn handle_pubrec(&mut self, client_id: &ClientId, packet_id: u16) {
        if let Some(ref mut _client) = self.clients.get_mut(client_id) {
            //let _publish = client.handle_pubrel(packet_id).expect("internal error");
            // TODO
        } else {
            warn!("Cannot handle PublishReceived({}) from unknown client {}", packet_id, client_id);
        }
    }

    fn handle_subscribe<'a>(&'a mut self, client_id: &'a ClientId, packet_id: u16, topic_filters: &'a mut Vec<(string::String<Bytes>, QoS)>) {
        if let Some(client) = self.clients.get_mut(client_id) {
            let mut status = Vec::new();
            let mut retains = Vec::new();
            for (topic, qos) in topic_filters.drain(..) {
                match std::str::from_utf8(topic.get_ref())
                    .map_err(|_| ())
                    .and_then(|s| Topic::from_str(s).map_err(|_| ()))
                {
                    Ok(topic) => {
                        for (retain_topic, retain_publish) in &self.retains {
                            match std::str::from_utf8(retain_topic.get_ref())
                                .map_err(|_| ())
                                .and_then(|s| Topic::from_str(s).map_err(|_| ()))
                            {
                                Ok(rt) => {
                                    if topic == rt {
                                        retains.push(retain_publish.clone());
                                    }
                                }
                                Err(_) => warn!("Invalid retain topic {}", retain_topic),
                            }
                        }

                        status.push(SubscribeReturnCode::Success(qos));
                        self.subscriptions.add_subscription(topic, client_id.clone(), qos);
                    }
                    Err(_) => status.push(SubscribeReturnCode::Failure),
                }
            }

            let packet = Packet::SubscribeAck { packet_id, status };

            client.send(packet);
            for publish in retains.drain(..) {
                client.publish(publish);
            }
        }
    }

    fn handle_unsubscribe<'a>(&'a mut self, client_id: &'a ClientId, packet_id: u16, _topic_filters: &'a mut Vec<string::String<Bytes>>) {
        if let Some(client) = self.clients.get_mut(client_id) {
            // TODO
            client.send(Packet::UnsubscribeAck { packet_id });
        }
    }

    /// Forward a publication to all subscribed clients
    fn forward<'a>(&'a mut self, publish: &'a Publish) {
        match std::str::from_utf8(publish.topic.get_ref())
            .map_err(|_| ())
            .and_then(|s| Topic::from_str(s).map_err(|_| ()))
        {
            Ok(topic) => {
                let subscriptions = self.subscriptions.get_subscriptions(&topic);
                for s in subscriptions {
                    if let Some(client) = self.clients.get_mut(&s.client_id) {
                        let mut publish = publish.clone();
                        if publish.qos as u8 > s.qos as u8 {
                            publish.qos = s.qos;
                        }
                        client.publish(publish.clone());
                    }
                }
            }
            Err(_) => {
                warn!("Invalid topic in publish");
                return;
            }
        }
    }

    fn handle_publish(&mut self, client_id: &ClientId, publish: &mut Publish) {
        if publish.retain {
            self.store_retain(publish.clone());
        }

        if let Some(client) = self.clients.get_mut(client_id) {
            client.handle_publish(publish);
        } else {
            warn!("Publish from unknown client {}", client_id);
        }

        self.forward(publish);
    }

    fn handle_disconnect(&mut self, addr: SocketAddr) {
        if let Some(client_id) = self.connections.remove(&addr) {
            if let Some(client) = self.clients.get_mut(&client_id) {
                client.connection.take();
                if client.clean_session {
                    self.clients.remove(&client_id);
                }
            }
            self.subscriptions.remove_client(&client_id);
            info!("{}: Disconnnected ({:?})", client_id, addr);
        }
    }

    fn handle_pingreq<'a>(&'a mut self, client_id: &'a ClientId) {
        if let Some(client) = self.clients.get_mut(client_id) {
            client.send(Packet::PingResponse);
        }
    }

    fn handle_client_connected(&mut self, addr: SocketAddr, connect: Connect, tx: mpsc::Sender<Packet>) {
        let id = ClientId::new(&connect.client_id);
        let session_present = if connect.clean_session {
            let client = Client::new(self.tx.clone(), tx, addr, connect.clean_session);
            self.clients.insert(id.clone(), client);
            false
        } else if let Some(mut client) = self.clients.get_mut(&id) {
            client.connection = Some(crate::client::Connection { tx, addr });
            client.clean_session = false;
            // TODO update session
            true
        } else {
            let id = ClientId::new(&connect.client_id);
            let client = Client::new(self.tx.clone(), tx, addr, connect.clean_session);
            self.clients.insert(id.clone(), client);
            false
        };
        self.connections.insert(addr, id.clone());

        let connack = Packet::ConnectAck {
            session_present,
            return_code: ConnectCode::ConnectionAccepted,
        };

        self.clients.get_mut(&id).map(|client| client.send(connack)).expect("TODO");
    }

    fn handle_connect(&mut self, addr: SocketAddr, stream: TcpStream) {
        let (sink, stream) = Framed::new(stream, mqtt_codec::Codec::new()).split();

        let broker = self.tx.clone();
        let broker_done = self.tx.clone();

        let client = stream
            .into_future()
            .and_then(move |(packet, stream)| {
                if let Some(Packet::Connect(connect)) = packet {
                    trace!("{} ⟹  {:?}", addr, connect);

                    let (client_tx, client_rx) = mpsc::channel(100);

                    broker
                        .clone()
                        .send(Event::ClientConnected(addr, connect, client_tx))
                        .wait()
                        .expect("Channel error");

                    // To client
                    let tx = broker.clone();
                    tokio::spawn(
                        client_rx
                            .map_err(|e| {
                                warn!("Error in client Receiver: {}", e);
                            })
                            .inspect(move |p| trace!("{} ⟸  {:?}", addr, p))
                            .forward(sink.sink_map_err(|e| warn!("Client rx error {:?}", e)))
                            .map(drop)
                            .then(move |r| {
                                info!("{} ⟹  Connection closed: {:?}", addr, r);
                                tx.send(Event::Disconnected(addr)).wait().expect("Channel error");
                                Ok(())
                            }),
                    );

                    // From client
                    let tx = broker.clone();
                    tokio::spawn(
                        stream
                            .inspect(move |p| trace!("{} ⟹  {:?}", addr, p))
                            .inspect_err(move |e| trace!("{} ⟹  {:?}", addr, e))
                            .map(move |p| Event::Packet(addr, p))
                            .or_else(move |e| {
                                info!("{} ⟹  Connection closed: {:?}", addr, e);
                                Ok::<_, ()>(Event::Disconnected(addr))
                            })
                            .forward(tx.sink_map_err(|e| trace!("Client Sink error {}", e)))
                            .map(move |_| {
                                info!("{} ⟹  Connection closed", addr);
                                broker_done.send(Event::Disconnected(addr)).wait().expect("Channel error");
                            }),
                    );
                } else {
                    warn!("{} ⟹  Unexpected packet: {:?}", addr, packet);
                }
                Ok(())
            })
            .map_err(drop)
            .map(drop);

        tokio::spawn(client);
    }

    fn store_retain(&mut self, publish: Publish) {
        if publish.payload.is_empty() {
            // Remove existing retains if new retain publish's payload len = 0
            self.retains.remove(&publish.topic);
        } else {
            self.retains.insert(publish.topic.clone(), publish);
        }
    }
}
