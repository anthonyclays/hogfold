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

use crate::{
    client::{Client, Connection},
    client_id::ClientId,
    error::Error,
    subscriptions::Subscriptions,
};
use bytes::Bytes;
use futures::{
    channel::mpsc,
    compat::{Sink01CompatExt, Stream01CompatExt},
    future::{FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{self, StreamExt},
};
use hashbrown::HashMap;
use log::{error, info, trace, warn};
use mqtt_codec::{QoS, *};
use std::str::FromStr;
use tokio::{codec::Framed, net::TcpStream, prelude::Stream};

type TopicString = string::String<Bytes>;

// TODO: Split this into the public and internal part
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    ClientConnected(Connect, ClientId, Connection),
    Connection(TcpStream), // TODO: This should be a Stream and Sink pair
    Disconnected(ClientId, Result<(), Error>),
    Packet(ClientId, Packet),
    Publish(String, u8, bool, Bytes),
}

#[derive(Debug)]
pub enum Notification {
    Subscriptions(Subscriptions),
}

#[derive(Debug)]
pub struct Broker {
    event_tx: mpsc::Sender<Event>,
    notification_tx: mpsc::Sender<Notification>,
    clients: HashMap<ClientId, Client>,
    subscriptions: Subscriptions,
    retains: HashMap<TopicString, Publish>,
}

impl<'a> Broker {
    pub async fn start() -> (mpsc::Sender<Event>, mpsc::Receiver<Notification>) {
        let (tx_event, mut rx_event) = mpsc::channel::<Event>(100);
        let (tx_notification, rx_notification) = mpsc::channel::<Notification>(10);

        let mut broker = Broker {
            event_tx: tx_event.clone(),
            notification_tx: tx_notification,
            clients: HashMap::new(),
            subscriptions: Subscriptions::default(),
            retains: HashMap::new(),
        };

        let broker_loop = async move {
            while let Some(event) = await!(rx_event.next()) {
                await!(broker.on_event(event));
            }
        };
        tokio::spawn(broker_loop.unit_error().boxed().compat());

        (tx_event, rx_notification)
    }

    async fn on_event(&'a mut self, event: Event) {
        match event {
            Event::Connection(stream) => await!(self.connection(stream)),
            Event::ClientConnected(connect, client_id, connection) => {
                await!(self.client(connect, client_id, connection));
            }
            Event::Packet(client_id, packet) => {
                let result = match packet {
                    Packet::Subscribe {
                        packet_id,
                        ref topic_filters,
                    } => await!(self.subscribe(&client_id, packet_id, &topic_filters)),
                    Packet::Unsubscribe {
                        packet_id,
                        ref topic_filters,
                    } => await!(self.unsubscribe(&client_id, packet_id, &topic_filters)),
                    Packet::Publish(ref publish) => await!(self.publish(Some(&client_id), publish)),
                    Packet::PublishAck { packet_id } => await!(self.publish_ack(&client_id, packet_id)),
                    Packet::PublishRelease { packet_id } => await!(self.pubrel(&client_id, packet_id)),
                    Packet::PublishComplete { packet_id } => await!(self.pubcomp(&client_id, packet_id)),
                    Packet::PublishReceived { packet_id } => await!(self.pubrec(&client_id, packet_id)),
                    Packet::PingRequest => await!(self.pingreq(&client_id)),
                    Packet::Disconnect => {
                        await!(self.disconnect(&client_id, Ok(())));
                        Ok(())
                    }
                    Packet::Connect(_) => {
                        // T,,ehe connect packet is handled before. This is a Connect received on a
                        // established connection -> disconnect client
                        await!(self.disconnect(&client_id, Err(Error::DuplicateConnect)));
                        Ok(())
                    }
                    p => unimplemented!("Unimplemented packet received: {:?}", p),
                };

                match result {
                    Ok(_) => (),
                    Err(Error::UnknownClient(_)) => warn!("Unknown client - this is probably a bug"),
                    Err(_) => await!(self.disconnect(&client_id, result)),
                }
            }
            Event::Disconnected(client_id, result) => await!(self.disconnect(&client_id, result)),
            Event::Publish(topic, qos, retain, payload) => {
                let publish = Publish {
                    dup: false,
                    retain,
                    qos: QoS::from(qos),
                    topic: string::String::from_str(&topic),
                    packet_id: None,
                    payload,
                };
                let result = self.publish(None, &publish);
                // TODO
                drop(result);
            }
        };
    }

    async fn subscribe(&'a mut self, client_id: &'a ClientId, packet_id: u16, topic_filters: &'a [(TopicString, QoS)]) -> Result<(), Error> {
        let mut status = Vec::new();
        let mut retains = Vec::new();

        for (topic, qos) in topic_filters {
            if qos == &QoS::ExactlyOnce {
                unimplemented!("QOS 2")
            }

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

                    status.push(SubscribeReturnCode::Success(*qos));
                    self.subscriptions.add_subscription(topic, client_id.clone(), *qos);
                }
                Err(_) => status.push(SubscribeReturnCode::Failure),
            }
        }

        let client = self.clients.get_mut(&client_id).ok_or_else(|| Error::UnknownClient(client_id.clone()))?;

        // Send SubscribeAck
        let suback = Packet::SubscribeAck { packet_id, status };
        await!(client.send(suback))?;

        // Send retains
        for publish in retains.drain(..) {
            await!(client.send_publish(publish))?;
        }

        await!(self.notification(Notification::Subscriptions(self.subscriptions.clone())));

        Ok(())
    }

    async fn unsubscribe(&'a mut self, client_id: &'a ClientId, packet_id: u16, _topic_filters: &'a [TopicString]) -> Result<(), Error> {
        let client = self.clients.get_mut(&client_id).ok_or_else(|| Error::UnknownClient(client_id.clone()))?;
        await!(client.send(Packet::UnsubscribeAck { packet_id }))?;

        // TODO

        await!(self.notification(Notification::Subscriptions(self.subscriptions.clone())));

        Ok(())
    }

    async fn publish(&'a mut self, client_id: Option<&'a ClientId>, publish: &'a Publish) -> Result<(), Error> {
        if let Some(client_id) = client_id {
            let client = self.clients.get_mut(&client_id).ok_or_else(|| Error::UnknownClient(client_id.clone()))?;
            await!(client.publish(publish))?;
        }

        if publish.retain {
            if publish.payload.is_empty() {
                // Remove existing retains if new retain publish's payload len = 0
                self.retains.remove(&publish.topic);
            } else {
                self.retains.insert(publish.topic.clone(), publish.clone());
            }
        }

        await!(self.forward(publish))
    }

    async fn publish_ack(&'a mut self, client_id: &'a ClientId, packet_id: u16) -> Result<(), Error> {
        self.clients
            .get_mut(&client_id)
            .ok_or_else(|| Error::UnknownClient(client_id.clone()))
            .and_then(|client| client.publish_ack(packet_id))
    }

    async fn pubrel(&'a mut self, _client_id: &'a ClientId, _packet_id: u16) -> Result<(), Error> {
        unimplemented!()
    }

    async fn pubcomp(&'a mut self, _client_id: &'a ClientId, _packet_id: u16) -> Result<(), Error> {
        unimplemented!()
    }

    async fn pubrec(&'a mut self, _client_id: &'a ClientId, _packet_id: u16) -> Result<(), Error> {
        unimplemented!()
    }

    async fn pingreq(&'a mut self, client_id: &'a ClientId) -> Result<(), Error> {
        let client = self.clients.get_mut(&client_id).ok_or_else(|| Error::UnknownClient(client_id.clone()))?;
        await!(client.send(Packet::PingResponse))
    }

    async fn disconnect(&'a mut self, client_id: &'a ClientId, result: Result<(), Error>) {
        match result {
            Ok(_) => (),
            Err(Error::InvalidTopic) => {
                warn!("Client {} used a invalid topic. Disconnecting", client_id);
            }
            Err(Error::ClientChannel) => {
                warn!("Client {} channel sending error. Disconnecting", client_id);
            }
            Err(Error::MissingPacketId) => {
                warn!("Required packet id missing in packet from {}. Disconnecting", client_id);
            }
            Err(e) => warn!("Client {} error: {:?}", client_id, e),
        }

        // TODO: clean session false
        self.clients.remove(&client_id);
        self.subscriptions.remove_client(&client_id);

        await!(self.notification(Notification::Subscriptions(self.subscriptions.clone())));
    }

    /// Forward a publication to all subscribed clients
    async fn forward(&'a mut self, publish: &'a Publish) -> Result<(), Error> {
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
                        await!(client.send_publish(publish))?;
                    }
                }
                Ok(())
            }
            Err(_) => {
                warn!("Invalid topic in publish");
                Err(Error::InvalidTopic)
            }
        }
    }

    async fn notification(&'a self, _notification: Notification) {
        // TODO
        // await!(self.notification_tx
        //     .clone()
        //     .send(notification)
        //     .compat());
    }

    async fn client(&'a mut self, connect: Connect, client_id: ClientId, connection: Connection) {
        info!("{}: Connected", client_id);
        if connect.clean_session {
            let mut client = Client::new(client_id.clone(), self.event_tx.clone(), true, connection);
            let connack = Packet::ConnectAck {
                session_present: false,
                return_code: ConnectCode::ConnectionAccepted,
            };

            match await!(client.send(connack)) {
                Ok(()) => drop(self.clients.insert(client_id, client)),
                Err(_) => drop(client),
            }
        } else {
            unimplemented!("clean session false");
        }
    }

    async fn connection(&'a mut self, stream: TcpStream) {
        let mut broker = self.event_tx.clone();
        let client = async move {
            #[derive(Debug)]
            #[allow(clippy::large_enum_variant)]
            enum Forward {
                Inbound(Result<Packet, mqtt_codec::ParseError>),
                Outbound(Packet),
                _Timeout,
            }

            // Connection Stream and Sink
            let (sink, stream) = Framed::new(stream, mqtt_codec::Codec::new()).split();

            // TODO: Timeout for receiving CONNECT packet

            // Client Stream and Sink
            let (client_tx, client_rx) = mpsc::channel(100);
            let mut stream = stream.compat();

            // First packet on packets must be a inbound CONNECT
            if let Ok(Packet::Connect(connect)) = await!(stream.next()).unwrap() {
                trace!("{:?}", connect);
                let id = ClientId::new(&connect.client_id);
                // Keep alive in seconds
                let keep_alive = connect.keep_alive;

                if await!(broker.send(Event::ClientConnected(connect, id.clone(), Connection::new(client_tx)))).is_err() {
                    // TODO
                    error!("Internal broker channel error");
                    return;
                }

                let stream = if keep_alive == 0 {
                    futures::future::Either::Left(stream)
                } else {
                    // TODO
                    // // [MQTT-3.1.2-24] keep_alive * 1.5
                    // let timeout = Duration::from_millis(u64::from(keep_alive) * 1000 * 3 / 2);
                    // let stream = tokio::timer::timeout::Timeout::new(stream, timeout);
                    // unreachable!()
                    // let stream = stream.then(|a| {
                    //     match a {
                    //         Ok(v) => Ok(Forward::Inbound(Ok(v))),
                    //         Err(e) => {
                    //             if e.is_inner() {
                    //                 Err(e.inner())
                    //             } else {
                    //                 Ok(Forward::Timeout)
                    //             }
                    //         }
                    //     }
                    // });
                    futures::future::Either::Right(stream)
                };

                let mut sink = sink.sink_compat();
                let tx = client_rx.map(Forward::Outbound);
                let rx = stream.map(Forward::Inbound);
                let mut packets = stream::select(tx, rx);

                loop {
                    match await!(packets.next()) {
                        Some(Forward::_Timeout) => {
                            trace!("{} → Timeout", id);
                            await!(broker.send(Event::Disconnected(id.clone(), Err(Error::PacketTimeout)))).ok();
                            break;
                        }
                        Some(Forward::Inbound(Ok(p))) => {
                            trace!("{} → {:?}", id, p);
                            let p = Event::Packet(id.clone(), p);
                            if await!(broker.send(p)).is_err() {
                                break;
                            }
                        }
                        Some(Forward::Outbound(p)) => {
                            trace!("{} ← {:?}", id, p);
                            match await!(sink.send(p)) {
                                Ok(_) => trace!("{} ← Success", id),
                                Err(e) => {
                                    warn!("{} ← Error: {:?}", id, e);
                                    await!(broker.send(Event::Disconnected(id.clone(), Err(Error::Protocol(e))))).ok();
                                    break;
                                }
                            }
                        }
                        None => {
                            info!("{}:  Connection closed", id);
                            await!(broker.send(Event::Disconnected(id.clone(), Ok(())))).ok();
                            break;
                        }
                        e => {
                            warn!("{}:  Connection error: {:?}", id, e);
                            await!(broker.send(Event::Disconnected(id.clone(), Ok(())))).ok(); // TODO: Error
                            break;
                        }
                    }
                }
            } else {
                warn!("Received something else than connect on {:?}", stream);
                return;
            }
        };
        tokio::spawn(client.unit_error().boxed().compat());
    }
}
