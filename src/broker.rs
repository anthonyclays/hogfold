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
use futures::stream::Stream;
use hashbrown::HashMap;
use log::{info, trace, warn};
use mqtt_codec::{QoS, *};
use std::str::FromStr;
use std::time::Duration;
use stream_cancel::Valved;
use tokio::{codec::Framed, net::TcpStream, prelude::*, runtime::Runtime, sync::mpsc};

type TopicString = string::String<Bytes>;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    ClientConnected(Connect, ClientId, Connection),
    Connection(TcpStream),
    Disconnected(ClientId),
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

impl Broker {
    pub fn start(runtime: &mut Runtime) -> (mpsc::Sender<Event>, mpsc::Receiver<Notification>) {
        let (tx_event, rx_event) = mpsc::channel::<Event>(100);
        let (tx_notification, rx_notification) = mpsc::channel::<Notification>(10);

        let mut broker = Broker {
            event_tx: tx_event.clone(),
            notification_tx: tx_notification,
            clients: HashMap::new(),
            subscriptions: Subscriptions::default(),
            retains: HashMap::new(),
        };

        runtime.spawn(
            rx_event
                .for_each(move |event| {
                    broker.on_event(event);
                    Ok(())
                })
                .map_err(drop)
                .map(drop),
        );
        (tx_event, rx_notification)
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Connection(stream) => self.tcp_connection(stream),
            Event::ClientConnected(connect, client_id, connection) => {
                self.client(connect, client_id, connection);
            }
            Event::Packet(client_id, packet) => {
                let result = match packet {
                    Packet::Subscribe {
                        packet_id,
                        ref topic_filters,
                    } => self.subscribe(&client_id, packet_id, &topic_filters),
                    Packet::Unsubscribe {
                        packet_id,
                        ref topic_filters,
                    } => self.unsubscribe(&client_id, packet_id, &topic_filters),
                    Packet::Publish(ref publish) => self.publish(Some(&client_id), publish),
                    Packet::PublishAck { packet_id } => self.publish_ack(&client_id, packet_id),
                    Packet::PublishRelease { packet_id } => self.pubrel(&client_id, packet_id),
                    Packet::PublishComplete { packet_id } => self.pubcomp(&client_id, packet_id),
                    Packet::PublishReceived { packet_id } => self.pubrec(&client_id, packet_id),
                    Packet::PingRequest => self.pingreq(&client_id),
                    Packet::Connect(_) | Packet::Disconnect => {
                        // The connect packet is handled before. This is a Connect received on a
                        // established connection -> disconnect client
                        self.disconnect(&client_id);
                        Ok(())
                    }
                    p => unimplemented!("Unimplemented packet received: {:?}", p),
                };

                match result {
                    Ok(_) => (),
                    Err(Error::InvalidTopic) => {
                        warn!("Client {} used a invalid topic. Disconnecting", client_id);
                        self.disconnect(&client_id);
                    }
                    Err(Error::UnknownClient(_)) => {}
                    Err(Error::ClientChannel(_)) => {
                        warn!("Client {} channel sending error. Disconnecting", client_id);
                        self.disconnect(&client_id);
                    }
                    Err(Error::MissingPacketId) => {
                        warn!("Required packet id missing in packet from {}. Disconnecting", client_id);
                        self.disconnect(&client_id);
                    }
                }
                if let Err(e) = result {
                    warn!("{:?}", e);
                }
            }
            Event::Disconnected(client_id) => self.disconnect(&client_id),
            Event::Publish(topic, qos, retain, payload) => {
                let result = self.publish(
                    None,
                    &Publish {
                        dup: false,
                        retain,
                        qos: QoS::from(qos),
                        topic: string::String::from_str(&topic),
                        packet_id: None,
                        payload,
                    },
                );
                // TODO
                drop(result);
            }
        };
    }

    fn subscribe(&mut self, client_id: &ClientId, packet_id: u16, topic_filters: &[(TopicString, QoS)]) -> Result<(), Error> {
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

        let suback = Packet::SubscribeAck { packet_id, status };
        client.send(suback)?;

        for publish in retains.drain(..) {
            // TODO: Disconnect client on publish error
            client.send_publish(publish)?;
        }

        self.notification_tx
            .clone()
            .send(Notification::Subscriptions(self.subscriptions.clone()))
            .wait()
            .ok(); // Ignore error if receiver is dropped
        Ok(())
    }

    fn unsubscribe(&mut self, client_id: &ClientId, packet_id: u16, _topic_filters: &[TopicString]) -> Result<(), Error> {
        self.clients
            .get_mut(&client_id)
            .ok_or_else(|| Error::UnknownClient(client_id.clone()))
            .and_then(|client| client.send(Packet::UnsubscribeAck { packet_id }))?;

        // TODO

        // TODO: Disconnect client on publish error

        self.notification_tx
            .clone()
            .send(Notification::Subscriptions(self.subscriptions.clone()))
            .wait()
            .ok();

        Ok(())
    }

    fn publish(&mut self, client_id: Option<&ClientId>, publish: &Publish) -> Result<(), Error> {
        if let Some(client_id) = client_id {
            self.clients
                .get_mut(&client_id)
                .ok_or_else(|| Error::UnknownClient(client_id.clone()))
                .and_then(|client| client.publish(publish))?;
        }

        if publish.retain {
            if publish.payload.is_empty() {
                // Remove existing retains if new retain publish's payload len = 0
                self.retains.remove(&publish.topic);
            } else {
                self.retains.insert(publish.topic.clone(), publish.clone());
            }
        }

        self.forward(publish)
    }

    fn publish_ack(&mut self, client_id: &ClientId, packet_id: u16) -> Result<(), Error> {
        self.clients
            .get_mut(&client_id)
            .ok_or_else(|| Error::UnknownClient(client_id.clone()))
            .and_then(|client| client.publish_ack(packet_id))
    }

    fn pubrel(&mut self, _client_id: &ClientId, _packet_id: u16) -> Result<(), Error> {
        unimplemented!()
        // if let Some(ref mut client) = self.clients.get_mut(client_id) {
        //     let publish = client.pubrel(packet_id).expect("internal error")?;
        //     let qos = *publish.qos;

        //     match std::str::from_utf8(publish.topic.get_ref())
        //         .map_err(drop)
        //         .and_then(|s| Topic::from_str(s).map_err(drop))
        //     {
        //         Ok(topic) => {
        //             let subscriptions = self.subscriptions.get_subscriptions(&topic);
        //             for s in subscriptions {
        //                 if let Some(client) = self.clients.get_mut(&s.client_id) {
        //                     match qos {
        //                         QoS::AtLeastOnce => client.outgoing_pub.push_back(publish.clone()),
        //                         QoS::ExactlyOnce => client.outgoing_rec.push_back(publish.clone()),
        //                         _ => (),
        //                     }
        //                     client.send(Packet::Publish(publish.clone()));
        //                 }
        //             }
        //         }
        //         Err(_) => {
        //             warn!("Invalid topic in pubrel");
        //             return;
        //         }
        //     }
        // } else {
        //     warn!("Cannot handle PublishRelease({}) from unknown client {}", packet_id, client_id);
        // }
    }

    fn pubcomp(&mut self, _client_id: &ClientId, _packet_id: u16) -> Result<(), Error> {
        unimplemented!()
    }

    fn pubrec(&mut self, _client_id: &ClientId, _packet_id: u16) -> Result<(), Error> {
        unimplemented!()
        // TODO
        // if let Some(client) = self.clients.get_mut(client_id) {
        //     let _publish = client.pubrel(packet_id)?;
        //     unimplemented!()
        // } else {
        //     Err(Error::UnknownClient(client_id.clone()))
        // }
    }

    fn pingreq(&mut self, client_id: &ClientId) -> Result<(), Error> {
        self.clients
            .get_mut(&client_id)
            .ok_or_else(|| Error::UnknownClient(client_id.clone()))
            .and_then(|client| client.send(Packet::PingResponse))
    }

    fn disconnect(&mut self, client_id: &ClientId) {
        // TODO: clean session false
        self.clients.remove(&client_id);
        self.subscriptions.remove_client(&client_id);

        self.notification_tx
            .clone()
            .send(Notification::Subscriptions(self.subscriptions.clone()))
            .wait()
            .ok();
        info!("{}: Disconnnected", client_id);
    }

    /// Forward a publication to all subscribed clients
    fn forward<'a>(&'a mut self, publish: &'a Publish) -> Result<(), Error> {
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
                        client.send_publish(publish)?;
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

    fn client(&mut self, connect: Connect, client_id: ClientId, connection: Connection) {
        info!("{}: Connected", client_id);
        if connect.clean_session {
            let mut client = Client::new(client_id.clone(), self.event_tx.clone(), true, connection);
            let connack = Packet::ConnectAck {
                session_present: false,
                return_code: ConnectCode::ConnectionAccepted,
            };

            match client.send(connack) {
                Ok(()) => {
                    self.clients.insert(client_id, client);
                }
                Err(_) => drop(client),
            }
        } else {
            unimplemented!("clean session false");
        }
    }

    fn tcp_connection(&mut self, stream: TcpStream) {
        let (sink, stream) = Framed::new(stream, mqtt_codec::Codec::new()).split();

        let broker = self.event_tx.clone();

        let client = stream
            .into_future()
            .and_then(move |(packet, stream)| {
                if let Some(Packet::Connect(connect)) = packet {
                    trace!("{:?}", connect);

                    let (client_tx, client_rx) = mpsc::channel(100);
                    let (stop_stream, stream) = Valved::new(stream);
                    let (stop_rx, client_rx) = Valved::new(client_rx);

                    let client_id = ClientId::new(&connect.client_id);
                    let connection = Connection::new(client_tx, [stop_stream, stop_rx]);
                    // Keep alive in seconds
                    let keep_alive = connect.keep_alive;

                    let connect = broker
                        .clone()
                        .send(Event::ClientConnected(connect, client_id.clone(), connection))
                        .map_err(drop);

                    // Outbound
                    let id = client_id.clone();
                    let outbound = client_rx
                        .inspect(move |p| trace!("{} ← {:?}", id, p))
                        .map_err(drop)
                        .forward(sink.sink_map_err(|e| warn!("{:?}", e)))
                        .map(drop);

                    // Inbound
                    #[derive(Debug)]
                    enum StreamError {
                        Mqtt(mqtt_codec::ParseError),
                        Timeout,
                        Timer,
                    }

                    let inbound = if keep_alive == 0 {
                        EitherStream::A(stream.map_err(StreamError::Mqtt))
                    } else {
                        // [MQTT-3.1.2-24] keep_alive * 1.5
                        let timeout = Duration::from_millis(u64::from(keep_alive) * 1000 * 3 / 2);
                        EitherStream::B(stream.timeout(timeout).map_err(|e| {
                            if e.is_inner() {
                                StreamError::Mqtt(e.into_inner().unwrap())
                            } else if e.is_elapsed() {
                                StreamError::Timeout
                            } else {
                                StreamError::Timer
                            }
                        }))
                    };

                    let id = client_id.clone();
                    let id_err = client_id.clone();
                    let disconnect = broker.clone().send(Event::Disconnected(client_id));
                    let inbound = inbound
                        .map(move |p| {
                            trace!("{} → {:?}", id, p);
                            Event::Packet(id.clone(), p)
                        })
                        .or_else(move |e| {
                            // Error on connection
                            trace!("{}:  {:?}", id_err, e);
                            Ok::<_, ()>(Event::Disconnected(id_err.clone()))
                        })
                        .forward(broker.sink_map_err(|e| trace!("Client Sink error {}", e)))
                        .then(move |_| {
                            // Normal termination
                            disconnect.wait().map_err(drop).map(drop)
                        });

                    tokio::spawn(
                        connect
                            .and_then(|_| {
                                tokio::spawn(inbound);
                                tokio::spawn(outbound);
                                Ok(())
                            })
                    );
                } else {
                    warn!("Unexpected packet from: {:?}", packet);
                    // Dropping stream closes the connection
                }
                Ok(())
            })
            .map_err(|e| warn!("Client stream error: {:?}", e))
            .map(drop);

        tokio::spawn(client);
    }
}

#[derive(Debug)]
pub enum EitherStream<A, B> {
    A(A),
    B(B),
}

impl<A, B> Stream for EitherStream<A, B>
where
    A: Stream,
    B: Stream<Item = A::Item, Error = A::Error>,
{
    type Item = A::Item;
    type Error = A::Error;

    fn poll(&mut self) -> Poll<Option<A::Item>, A::Error> {
        match *self {
            EitherStream::A(ref mut a) => a.poll(),
            EitherStream::B(ref mut b) => b.poll(),
        }
    }
}
