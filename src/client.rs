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

use crate::broker::Event;
use log::error;
use mqtt_codec::{Packet, Publish, QoS};
use std::{collections::VecDeque, net::SocketAddr};
use tokio::{prelude::*, sync::mpsc};

#[derive(Clone, Debug)]
pub struct Connection {
    pub tx: mpsc::Sender<Packet>,
    pub addr: SocketAddr,
}

trait Matches<T> {
    fn matches(&self, rhs: &T) -> bool;
}

impl Matches<u16> for u16 {
    fn matches(&self, rhs: &u16) -> bool {
        self == rhs
    }
}

impl Matches<u16> for Publish {
    fn matches(&self, rhs: &u16) -> bool {
        self.packet_id.map(|ref id| id == rhs).unwrap_or(false)
    }
}

trait Queue<T, K> {
    fn remove_key(&mut self, key: &K) -> Option<T>;
}

impl<T, K> Queue<T, K> for VecDeque<T>
where
    T: Matches<K>,
{
    fn remove_key(&mut self, key: &K) -> Option<T> {
        if let Some(index) = self.iter().position(|ref x| x.matches(key)) {
            self.remove(index)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Client {
    pub connection: Option<Connection>,
    pub clean_session: bool,
    last_packet_id: u16,
    broker: mpsc::Sender<Event>,

    /// For QoS 1. Stores outgoing publishes
    pub outgoing_pub: VecDeque<Publish>,
    /// For QoS 2. Stores outgoing publishes
    pub outgoing_rec: VecDeque<Publish>,
    /// For QoS 2. Stores outgoing release
    pub outgoing_rel: VecDeque<u16>,
    /// For QoS 2. Stores outgoing comp
    pub outgoing_comp: VecDeque<u16>,
}

impl Client {
    pub fn new(broker: mpsc::Sender<Event>, tx: mpsc::Sender<Packet>, addr: SocketAddr, clean_session: bool) -> Client {
        Client {
            connection: Some(Connection { tx, addr }),
            clean_session,
            last_packet_id: 0,
            broker,
            outgoing_pub: VecDeque::new(),
            outgoing_rec: VecDeque::new(),
            outgoing_rel: VecDeque::new(),
            outgoing_comp: VecDeque::new(),
        }
    }

    pub fn send(&mut self, packet: Packet) {
        if let Some(ref connection) = self.connection {
            let send = connection.tx.clone().send(packet);
            if send.wait().is_err() {
                if self.clean_session {
                    // TODO
                } else {
                    self.connection = None;
                }
            }
        }
    }

    pub fn publish(&mut self, mut publish: Publish) {
        publish.packet_id = if publish.qos == QoS::AtMostOnce { None } else { Some(self.next_pkid()) };

        match publish.qos {
            QoS::AtLeastOnce => self.outgoing_pub.push_back(publish.clone()),
            QoS::ExactlyOnce => (),
            _ => (),
        }
        let packet = Packet::Publish(publish);
        self.send(packet);
    }

    pub fn handle_publish(&mut self, publish: &Publish) {
        match publish.qos {
            QoS::AtMostOnce => (),
            QoS::AtLeastOnce => {
                if let Some(pkid) = publish.packet_id {
                    self.send(Packet::PublishAck { packet_id: pkid });
                } else {
                    error!("Ignoring publish packet. No pkid for QoS1 packet");
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_id) = publish.packet_id {
                    self.outgoing_rec.push_back(publish.clone());
                    self.send(Packet::PublishReceived { packet_id });
                } else {
                    error!("Ignoring record packet. No pkid for QoS2 packet");
                }
            }
        };
    }

    pub fn handle_puback(&mut self, packet_id: u16) {
        self.outgoing_pub.remove_key(&packet_id);
    }

    pub fn handle_pubrel(&mut self, packet_id: u16) -> Option<Publish> {
        self.send(Packet::PublishComplete { packet_id });

        self.outgoing_rec.remove_key(&packet_id)
    }

    pub fn handle_pubcomp(&mut self, packet_id: u16) {
        self.outgoing_rel.remove_key(&packet_id);
    }

    fn next_pkid(&mut self) -> u16 {
        self.last_packet_id = self.last_packet_id.overflowing_add(1).0;
        self.last_packet_id
    }
}
