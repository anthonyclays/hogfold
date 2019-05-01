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

use crate::{broker::Event, client_id::ClientId, error::Error};
use futures::{channel::mpsc, prelude::*};
use log::error;
use mqtt_codec::{Packet, Publish, QoS};
use std::collections::VecDeque;

#[derive(Debug)]
pub struct Connection {
    tx: mpsc::Sender<Packet>,
}

impl Connection {
    pub fn new(tx: mpsc::Sender<Packet>) -> Connection {
        Connection { tx }
    }
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
pub(crate) struct Client {
    pub id: ClientId,
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

impl<'a> Client {
    pub fn new(id: ClientId, broker: mpsc::Sender<Event>, clean_session: bool, connection: Connection) -> Client {
        Client {
            id,
            connection: Some(connection),
            clean_session,
            last_packet_id: 0,
            broker,
            outgoing_pub: VecDeque::new(),
            outgoing_rec: VecDeque::new(),
            outgoing_rel: VecDeque::new(),
            outgoing_comp: VecDeque::new(),
        }
    }

    pub(crate) async fn send(&'a mut self, packet: Packet) -> Result<(), Error> {
        if let Some(ref mut connection) = self.connection {
            await!(connection.tx.send(packet)).map_err(|_| Error::ClientChannel).map(drop)
        } else {
            Ok(())
        }
    }

    pub async fn send_publish(&'a mut self, mut publish: Publish) -> Result<(), Error> {
        publish.packet_id = if publish.qos == QoS::AtMostOnce { None } else { Some(self.next_pkid()) };

        match publish.qos {
            QoS::AtMostOnce => (),
            QoS::AtLeastOnce => self.outgoing_pub.push_back(publish.clone()),
            QoS::ExactlyOnce => unimplemented!(),
        }
        let packet = Packet::Publish(publish);
        await!(self.send(packet))
    }

    pub async fn publish(&'a mut self, publish: &'a Publish) -> Result<(), Error> {
        match publish.qos {
            QoS::AtMostOnce => Ok(()),
            QoS::AtLeastOnce => {
                if let Some(pkid) = publish.packet_id {
                    await!(self.send(Packet::PublishAck { packet_id: pkid }))
                } else {
                    error!("Ignoring publish packet. No pkid for QoS1 packet");
                    Err(Error::MissingPacketId)
                }
            }
            QoS::ExactlyOnce => unimplemented!(),
        }
    }

    pub fn publish_ack(&mut self, packet_id: u16) -> Result<(), Error> {
        self.outgoing_pub.remove_key(&packet_id);
        Ok(())
    }

    pub fn _pubrel(&mut self, packet_id: u16) -> Result<(), Error> {
        self.outgoing_rec.remove_key(&packet_id);
        Ok(())
    }

    pub fn _pubcomp(&mut self, packet_id: u16) -> Result<(), Error> {
        self.outgoing_rel.remove_key(&packet_id);
        Ok(())
    }

    fn next_pkid(&mut self) -> u16 {
        self.last_packet_id = self.last_packet_id.overflowing_add(1).0;
        self.last_packet_id
    }
}
