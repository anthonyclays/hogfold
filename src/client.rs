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
use mqtt3::{Packet, PacketIdentifier, Publish, QoS};
use std::{collections::VecDeque, net::SocketAddr};
use tokio::{prelude::*, sync::mpsc};

#[derive(Clone)]
struct Connection(mpsc::Sender<Packet>, SocketAddr);

pub struct Client {
    connection: Option<Connection>,
    last_pkid: u16,
    broker: mpsc::Sender<Event>,

    outgoing_pub: VecDeque<Publish>,
    outgoing_rec: VecDeque<Publish>,
}

impl Client {
    pub fn new(broker: mpsc::Sender<Event>, tx: mpsc::Sender<Packet>, addr: SocketAddr) -> Client {
        Client {
            connection: Some(Connection(tx, addr)),
            last_pkid: 0,
            broker,
            outgoing_pub: VecDeque::new(),
            outgoing_rec: VecDeque::new(),
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        if let Some(ref connection) = self.connection {
            let mut connection = connection.clone();
            let send = connection.0.send_async(packet);
            if await!(send).is_err() {
                self.connection = None;
                await!(self.broker.send_async(Event::Disconnected(connection.1))).ok();
            }
        }
    }

    pub async fn publish<'a>(&'a mut self, mut publish: Publish) {
        publish.pid = if publish.qos == QoS::AtMostOnce { None } else { Some(self.next_pkid()) };

        match publish.qos {
            QoS::AtLeastOnce => self.outgoing_pub.push_back(publish.clone()),
            QoS::ExactlyOnce => self.outgoing_rec.push_back(publish.clone()),
            _ => (),
        }
        let packet = Packet::Publish(publish);
        await!(self.send(packet));
    }

    pub fn handle_puback(&mut self, pkid: PacketIdentifier) {
        if let Some(index) = self.outgoing_pub.iter().position(|x| x.pid == Some(pkid)) {
            self.outgoing_pub.remove(index);
        }
    }

    fn next_pkid(&mut self) -> PacketIdentifier {
        self.last_pkid = self.last_pkid.overflowing_add(1).0;
        PacketIdentifier(self.last_pkid)
    }
}
