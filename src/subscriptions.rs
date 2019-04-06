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

use crate::client_id::ClientId;
use failure::Error;
use hashbrown::{HashMap, HashSet};
use mqtt_codec::*;
use std::{
    cmp,
    hash::{Hash, Hasher},
};

#[derive(Debug, Default)]
pub struct Subscriptions(HashMap<Topic, HashSet<Subscription>>);

#[derive(Debug, Clone)]
pub struct Subscription {
    pub client_id: ClientId,
    pub qos: QoS,
}

impl Hash for Subscription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.client_id.hash(state);
    }
}

impl cmp::PartialEq for Subscription {
    fn eq(&self, other: &Subscription) -> bool {
        // Compare only client_id
        self.client_id == other.client_id
    }
}

impl Eq for Subscription {}

impl Subscriptions {
    pub fn add_subscription(&mut self, topic: Topic, client_id: ClientId, qos: QoS) {
        let clients = self.0.entry(topic).or_insert_with(HashSet::new);
        let subscription = Subscription { client_id, qos };
        clients.insert(subscription);
    }

    pub fn _remove_subscription(&mut self, topic: &Topic, client_id: &ClientId) -> Result<(), Error> {
        if let Some(clients) = self.0.get_mut(topic) {
            clients.retain(|s| &s.client_id != client_id);
            if clients.is_empty() {
                self.0.remove(&topic);
            }
        }
        self.0.retain(|_, v| !v.is_empty());
        Ok(())
    }

    /// Remove a client from all the subscriptions
    pub fn remove_client(&mut self, client_id: &ClientId) {
        for (_, clients) in self.0.iter_mut() {
            clients.retain(|s| &s.client_id != client_id);
        }
        self.0.retain(|_, v| !v.is_empty());
    }

    /// For a given concrete topic, match topics & return list of subscribed clients
    pub fn get_subscriptions(&mut self, topic: &Topic) -> Vec<Subscription> {
        self.0.iter().filter(|(t, _)| t == &topic).flat_map(|(_, s)| s).cloned().collect()
    }
}
