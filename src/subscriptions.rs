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

use crate::broker::ClientId;
use failure::Error;
use mqtt3::{SubscribeTopic, ToTopicPath, TopicPath};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct Subscriptions {
    concrete: HashMap<SubscribeTopic, HashSet<ClientId>>,
    wild: HashMap<SubscribeTopic, HashSet<ClientId>>,
}

impl Subscriptions {
    pub fn add_subscription(&mut self, topic: SubscribeTopic, client_id: ClientId) -> Result<(), Error> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;

        if topic_path.wildcards {
            let clients = self.wild.entry(topic).or_insert_with(HashSet::new);
            clients.insert(client_id);
        } else {
            let clients = self.concrete.entry(topic).or_insert_with(HashSet::new);
            clients.insert(client_id);
        }
        Ok(())
    }

    pub fn _remove_subscription_client(&mut self, topic: SubscribeTopic, client_id: &ClientId) -> Result<(), Error> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;

        if topic_path.wildcards {
            if let Some(clients) = self.wild.get_mut(&topic) {
                clients.remove(client_id);
                if clients.is_empty() {
                    self.wild.remove(&topic);
                }
            }
            self.wild.retain(|_, v| !v.is_empty());
        } else if let Some(clients) = self.concrete.get_mut(&topic) {
            clients.remove(client_id);
            self.concrete.retain(|_, v| !v.is_empty());
        }
        Ok(())
    }

    /// Remove a client from all the subscriptions
    pub fn remove_client(&mut self, client_id: &ClientId) {
        for (_, clients) in self.concrete.iter_mut() {
            clients.remove(client_id);
        }
        self.concrete.retain(|_, v| !v.is_empty());

        for (_, clients) in self.wild.iter_mut() {
            clients.remove(client_id);
        }
        self.wild.retain(|_, v| !v.is_empty());
    }

    /// For a given concrete topic, match topics & return list of subscribed clients
    pub fn get_subscribed_clients(&mut self, topic: SubscribeTopic) -> Result<Vec<ClientId>, Error> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;
        let qos = topic.qos;

        // subscription topic should only have concrete topic path
        drop(topic_path.to_topic_name()?);

        let mut all_clients = vec![];

        // O(1) matches from concrete hashmap
        if let Some(clients) = self.concrete.get(&topic) {
            all_clients.extend(clients.iter().cloned());
        }

        for (subscription, clients) in self.wild.iter() {
            let wild_subscription_topic = TopicPath::from_str(subscription.topic_path.clone())?;
            let wild_subscription_qos = subscription.qos;

            if wild_subscription_qos == qos && wild_subscription_topic.is_match(&topic_path) {
                all_clients.extend(clients.iter().cloned());
            }
        }

        Ok(all_clients)
    }
}
