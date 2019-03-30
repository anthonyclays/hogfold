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

#![feature(await_macro, async_await, futures_api)]

use log::info;
use std::{env, net::SocketAddr};
use tokio::{await, net::TcpListener, prelude::*};
mod codec;

mod broker;
mod client;
mod subscriptions;

fn main() {
    env::set_var("RUST_LOG", "hogfold=debug");
    env_logger::init();

    let addr = "[::1]:1883".parse::<SocketAddr>().unwrap();
    info!("Binding {}", addr);
    let listener = TcpListener::bind(&addr).unwrap();

    tokio::run_async(async {
        let mut broker = broker::Broker::start();
        let mut incoming = listener.incoming();

        while let Some(stream) = await!(incoming.next()) {
            let stream = stream.unwrap();
            let addr = stream.peer_addr().unwrap();
            info!("Connection from {}", addr);
            await!(broker.send_async(broker::Event::Connected(stream))).ok();
        }
    });
}
