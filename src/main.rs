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
#![feature(async_await)]

use futures::{
    compat::Stream01CompatExt,
    future::{FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use hogfold::broker;
use log::info;
use mqtt_codec::TCP_PORT;
use std::{env, error::Error, net::SocketAddr};
use tokio::{net::TcpListener, prelude::*};

fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(debug_assertions)]
    env::set_var("RUST_LOG", "hogfold=trace");
    #[cfg(not(debug_assertions))]
    env::set_var("RUST_LOG", "hogfold=info");
    env_logger::init();

    let server = async move {
        let (mut broker, _) = broker::Broker::start().await;

        let addr = format!("127.0.0.1:{}", TCP_PORT).parse::<SocketAddr>()?;
        info!("Binding {}", addr);
        let v4 = TcpListener::bind(&addr)?.incoming();

        let addr = format!("[::1]:{}", TCP_PORT).parse::<SocketAddr>()?;
        info!("Binding {}", addr);
        let v6 = TcpListener::bind(&addr)?.incoming();

        let mut connections = v4.select(v6).compat();
        loop {
            match connections.next().await {
                Some(connection) => {
                    let connection = broker::Event::Connection(connection?);
                    broker.send(connection).map(drop).await;
                }
                None => break Ok(()),
            }
        }
    };

    tokio::run(server.map_err(|e: Box<dyn Error>| panic!("{}", e)).boxed().compat());
    Ok(())
}
