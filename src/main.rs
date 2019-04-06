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
use failure::Error;
use log::info;
use mqtt_codec::TCP_PORT;
use std::{env, net::SocketAddr};
use tokio::{net::TcpListener, prelude::*};

mod broker;
mod client;
mod client_id;
mod subscriptions;

fn main() -> Result<(), Error> {
    #[cfg(debug_assertions)]
    env::set_var("RUST_LOG", "hogfold=trace");
    #[cfg(not(debug_assertions))]
    env::set_var("RUST_LOG", "hogfold=info");
    env_logger::init();

    let mut runtime = tokio::runtime::Runtime::new()?;
    let broker = broker::Broker::start(&mut runtime);
    let v6_addr = format!("[::1]:{}", TCP_PORT).parse::<SocketAddr>().unwrap();
    let v4_addr = format!("127.0.0.1:{}", TCP_PORT).parse::<SocketAddr>().unwrap();

    info!("Binding {}", v4_addr);
    let v4_listener = TcpListener::bind(&v4_addr)?;
    info!("Binding {}", v6_addr);
    let v6_listener = TcpListener::bind(&v6_addr)?;

    tokio::run(
        v4_listener
            .incoming()
            .select(v6_listener.incoming())
            .and_then(|stream| stream.peer_addr().map(|addr| (addr, stream)))
            .map(broker::Event::Connection)
            .map_err(drop)
            .forward(broker.sink_map_err(drop))
            .map(drop)
            .map_err(drop),
    );
    Ok(())
}
