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
use failure::Fail;

// TODO: Should be crate private
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Invalid topic")]
    InvalidTopic,
    #[fail(display = "Unknown client id: {}", _0)]
    UnknownClient(ClientId),
    #[fail(display = "Error on client channel")]
    ClientChannel,
    #[fail(display = "Missing packet id")]
    MissingPacketId,
    #[fail(display = "Received duplicate Connect packet")]
    DuplicateConnect,
    #[fail(display = "Packet reception timeout")]
    PacketTimeout,
    #[fail(display = "IO error: {}", _0)]
    Io(std::io::Error),
    #[fail(display = "Protocol error: {:?}", _0)]
    Protocol(mqtt_codec::ParseError),
}
