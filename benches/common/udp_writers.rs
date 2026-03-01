//! UDP writer helpers for sync KCP implementations.

use std::cell::RefCell;
use std::io::Write;
use std::net::{SocketAddr, UdpSocket};
use std::rc::Rc;
use std::time::Duration;

/// Writer that appends to a shared buffer (for kcp crate loopback).
pub struct LoopbackWriter(pub Rc<RefCell<Vec<u8>>>);

impl Write for LoopbackWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Writer that sends KCP output to a fixed peer over UDP.
pub struct ClientUdpWriter {
    pub socket: Rc<UdpSocket>,
    pub peer: SocketAddr,
}

impl Write for ClientUdpWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for _ in 0..10 {
            match self.socket.send_to(buf, self.peer) {
                Ok(_) => return Ok(buf.len()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_micros(100));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Writer for server: buffers until peer is set, then sends. Call send_buffered(peer) when first packet arrives.
pub struct ServerUdpWriter {
    pub socket: Rc<UdpSocket>,
    pub buffer: Rc<RefCell<Vec<u8>>>,
    pub peer: Rc<RefCell<Option<SocketAddr>>>,
}

impl ServerUdpWriter {
    pub fn send_buffered(&self, peer: SocketAddr) -> std::io::Result<()> {
        *self.peer.borrow_mut() = Some(peer);
        let buf = std::mem::take(&mut *self.buffer.borrow_mut());
        if !buf.is_empty() {
            self.socket.send_to(&buf, peer)?;
        }
        Ok(())
    }
}

impl Write for ServerUdpWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(peer) = *self.peer.borrow() {
            for _ in 0..10 {
                match self.socket.send_to(buf, peer) {
                    Ok(_) => return Ok(buf.len()),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_micros(100));
                    }
                    Err(e) => return Err(e),
                }
            }
        } else {
            self.buffer.borrow_mut().extend_from_slice(buf);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
