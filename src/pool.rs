//! Connection pooling for a single MongoDB server.
use Error::{ArgumentError, OperationError};
use Result;

use connstring::Host;

use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

pub static DEFAULT_POOL_SIZE: usize = 5;

/// Handles threaded connections to a MongoDB server.
#[derive(Clone)]
pub struct ConnectionPool {
    /// The connection host.
    pub host: Host,
    // The socket pool.
    inner: Arc<Mutex<Pool>>,
    // A condition variable used for threads waiting for the pool
    // to be repopulated with available connections.
    wait_lock: Arc<Condvar>,
}

struct Pool {
    /// The maximum number of concurrent connections allowed.
    pub size: usize,
    // The current number of open connections.
    pub len: Arc<AtomicUsize>,
    // The idle socket pool.
    sockets: Vec<TcpStream>,
    // The pool iteration. When a server monitor fails to execute ismaster,
    // the connection pool is cleared and the iteration is incremented.
    iteration: usize,
}

/// Holds an available socket, with logic to return the socket
/// to the connection pool when dropped.
pub struct PooledStream {
    // This socket option will always be Some(stream) until it is
    // returned to the pool using take().
    socket: Option<TcpStream>,
    // A reference to the pool that the stream was taken from.
    pool: Arc<Mutex<Pool>>,
    // A reference to the waiting condvar associated with the pool.
    wait_lock: Arc<Condvar>,
    // The pool iteration at the moment of extraction.
    iteration: usize,
}

impl PooledStream {
    /// Returns a reference to the socket.
    pub fn get_socket<'a>(&'a self) -> &'a TcpStream {
        self.socket.as_ref().unwrap()
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        // Attempt to lock and return the socket to the pool,
        // or give up if the pool lock has been poisoned.
        if let Ok(mut locked) = self.pool.lock() {
            if self.iteration == locked.iteration {
                locked.sockets.push(self.socket.take().unwrap());
                // Notify waiting threads that the pool has been repopulated.
                self.wait_lock.notify_one();
            }
        }
    }
}

impl ConnectionPool {

    /// Returns a connection pool with a default size.
    pub fn new(host: Host) -> ConnectionPool {
        ConnectionPool::with_size(host, DEFAULT_POOL_SIZE)
    }

    /// Returns a connection pool with a specified capped size.
    pub fn with_size(host: Host, size: usize) -> ConnectionPool {
        ConnectionPool {
            host: host,
            wait_lock: Arc::new(Condvar::new()),
            inner: Arc::new(Mutex::new(Pool {
                len: Arc::new(ATOMIC_USIZE_INIT),
                size: size,
                sockets: Vec::with_capacity(size),
                iteration: 0,
            })),
        }
    }

    /// Sets the maximum number of open connections.
    pub fn set_size(&self, size: usize) -> Result<()> {
        if size < 1 {
            Err(ArgumentError("The connection pool size must be greater than zero.".to_owned()))
        } else {
            let mut locked = try!(self.inner.lock());
            locked.size = size;
            Ok(())
        }
    }

    // Clear all open socket connections.
    pub fn clear(&self) {
        if let Ok(mut locked) = self.inner.lock() {
            locked.iteration += 1;
            locked.sockets.clear();
            locked.len.store(0, Ordering::SeqCst);
        }
    }
    
    /// Attempts to acquire a connected socket. If none are available and
    /// the pool has not reached its maximum size, a new socket will connect.
    /// Otherwise, the function will block until a socket is returned to the pool.
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        let mut locked = try!(self.inner.lock());
        if locked.size == 0 {
            return Err(OperationError("The connection pool does not allow connections; \
                                       increase the size of the pool.".to_owned()));
        }

        loop {
            // Acquire available existing socket
            if let Some(stream) = locked.sockets.pop() {
                return Ok(PooledStream {
                    socket: Some(stream),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                    iteration: locked.iteration,
                });
            }

            // Attempt to make a new connection
            let len = locked.len.load(Ordering::SeqCst);
            if len < locked.size {
                let socket = try!(self.connect());
                let _ = locked.len.fetch_add(1, Ordering::SeqCst);
                return Ok(PooledStream {
                    socket: Some(socket),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                    iteration: locked.iteration,
                });
            }

            // Release lock and wait for pool to be repopulated
            locked = try!(self.wait_lock.wait(locked));
        }
    }

    // Connects to a MongoDB server as defined by the initial configuration.
    fn connect(&self) -> Result<TcpStream> {
        let ref host_name = self.host.host_name;
        let port = self.host.port;
        let stream = try!(TcpStream::connect((&host_name[..], port)));
        Ok(stream)
    }
}
