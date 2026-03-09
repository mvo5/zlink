//! Contains connection related API.

mod read_connection;
pub use read_connection::ReadConnection;
pub mod chain;
pub mod socket;
mod write_connection;
use crate::{
    reply::{self, Reply},
    Call, Result,
};
pub use chain::Chain;
use core::{fmt::Debug, sync::atomic::AtomicUsize};
pub use write_connection::WriteConnection;

use serde::{Deserialize, Serialize};
pub use socket::Socket;

/// A connection.
///
/// The low-level API to send and receive messages.
///
/// Each connection gets a unique identifier when created that can be queried using
/// [`Connection::id`]. This ID is shared betwen the read and write halves of the connection. It
/// can be used to associate the read and write halves of the same connection.
///
/// # Cancel safety
///
/// All async methods of this type are cancel safe unless explicitly stated otherwise in its
/// documentation.
#[derive(Debug)]
pub struct Connection<S: Socket> {
    read: ReadConnection<S::ReadHalf>,
    write: WriteConnection<S::WriteHalf>,
}

impl<S> Connection<S>
where
    S: Socket,
{
    /// Create a new connection.
    pub fn new(socket: S) -> Self {
        let (read, write) = socket.split();
        let id = NEXT_ID.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        Self {
            read: ReadConnection::new(read, id),
            write: WriteConnection::new(write, id),
        }
    }

    /// The reference to the read half of the connection.
    pub fn read(&self) -> &ReadConnection<S::ReadHalf> {
        &self.read
    }

    /// The mutable reference to the read half of the connection.
    pub fn read_mut(&mut self) -> &mut ReadConnection<S::ReadHalf> {
        &mut self.read
    }

    /// The reference to the write half of the connection.
    pub fn write(&self) -> &WriteConnection<S::WriteHalf> {
        &self.write
    }

    /// The mutable reference to the write half of the connection.
    pub fn write_mut(&mut self) -> &mut WriteConnection<S::WriteHalf> {
        &mut self.write
    }

    /// Split the connection into read and write halves.
    pub fn split(self) -> (ReadConnection<S::ReadHalf>, WriteConnection<S::WriteHalf>) {
        (self.read, self.write)
    }

    /// Join the read and write halves into a connection (the opposite of [`Connection::split`]).
    pub fn join(read: ReadConnection<S::ReadHalf>, write: WriteConnection<S::WriteHalf>) -> Self {
        Self { read, write }
    }

    /// The unique identifier of the connection.
    pub fn id(&self) -> usize {
        assert_eq!(self.read.id(), self.write.id());
        self.read.id()
    }

    /// Sends a method call.
    ///
    /// Convenience wrapper around [`WriteConnection::send_call`].
    pub async fn send_call<Method>(&mut self, call: &Call<Method>) -> Result<()>
    where
        Method: Serialize + Debug,
    {
        self.write.send_call(call).await
    }

    /// Receives a method call reply.
    ///
    /// Convenience wrapper around [`ReadConnection::receive_reply`].
    pub async fn receive_reply<'r, ReplyParams, ReplyError>(
        &'r mut self,
    ) -> Result<reply::Result<ReplyParams, ReplyError>>
    where
        ReplyParams: Deserialize<'r> + Debug,
        ReplyError: Deserialize<'r> + Debug,
    {
        self.read.receive_reply().await
    }

    /// Call a method and receive a reply.
    ///
    /// This is a convenience method that combines [`Connection::send_call`] and
    /// [`Connection::receive_reply`].
    pub async fn call_method<'r, Method, ReplyParams, ReplyError>(
        &'r mut self,
        call: &Call<Method>,
    ) -> Result<reply::Result<ReplyParams, ReplyError>>
    where
        Method: Serialize + Debug,
        ReplyParams: Deserialize<'r> + Debug,
        ReplyError: Deserialize<'r> + Debug,
    {
        self.send_call(call).await?;
        self.receive_reply().await
    }

    /// Call a method by name with dynamic (untyped) parameters and reply.
    ///
    /// This is a convenience wrapper around [`Connection::call_method`] for use cases where the
    /// method name and parameters are not known at compile time — e.g. HTTP bridges, CLI tools,
    /// proxies, or anything that discovers available methods via `GetInfo` at runtime.
    ///
    /// Standard `org.varlink.service.*` errors are still caught and returned as
    /// [`crate::Error::VarlinkService`]. Application-specific errors are returned as
    /// `Err(serde_json::Value)` in the inner [`reply::Result`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> zlink_core::Result<()> {
    /// # let mut conn: zlink_core::Connection<zlink_core::connection::socket::impl_for_doc::Socket> = todo!();
    /// let reply = conn
    ///     .call_dynamic("org.example.GetUser", serde_json::json!({"id": 42}))
    ///     .await?;
    /// match reply {
    ///     Ok(reply) => println!("reply: {:?}", reply.parameters()),
    ///     Err(error) => println!("application error: {error}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_dynamic(
        &mut self,
        method: &str,
        parameters: serde_json::Value,
    ) -> Result<reply::Result<serde_json::Value, serde_json::Value>> {
        let call = Call::new(DynamicMethod { method, parameters });
        self.send_call(&call).await?;
        self.receive_reply().await
    }

    /// Receive a method call over the socket.
    ///
    /// Convenience wrapper around [`ReadConnection::receive_call`].
    pub async fn receive_call<'m, Method>(&'m mut self) -> Result<Call<Method>>
    where
        Method: Deserialize<'m> + Debug,
    {
        self.read.receive_call().await
    }

    /// Send a reply over the socket.
    ///
    /// Convenience wrapper around [`WriteConnection::send_reply`].
    pub async fn send_reply<ReplyParams>(&mut self, reply: &Reply<ReplyParams>) -> Result<()>
    where
        ReplyParams: Serialize + Debug,
    {
        self.write.send_reply(reply).await
    }

    /// Send an error reply over the socket.
    ///
    /// Convenience wrapper around [`WriteConnection::send_error`].
    pub async fn send_error<ReplyError>(&mut self, error: &ReplyError) -> Result<()>
    where
        ReplyError: Serialize + Debug,
    {
        self.write.send_error(error).await
    }

    /// Enqueue a call to the server.
    ///
    /// Convenience wrapper around [`WriteConnection::enqueue_call`].
    pub fn enqueue_call<Method>(&mut self, method: &Call<Method>) -> Result<()>
    where
        Method: Serialize + Debug,
    {
        self.write.enqueue_call(method)
    }

    /// Flush the connection.
    ///
    /// Convenience wrapper around [`WriteConnection::flush`].
    pub async fn flush(&mut self) -> Result<()> {
        self.write.flush().await
    }

    /// Start a chain of method calls.
    ///
    /// This allows batching multiple calls together and sending them in a single write operation.
    ///
    /// # Examples
    ///
    /// ## Basic Usage with Sequential Access
    ///
    /// ```no_run
    /// use zlink_core::{Connection, Call, reply};
    /// use serde::{Serialize, Deserialize};
    /// use serde_prefix_all::prefix_all;
    /// use futures_util::{pin_mut, stream::StreamExt};
    ///
    /// # async fn example() -> zlink_core::Result<()> {
    /// # let mut conn: Connection<zlink_core::connection::socket::impl_for_doc::Socket> = todo!();
    ///
    /// #[prefix_all("org.example.")]
    /// #[derive(Debug, Serialize, Deserialize)]
    /// #[serde(tag = "method", content = "parameters")]
    /// enum Methods {
    ///     GetUser { id: u32 },
    ///     GetProject { id: u32 },
    /// }
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct User { name: String }
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct Project { title: String }
    ///
    /// #[derive(Debug, zlink_core::ReplyError)]
    /// #[zlink(
    ///     interface = "org.example",
    ///     // Not needed in the real code because you'll use `ReplyError` through `zlink` crate.
    ///     crate = "zlink_core",
    /// )]
    /// enum ApiError {
    ///     UserNotFound { code: i32 },
    ///     ProjectNotFound { code: i32 },
    /// }
    ///
    /// let get_user = Call::new(Methods::GetUser { id: 1 });
    /// let get_project = Call::new(Methods::GetProject { id: 2 });
    ///
    /// // Chain calls and send them in a batch
    /// let replies = conn
    ///     .chain_call::<Methods, User, ApiError>(&get_user)?
    ///     .append(&get_project)?
    ///     .send().await?;
    /// pin_mut!(replies);
    ///
    /// // Access replies sequentially - types are now fixed by the chain
    /// let user_reply = replies.next().await.unwrap()?;
    /// let project_reply = replies.next().await.unwrap()?;
    ///
    /// match user_reply {
    ///     Ok(user) => println!("User: {}", user.parameters().unwrap().name),
    ///     Err(error) => println!("User error: {:?}", error),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Arbitrary Number of Calls
    ///
    /// ```no_run
    /// # use zlink_core::{Connection, Call, reply};
    /// # use serde::{Serialize, Deserialize};
    /// # use futures_util::{pin_mut, stream::StreamExt};
    /// # use serde_prefix_all::prefix_all;
    /// # async fn example() -> zlink_core::Result<()> {
    /// # let mut conn: Connection<zlink_core::connection::socket::impl_for_doc::Socket> = todo!();
    /// # #[prefix_all("org.example.")]
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # #[serde(tag = "method", content = "parameters")]
    /// # enum Methods {
    /// #     GetUser { id: u32 },
    /// # }
    /// # #[derive(Debug, Deserialize)]
    /// # struct User { name: String }
    /// # #[derive(Debug, zlink_core::ReplyError)]
    /// #[zlink(
    ///     interface = "org.example",
    ///     // Not needed in the real code because you'll use `ReplyError` through `zlink` crate.
    ///     crate = "zlink_core",
    /// )]
    /// # enum ApiError {
    /// #     UserNotFound { code: i32 },
    /// #     ProjectNotFound { code: i32 },
    /// # }
    /// # let get_user = Call::new(Methods::GetUser { id: 1 });
    ///
    /// // Chain many calls (no upper limit)
    /// let mut chain = conn.chain_call::<Methods, User, ApiError>(&get_user)?;
    /// for i in 2..100 {
    ///     chain = chain.append(&Call::new(Methods::GetUser { id: i }))?;
    /// }
    ///
    /// let replies = chain.send().await?;
    /// pin_mut!(replies);
    ///
    /// // Process all replies sequentially - types are fixed by the chain
    /// while let Some(user_reply) = replies.next().await {
    ///     let user_reply = user_reply?;
    ///     // Handle each reply...
    ///     match user_reply {
    ///         Ok(user) => println!("User: {}", user.parameters().unwrap().name),
    ///         Err(error) => println!("Error: {:?}", error),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance Benefits
    ///
    /// Instead of multiple write operations, the chain sends all calls in a single
    /// write operation, reducing context switching and therefore minimizing latency.
    pub fn chain_call<'c, Method, ReplyParams, ReplyError>(
        &'c mut self,
        call: &Call<Method>,
    ) -> Result<Chain<'c, S, ReplyParams, ReplyError>>
    where
        Method: Serialize + Debug,
        ReplyParams: Deserialize<'c> + Debug,
        ReplyError: Deserialize<'c> + Debug,
    {
        Chain::new(self, call)
    }
}

impl<S> From<S> for Connection<S>
where
    S: Socket,
{
    fn from(socket: S) -> Self {
        Self::new(socket)
    }
}

/// Internal method type for [`Connection::call_dynamic`].
///
/// Serializes as `{"method": "<name>", "parameters": <value>}` which is what the [`Call`]
/// serializer expects to flatten into the wire message.
#[derive(Debug, Serialize)]
struct DynamicMethod<'m> {
    method: &'m str,
    parameters: serde_json::Value,
}

pub(crate) const BUFFER_SIZE: usize = 256;
const MAX_BUFFER_SIZE: usize = 100 * 1024 * 1024; // Don't allow buffers over 100MB.

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::mock_socket::MockSocket;
    use serde_json::json;

    #[tokio::test]
    async fn call_dynamic_success() -> crate::Result<()> {
        let socket = MockSocket::new(&[r#"{"parameters":{"name":"alice","id":42}}"#]);
        let mut conn = Connection::new(socket);

        let reply = conn
            .call_dynamic("org.example.GetUser", json!({"id": 42}))
            .await?;

        let params = reply.unwrap().into_parameters().unwrap();
        assert_eq!(params["name"], "alice");
        assert_eq!(params["id"], 42);
        Ok(())
    }

    #[tokio::test]
    async fn call_dynamic_empty_parameters() -> crate::Result<()> {
        let socket = MockSocket::new(&[r#"{"parameters":{}}"#]);
        let mut conn = Connection::new(socket);

        let reply = conn.call_dynamic("org.example.Ping", json!({})).await?;

        let params = reply.unwrap().into_parameters().unwrap();
        assert_eq!(params, json!({}));
        Ok(())
    }

    #[tokio::test]
    async fn call_dynamic_no_parameters_in_reply() -> crate::Result<()> {
        let socket = MockSocket::new(&[r#"{}"#]);
        let mut conn = Connection::new(socket);

        let reply = conn.call_dynamic("org.example.Ping", json!({})).await?;

        assert!(reply.unwrap().into_parameters().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn call_dynamic_application_error() -> crate::Result<()> {
        let socket =
            MockSocket::new(&[r#"{"error":"org.example.UserNotFound","parameters":{"id":99}}"#]);
        let mut conn = Connection::new(socket);

        let reply = conn
            .call_dynamic("org.example.GetUser", json!({"id": 99}))
            .await?;

        let err = reply.unwrap_err();
        assert_eq!(err["error"], "org.example.UserNotFound");
        assert_eq!(err["parameters"]["id"], 99);
        Ok(())
    }

    #[tokio::test]
    async fn call_dynamic_varlink_service_error() {
        let socket = MockSocket::new(&[
            r#"{"error":"org.varlink.service.MethodNotFound","parameters":{"method":"org.example.Missing"}}"#,
        ]);
        let mut conn = Connection::new(socket);

        let result = conn.call_dynamic("org.example.Missing", json!({})).await;

        // org.varlink.service.* errors are returned as the top-level zlink::Error.
        let err = result.unwrap_err();
        assert!(
            matches!(err, crate::Error::VarlinkService(_)),
            "expected VarlinkService error, got: {err:?}"
        );
    }
}
