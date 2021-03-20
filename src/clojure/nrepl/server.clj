(ns nrepl.server
  "Default server implementations"
  {:author "Chas Emerick"}
  (:require
   [nrepl.ack :as ack]
   [nrepl.middleware.dynamic-loader :as dynamic-loader]
   [nrepl.middleware :as middleware]
   nrepl.middleware.completion
   nrepl.middleware.interruptible-eval
   nrepl.middleware.load-file
   nrepl.middleware.lookup
   nrepl.middleware.session
   nrepl.middleware.sideloader
   [nrepl.misc :refer [log noisy-future response-for returning]]
   [nrepl.socket :as socket :refer [inet-socket unix-server-socket]]
   [nrepl.transport :as t])
  (:import
   (java.net SocketException)
   [java.nio.channels ClosedChannelException]))

(defn handle*
  [msg handler transport]
  (try
    (handler (assoc msg :transport transport))
    (catch Throwable t
      (log t "Unhandled REPL handler exception processing message" msg))))

(defn- normalize-msg
  "Normalize messages that are not quite in spec. This comes into effect with
   The EDN transport, and other transports that allow more types/data structures
   than bencode, as there's more opportunity to be out of specification."
  [msg]
  (cond-> msg
    (keyword? (:op msg)) (update :op name)))

(defn handle
  "Handles requests received via [transport] using [handler].
   Returns nil when [recv] returns nil for the given transport."
  [handler transport]
  (when-let [msg (normalize-msg (t/recv transport))]
    (noisy-future (handle* msg handler transport))
    (recur handler transport)))

(defn- safe-close
  [^java.io.Closeable x]
  (try
    (.close x)
    (catch java.io.IOException e
      (log e "Failed to close " x))))

(defn- accept-connection
  [{:keys [server-socket open-transports transport greeting handler]
    :as server}]
  (when-let [sock (try
                    (socket/accept server-socket)
                    (catch ClosedChannelException ex
                      nil))]
    (noisy-future
     (let [transport (transport sock)]
       (try
         (swap! open-transports conj transport)
         (when greeting (greeting transport))
         (handle handler transport)
         (catch SocketException ex
           nil)
         (finally
           (swap! open-transports disj transport)
           (safe-close transport)))))
    (noisy-future
     (try
       (accept-connection server)
       (catch SocketException ex
         nil)))))

(defn stop-server
  "Stops a server started via `start-server`."
  [{:keys [open-transports ^java.io.Closeable server-socket] :as server}]
  (returning server
    (.close server-socket)
    (swap! open-transports
           #(reduce
             (fn [s t]
               ;; should always be true for the socket server...
               (if (instance? java.io.Closeable t)
                 (do
                   (safe-close t)
                   (disj s t))
                 s))
             % %))))

(defn unknown-op
  "Sends an :unknown-op :error for the given message."
  [{:keys [op transport] :as msg}]
  (t/send transport (response-for msg :status #{:error :unknown-op :done} :op op)))

(def default-middleware
  "Middleware vars that are implicitly merged with any additional
   middleware provided to nrepl.server/default-handler."
  [#'nrepl.middleware/wrap-describe
   #'nrepl.middleware.completion/wrap-completion
   #'nrepl.middleware.interruptible-eval/interruptible-eval
   #'nrepl.middleware.load-file/wrap-load-file
   #'nrepl.middleware.lookup/wrap-lookup
   #'nrepl.middleware.session/add-stdin
   #'nrepl.middleware.session/session
   #'nrepl.middleware.sideloader/wrap-sideloader
   #'nrepl.middleware.dynamic-loader/wrap-dynamic-loader])

(def built-in-ops
  "Get all the op names from default middleware automatically"
  (->> default-middleware
       (map #(-> % meta :nrepl.middleware/descriptor :handles keys))
       (reduce concat)
       set))

(def ^{:deprecated "0.8.0"} default-middlewares
  "Use `nrepl.server/default-middleware` instead. Middleware"
  default-middleware)

(defn default-handler
  "A default handler supporting interruptible evaluation, stdin, sessions,
   readable representations of evaluated expressions via `pr`, sideloading, and
   dynamic loading of middleware.

   Additional middleware to mix into the default stack may be provided; these
   should all be values (usually vars) that have an nREPL middleware descriptor
   in their metadata (see `nrepl.middleware/set-descriptor!`).

   This handler bootstraps by initiating with just the dynamic loader, then
   using that to load the other middleware."
  [& additional-middleware]
  (let [initial-handler (dynamic-loader/wrap-dynamic-loader nil)
        state           (atom {:handler initial-handler
                               :stack   [#'nrepl.middleware.dynamic-loader/wrap-dynamic-loader]})]
    (binding [dynamic-loader/*state* state]
      (initial-handler {:op          "swap-middleware"
                        :state       state
                        :middleware (concat default-middleware additional-middleware)}))
    (fn [msg]
      (binding [dynamic-loader/*state* state]
        ((:handler @state) msg)))))

(defrecord Server [server-socket port open-transports transport greeting handler]
  java.io.Closeable
  (close [this] (stop-server this)))

(defn start-server
  "Starts a socket-based nREPL server.  Configuration options include:

   * :port — defaults to 0, which autoselects an open port
   * :bind — bind address, by default \"127.0.0.1\"
   * :socket - filesystem socket path (alternative to :port and :bind)
   * :handler — the nREPL message handler to use for each incoming connection;
       defaults to the result of `(default-handler)`
   * :transport-fn — a function that, given a java.net.Socket corresponding
       to an incoming connection, will return a value satisfying the
       nrepl.Transport protocol for that Socket.
   * :ack-port — if specified, the port of an already-running server
       that will be connected to inform of the new server's port.
       Useful only by Clojure tooling implementations.
   * :greeting-fn - called after a client connects, receives
       a nrepl.transport/Transport. Usually, Clojure-aware client-side tooling
       would provide this greeting upon connecting to the server, but telnet et
       al. isn't that. See `nrepl.transport/tty-greeting` for an example of such
       a function.

   Returns a (record) handle to the server that is started, which may be stopped
   either via `stop-server`, (.close server), or automatically via `with-open`.
   The port that the server is open on is available in the :port slot of the
   server map (useful if the :port option is 0 or was left unspecified."
  [& {:keys [port bind socket transport-fn handler ack-port greeting-fn]}]
  (when (and socket (or port bind))
    (let [msg "Cannot listen on both port and filesystem socket"]
      (log msg)
      (throw (ex-info msg {:nrepl/kind ::invalid-start-request}))))
  (let [transport-fn (or transport-fn t/bencode)
        ss (if socket
             (unix-server-socket socket)
             (inet-socket bind port))
        server (Server. ss
                        (when-not socket (.getLocalPort ss))
                        (atom #{})
                        transport-fn
                        greeting-fn
                        (or handler (default-handler)))]
    (noisy-future
     (try
       (accept-connection server)
       (catch SocketException ex
         nil)))
    (when ack-port
      (ack/send-ack (:port server) ack-port transport-fn))
    server))
