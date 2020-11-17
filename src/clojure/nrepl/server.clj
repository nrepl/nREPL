(ns nrepl.server
  "Default server implementations"
  {:author "Chas Emerick"}
  (:refer-clojure :exclude [future future-call])
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
   [nrepl.misc :refer [log response-for returning]]
   [nrepl.transport :as t])
  (:import
   [java.net InetSocketAddress ServerSocket]))

(defn ^:private deref-future
  ([^java.util.concurrent.Future fut]
   (.get fut))
  ([^java.util.concurrent.Future fut timeout-ms timeout-val]
   (try (.get fut timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)
        (catch java.util.concurrent.TimeoutException e
          timeout-val))))

(defn platform-future-call
  "Takes a function of no args and yields a future object that will
  invoke the function in another thread, and will cache the result and
  return it on all subsequent calls to deref/@. If the computation has
  not yet finished, calls to deref/@ will block, unless the variant
  of deref with timeout is used. See also - realized?.

  Copied from clojure.core and modified to accept a thread-factory"
  {:static true}
  [thread-factory f]
  (let [f (bound-fn* f)
        fut (java.util.concurrent.CompletableFuture.)]
    (.start
     (.newThread thread-factory
                 #(.complete fut (f))))
    (reify
      clojure.lang.IDeref
      (deref [_] (deref-future fut))
      clojure.lang.IBlockingDeref
      (deref
        [_ timeout-ms timeout-val]
        (deref-future fut timeout-ms timeout-val))
      clojure.lang.IPending
      (isRealized [_] (.isDone fut))
      java.util.concurrent.Future
      (get [_] (.get fut))
      (get [_ timeout unit] (.get fut timeout unit))
      (isCancelled [_] (.isCancelled fut))
      (isDone [_] (.isDone fut))
      (cancel [_ interrupt?] (.cancel fut interrupt?)))))

(defmacro platform-future
  "Takes a body of expressions and yields a future object that will
  invoke the body in another thread, and will cache the result and
  return it on all subsequent calls to deref/@. If the computation has
  not yet finished, calls to deref/@ will block, unless the variant of
  deref with timeout is used. See also - realized?.

  Copied from clojure.core and modified to accept a thread-factory"
  [thread-factory & body] `(platform-future-call ~thread-factory (^{:once true} fn* [] ~@body)))

(defn handle*
  [msg handler transport thread-factory]
  (try
    (handler (assoc msg
                    :transport transport
                    :thread-factory thread-factory))
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
  [handler transport thread-factory]
  (when-let [msg (normalize-msg (t/recv transport))]
    (platform-future thread-factory
                     (handle* msg handler transport thread-factory))
    (recur handler transport thread-factory)))

(defn- safe-close
  [^java.io.Closeable x]
  (try
    (.close x)
    (catch java.io.IOException e
      (log e "Failed to close " x))))

(defn- accept-connection
  [{:keys [^ServerSocket server-socket open-transports transport greeting handler thread-factory]
    :as server}]
  (when-not (.isClosed server-socket)
    (let [sock (.accept server-socket)]
      (platform-future thread-factory
                       (let [transport (transport sock)]
                         (try
                           (swap! open-transports conj transport)
                           (when greeting (greeting transport))
                           (handle handler transport thread-factory)
                           (finally
                             (swap! open-transports disj transport)
                             (safe-close transport)))))
      (platform-future thread-factory (accept-connection server)))))

(defn stop-server
  "Stops a server started via `start-server`."
  [{:keys [open-transports ^ServerSocket server-socket] :as server}]
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

(defrecord Server [server-socket port open-transports transport greeting handler thread-factory]
  java.io.Closeable
  (close [this] (stop-server this)))

(defn create-default-thread-factory
  []
  (reify java.util.concurrent.ThreadFactory
    (newThread [_ r]
      (Thread. r))))

(defn start-server
  "Starts a socket-based nREPL server.  Configuration options include:

   * :port — defaults to 0, which autoselects an open port
   * :bind — bind address, by default \"127.0.0.1\"
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
  [& {:keys [port bind transport-fn handler ack-port greeting-fn thread-factory]}]
  (let [port (or port 0)
        addr (fn [^String bind ^Integer port] (InetSocketAddress. bind port))
        transport-fn (or transport-fn t/bencode)
        ;; We fallback to 127.0.0.1 instead of to localhost to avoid
        ;; a dependency on the order of ipv4 and ipv6 records for
        ;; localhost in /etc/hosts
        bind (or bind "127.0.0.1")
        ss (doto (ServerSocket.)
             (.setReuseAddress true)
             (.bind (addr bind port)))
        thread-factory (or thread-factory (create-default-thread-factory))
        server (Server. ss
                        (.getLocalPort ss)
                        (atom #{})
                        transport-fn
                        greeting-fn
                        (or handler (default-handler))
                        thread-factory)]
    (platform-future thread-factory
                     (accept-connection server))
    (when ack-port
      (ack/send-ack (:port server) ack-port transport-fn))
    server))
