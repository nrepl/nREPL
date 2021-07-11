(ns nrepl.transport
  {:author "Chas Emerick"}
  (:refer-clojure :exclude [send])
  (:require
   [clojure.java.io :as io]
   [clojure.walk :as walk]
   [nrepl.bencode :as bencode]
   [nrepl.socket :as socket]
   [clojure.edn :as edn]
   [nrepl.misc :refer [noisy-future uuid]]
   nrepl.version)
  (:import
   clojure.lang.RT
   [java.io ByteArrayOutputStream EOFException PushbackInputStream PushbackReader OutputStream]
   [java.net SocketException StandardSocketOptions]
   [java.nio ByteBuffer]
   [java.util.concurrent BlockingQueue LinkedBlockingQueue SynchronousQueue TimeUnit]))

(def orig-warn-on-reflection *warn-on-reflection*)

(defprotocol Transport
  "Defines the interface for a wire protocol implementation for use
   with nREPL."
  (recv [this] [this timeout]
    "Reads and returns the next message received.  Will block.
     Should return nil the a message is not available after `timeout`
     ms or if the underlying channel has been closed.")
  (send [this msg] "Sends msg. Implementations should return the transport."))

(deftype FnTransport [recv-fn send-fn close]
  Transport
  (send [this msg] (send-fn msg) this)
  (recv [this] (.recv this Long/MAX_VALUE))
  (recv [_this timeout] (recv-fn timeout))
  java.io.Closeable
  (close [_this] (close)))

(defn fn-transport
  "Returns a Transport implementation that delegates its functionality
   to the 2 or 3 functions provided."
  ([transport-read write] (fn-transport transport-read write nil))
  ([transport-read write close]
   (let [read-queue (SynchronousQueue.)
         msg-pump (noisy-future
                   (try
                     (try
                       (while true
                         (.put read-queue (transport-read)))
                       (catch Throwable t
                         (.put read-queue t)))
                     (catch InterruptedException ex
                       nil)))]
     (FnTransport.
      (let [failure (atom nil)]
        #(if @failure
           (throw @failure)
           (let [msg (.poll read-queue % TimeUnit/MILLISECONDS)]
             (if (instance? Throwable msg)
               (do (reset! failure msg) (throw msg))
               msg))))
      write
      (fn [] (close) (future-cancel msg-pump))))))

(defmulti #^{:private true} <bytes class)

(defmethod <bytes :default
  [input]
  input)

(defmethod <bytes (RT/classForName "[B")
  [#^"[B" input]
  (String. input "UTF-8"))

(defmethod <bytes clojure.lang.IPersistentVector
  [input]
  (vec (map <bytes input)))

(defmethod <bytes clojure.lang.IPersistentMap
  [input]
  (->> input
       (map (fn [[k v]] [k (<bytes v)]))
       (into {})))

(set! *warn-on-reflection* false)

(defn- connected? [s] (.isConnected s))
(defn- flush-output [s] (.flush s))
(defn- close-stream [s] (.close s))

(set! *warn-on-reflection* orig-warn-on-reflection)

(defmacro ^{:private true} rethrow-on-disconnection
  [s & body]
  `(try
     ~@body
     (catch RuntimeException e#
       (if (= "EOF while reading" (.getMessage e#))
         (throw (SocketException. "The transport's socket appears to have lost its connection to the nREPL server"))
         (throw e#)))
     (catch EOFException e#
       (if (= "Invalid netstring. Unexpected end of input." (.getMessage e#))
         (throw (SocketException. "The transport's socket appears to have lost its connection to the nREPL server"))
         (throw e#)))
     (catch Throwable e#
       (if (and ~s (not (connected? ~s)))
         (throw (SocketException. "The transport's socket appears to have lost its connection to the nREPL server"))
         (throw e#)))))

(defn ^{:private true} safe-write-bencode
  "Similar to `bencode/write-bencode`, except it will only writes to the output
   stream if the whole `thing` is writable. In practice, it avoids sending partial
    messages down the transport, which is almost always bad news for the client.

   This will still throw an exception if called with something unencodable."
  [^OutputStream output thing]
  (let [buffer (ByteArrayOutputStream.)]
    (bencode/write-bencode buffer thing)
    (.write output (.toByteArray buffer))))

(defn bencode
  "Returns a Transport implementation that serializes messages
   over the given Socket or InputStream/OutputStream using bencode."
  ([s] (bencode s s s))
  ([in out & [s]]
   (let [in (PushbackInputStream. (socket/buffered-input in))
         out (socket/buffered-output out)]
     (fn-transport
      #(let [payload (rethrow-on-disconnection s (bencode/read-bencode in))
             unencoded (<bytes (payload "-unencoded"))
             to-decode (apply dissoc payload "-unencoded" unencoded)]
         (walk/keywordize-keys (merge (dissoc payload "-unencoded")
                                      (when unencoded {"-unencoded" unencoded})
                                      (<bytes to-decode))))
      #(rethrow-on-disconnection s
                                 (locking out
                                   (doto out
                                     (safe-write-bencode %)
                                     flush-output)))
      (fn []
        (if s
          (close-stream s)
          (do
            (close-stream in)
            (close-stream out))))))))

(defn edn
  "Returns a Transport implementation that serializes messages
   over the given Socket or InputStream/OutputStream using EDN."
  {:added "0.7"}
  ([s] (edn s s s))
  ([in out & [s]]
   (let [in (java.io.PushbackReader. (io/reader in))
         out (io/writer out)]
     (fn-transport
      #(rethrow-on-disconnection s (edn/read in))
      #(rethrow-on-disconnection s
                                 (locking out
                                   ;; TODO: The transport doesn't seem to work
                                   ;; without these bindings. Worth investigating
                                   ;; why
                                   (binding [*print-readably* true
                                             *print-length*   nil
                                             *print-level*    nil]
                                     (doto out
                                       (.write (str %))
                                       (.flush)))))
      (fn []
        (if s
          (close-stream s)
          (do
            (close-stream in)
            (close-stream out))))))))

(defn tty
  "Returns a Transport implementation suitable for serving an nREPL backend
   via simple in/out readers, as with a tty or telnet connection."
  ([s] (tty s s s))
  ([in out & [s]]
   (let [r (PushbackReader. (io/reader in))
         w (io/writer out)
         cns (atom "user")
         prompt (fn [newline?]
                  (when newline? (.write w (int \newline)))
                  (.write w (str @cns "=> ")))
         session-id (atom nil)
         read-msg #(let [code (read r)]
                     (merge {:op "eval" :code [code] :ns @cns :id (str "eval" (uuid))}
                            (when @session-id {:session @session-id})))
         read-seq (atom (cons {:op "clone"} (repeatedly read-msg)))
         write (fn [{:keys [out err value status ns new-session id]}]
                 (when new-session (reset! session-id new-session))
                 (when ns (reset! cns ns))
                 (doseq [^String x [out err value] :when x]
                   (.write w x))
                 (when (and (= status #{:done}) id (.startsWith ^String id "eval"))
                   (prompt true))
                 (.flush w))
         read #(let [head (promise)]
                 (swap! read-seq (fn [s]
                                   (deliver head (first s))
                                   (rest s)))
                 @head)]
     (fn-transport read write
                   (when s
                     (swap! read-seq (partial cons {:session @session-id :op "close"}))
                     #(close-stream s))))))

(defn tty-greeting
  "A greeting fn usable with `nrepl.server/start-server`,
   meant to be used in conjunction with Transports returned by the
   `tty` function.

   Usually, Clojure-aware client-side tooling would provide this upon connecting
   to the server, but telnet et al. isn't that."
  [transport]
  (send transport {:out (str ";; nREPL " (:version-string nrepl.version/version)
                             \newline
                             ";; Clojure " (clojure-version)
                             \newline
                             "user=> ")}))

(defmulti uri-scheme
  "Return the uri scheme associated with a transport var."
  identity)

(defmethod uri-scheme #'bencode [_] "nrepl")

(defmethod uri-scheme #'tty [_] "telnet")

(defmethod uri-scheme #'edn [_] "nrepl+edn")

(defmethod uri-scheme :default
  [transport]
  (printf "WARNING: No uri scheme associated with transport %s\n" transport)
  "unknown")

(deftype QueueTransport [^BlockingQueue in ^BlockingQueue out]
  nrepl.transport.Transport
  (send [this msg] (.put out msg) this)
  (recv [_this] (.take in))
  (recv [_this timeout] (.poll in timeout TimeUnit/MILLISECONDS)))

(defn piped-transports
  "Returns a pair of Transports that read from and write to each other."
  []
  (let [a (LinkedBlockingQueue.)
        b (LinkedBlockingQueue.)]
    [(QueueTransport. a b) (QueueTransport. b a)]))
