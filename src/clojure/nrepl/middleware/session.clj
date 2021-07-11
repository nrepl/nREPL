(ns nrepl.middleware.session
  "Support for persistent, cross-connection REPL sessions."
  {:author "Chas Emerick"}
  (:require
   clojure.main
   [nrepl.middleware :refer [set-descriptor!]]
   [nrepl.middleware.interruptible-eval :refer [*msg* evaluate]]
   [nrepl.misc :refer [noisy-future uuid response-for]]
   [nrepl.transport :as t])
  (:import
   (clojure.lang LineNumberingPushbackReader)
   (java.io Reader)
   (java.util.concurrent.atomic AtomicLong)
   (java.util.concurrent BlockingQueue LinkedBlockingQueue SynchronousQueue
                         ExecutorService
                         ThreadFactory ThreadPoolExecutor
                         TimeUnit)))

(def ^{:private true} sessions (atom {}))

(defn close-all-sessions!
  "Use this fn to manually shut down all sessions. Since each new session spanws
   a new thread, and sessions need to be otherwise explicitly closed, we can
   accumulate too many active sessions for the JVM. This occurs when we are
   running tests in watch mode."
  []
  (run! (fn [[id session]]
          (when-let [close (:close (meta session))]
            (close))
          (swap! sessions dissoc id))
        @sessions))

;; TODO: the way this is currently, :out and :err will continue to be
;; associated with a particular *msg* (and session) even when produced from a future,
;; agent, etc. due to binding conveyance.  This may or may not be desirable
;; depending upon the expectations of the client/user.  I'm not sure at the moment
;; how best to make it configurable though...

(def ^{:dynamic true :private true} *skipping-eol* false)

(defn- configure-thread-factory
  "Returns a new ThreadFactory for the given session.  This implementation
   generates daemon threads, with names that include the session id."
  []
  (let [session-thread-counter (AtomicLong. 0)
        ;; Create a constant dcl for use across evaluations. This allows
        ;; modifications to the classloader to persist.
        cl (clojure.lang.DynamicClassLoader.
            (.getContextClassLoader (Thread/currentThread)))]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. runnable
                       (format "nREPL-worker-%s" (.getAndIncrement session-thread-counter)))
          (.setDaemon true)
          (.setContextClassLoader cl))))))

(defn- configure-executor
  "Returns a ThreadPoolExecutor, configured (by default) to
   have 1 core thread, use an unbounded queue, create only daemon threads,
   and allow unused threads to expire after 30s."
  [& {:keys [keep-alive queue thread-factory]
      :or {keep-alive 30000
           queue (SynchronousQueue.)}}]
  (let [^ThreadFactory thread-factory (or thread-factory (configure-thread-factory))]
    (ThreadPoolExecutor. 1 Integer/MAX_VALUE
                         (long keep-alive) TimeUnit/MILLISECONDS
                         ^BlockingQueue queue
                         thread-factory)))

(def default-executor "Delay containing the default Executor." (delay (configure-executor)))

(defn default-exec
  "Submits a task for execution using #'default-executor.
   The submitted task is made of:
   * an id (typically the message id),
   * thunk, a Runnable, the task itself,
   * ack, another Runnable, ran to notify of successful execution of thunk.
   The thunk/ack split is meaningful for interruptible eval: only the thunk can be interrupted."
  [_id ^Runnable thunk ^Runnable ack]
  (let [^Runnable f #(do (.run thunk) (.run ack))]
    (.submit ^ExecutorService @default-executor f)))

(defn- session-in
  "Returns a LineNumberingPushbackReader suitable for binding to *in*.
   When something attempts to read from it, it will (if empty) send a
   {:status :need-input} message on the provided transport so the client/user
   can provide content to be read."
  [session-id transport]
  (let [input-queue (LinkedBlockingQueue.)
        request-input (fn []
                        (cond (> (.size input-queue) 0)
                              (.take input-queue)
                              *skipping-eol*
                              nil
                              :else
                              (do
                                (t/send transport
                                        (response-for *msg* :session session-id
                                                      :status :need-input))
                                (.take input-queue))))
        do-read (fn [buf off len]
                  (locking input-queue
                    (loop [i off]
                      (cond
                        (>= i (+ off len))
                        (+ off len)
                        (.peek input-queue)
                        (do (aset-char buf i (char (.take input-queue)))
                            (recur (inc i)))
                        :else
                        i))))
        reader (LineNumberingPushbackReader.
                (proxy [Reader] []
                  (close [] (.clear input-queue))
                  (read
                    ([]
                     (let [^Reader this this] (proxy-super read)))
                    ([x]
                     (let [^Reader this this]
                       (if (instance? java.nio.CharBuffer x)
                         (proxy-super read ^java.nio.CharBuffer x)
                         (proxy-super read ^chars x))))
                    ([^chars buf off len]
                     (if (zero? len)
                       -1
                       (let [first-character (request-input)]
                         (if (or (nil? first-character) (= first-character -1))
                           -1
                           (do
                             (aset-char buf off (char first-character))
                             (- (do-read buf (inc off) (dec len))
                                off)))))))))]
    {:input-queue input-queue
     :stdin-reader reader}))

(defn- create-session
  "Returns a new atom containing a map of bindings as per
  `clojure.core/get-thread-bindings`. *in* is obtained using `session-in`, *ns*
  defaults to 'user, and other bindings as optionally provided in
  `session` are merged in."
  ([{:keys [transport session out-limit]}]
   (let [id (uuid)
         {:keys [input-queue stdin-reader]} (session-in id transport)
         the-session (atom (into (or (some-> session deref) {})
                                 {#'*in* stdin-reader
                                  #'*ns* (create-ns 'user)})
                           :meta {:id id
                                  :out-limit (or out-limit (:out-limit (meta session)))
                                  :stdin-reader stdin-reader
                                  :input-queue input-queue
                                  :exec default-exec})
         msg {:code "" :session the-session}]
     ;; to fully initialize bindings
     (binding [*msg* msg]
       (evaluate msg))
     the-session)))

(defn- interrupt-stop
  "This works as follows

  1. Calls interrupt
  2. Wait 100ms. This is mainly to allow thread that respond quickly to
     interrupts to send a message back in response to the interrupt. Significantly,
     this includes an exception thrown by `Thread/sleep`.
  3. Asynchronously: wait another 5000ms for the thread to cleanly terminate.
     Only calls `.stop` if it fails to do so (and risk state corruption)

  This set of behaviours strikes a balance between allowing a thread to respond
  to an interrupt, but also ensuring we actually kill runaway processes.

  If required, a future feature could make both timeouts configurable, either
  as a server config or parameters provided by the `interrupt` message."
  [^Thread t]
  (.interrupt t)
  (Thread/sleep 100)
  (noisy-future
   (Thread/sleep 5000)
   (when-not (= (Thread$State/TERMINATED)
                (.getState t))
     (.stop t))))

(defn session-exec
  "Takes a session id and returns a maps of three functions meant for interruptible-eval:
   * :exec, takes an id (typically a msg-id), a thunk and an ack runnables (see #'default-exec for ampler
     context). Executions are serialized and occurs on a single thread.
   * :interrupt, takes an id and tries to interrupt the matching execution (submitted with :exec above).
     A nil id is meant to match the currently running execution. The return value can be either:
     :idle (no running execution), the interrupted id, or nil when the running id doesn't match the id argument.
     Upon successful interruption the backing thread is replaced.
   * :close, terminates the backing thread."
  [id]
  (let [cl (clojure.lang.DynamicClassLoader.
            (.getContextClassLoader (Thread/currentThread)))
        queue (LinkedBlockingQueue.)
        running (atom nil)
        thread (atom nil)
        main-loop #(try
                     (loop []
                       (let [[exec-id ^Runnable r ^Runnable ack] (.take queue)]
                         (reset! running exec-id)
                         (when (try
                                 (.run r)
                                 (compare-and-set! running exec-id nil)
                                 (finally
                                   (compare-and-set! running exec-id nil)))
                           (some-> ack .run)
                           (recur))))
                     (catch InterruptedException _e))
        spawn-thread #(doto (Thread. main-loop (str "nREPL-session-" id))
                        (.setDaemon true)
                        (.setContextClassLoader cl)
                        .start)]
    (reset! thread (spawn-thread))
    ;; This map is added to the meta of the session object by `register-session`,
    ;; it contains functions that are accessed by `interrupt-session` and `close-session`.
    {:interrupt (fn [exec-id]
                  ;; nil means interrupt whatever is running
                  ;; returns :idle, interrupted id or nil
                  (let [current @running]
                    (cond
                      (nil? current) :idle
                      (and (or (nil? exec-id) (= current exec-id)) ; cas only checks identity, so check equality first
                           (compare-and-set! running current nil))
                      (do
                        (interrupt-stop @thread)
                        (reset! thread (spawn-thread))
                        current))))
     :close #(interrupt-stop @thread)
     :exec (fn [exec-id r ack]
             (.put queue [exec-id r ack]))}))

(defn- register-session
  "Registers a new session containing the baseline bindings contained in the
   given message's :session."
  [{:keys [transport] :as msg}]
  (let [session (create-session msg)
        {:keys [id]} (meta session)]
    (alter-meta! session into (session-exec id))
    (swap! sessions assoc id session)
    (t/send transport (response-for msg :status :done :new-session id))))

(defn- interrupt-session
  [{:keys [session interrupt-id transport] :as msg}]
  (let [{:keys [interrupt] session-id :id} (meta session)
        interrupted-id (when interrupt (interrupt interrupt-id))]
    (cond
      (nil? interrupt)
      (t/send transport (response-for msg :status #{:error :session-ephemeral :done}))

      (nil? interrupted-id)
      (t/send transport (response-for msg :status #{:error :interrupt-id-mismatch :done}))

      (= :idle interrupted-id)
      (t/send transport (response-for msg :status #{:session-idle :done}))

      :else
      (do
        (t/send transport {:status #{:interrupted :done}
                           :id interrupted-id
                           :session session-id})
        (t/send transport (response-for msg :status #{:done}))))))

(defn- close-session
  "Drops the session associated with the given message."
  [{:keys [session transport] :as msg}]
  (let [{:keys [close] session-id :id} (meta session)]
    (when close (close))
    (swap! sessions dissoc session-id)
    (t/send transport (response-for msg :status #{:done :session-closed}))))

(defn session
  "Session middleware.  Returns a handler which supports these :op-erations:

   * \"clone\", which will cause a new session to be retained.  The ID of this
     new session will be returned in a response message in a :new-session
     slot.  The new session's state (dynamic scope, etc) will be a copy of
     the state of the session identified in the :session slot of the request.
   * \"interrupt\", which will attempt to interrupt the current execution with
     id provided in the :interrupt-id slot.
   * \"close\", which drops the session indicated by the
     ID in the :session slot.  The response message's :status will include
     :session-closed.
   * \"ls-sessions\", which results in a response message
     containing a list of the IDs of the currently-retained sessions in a
     :session slot.

   Messages indicating other operations are delegated to the given handler,
   with the session identified by the :session ID added to the message. If
   no :session ID is found, a new session is created (which will only
   persist for the duration of the handling of the given message).

   Requires the interruptible-eval middleware (specifically, its binding of
   *msg* to the currently-evaluated message so that session-specific *out*
   and *err* content can be associated with the originating message)."
  [h]
  (fn [{:keys [op session transport] :as msg}]
    (let [the-session (if session
                        (@sessions session)
                        (create-session msg))]
      (if-not the-session
        (t/send transport (response-for msg :status #{:error :unknown-session :done}))
        (let [msg (assoc msg :session the-session)]
          (case op
            "clone" (register-session msg)
            "interrupt" (interrupt-session msg)
            "close" (close-session msg)
            "ls-sessions" (t/send transport (response-for msg :status :done
                                                          :sessions (or (keys @sessions) [])))
            (h msg)))))))

(set-descriptor! #'session
                 {:requires #{}
                  :expects #{}
                  :describe-fn (fn [{:keys [session]}]
                                 (when (and session (instance? clojure.lang.Atom session))
                                   {:current-ns (-> @session (get #'*ns*) str)}))
                  :handles {"clone"
                            {:doc "Clones the current session, returning the ID of the newly-created session."
                             :requires {}
                             :optional {"session" "The ID of the session to be cloned; if not provided, a new session with default bindings is created, and mapped to the returned session ID."}
                             :returns {"new-session" "The ID of the new session."}}
                            "interrupt"
                            {:doc "Attempts to interrupt some executing request. When interruption succeeds, the thread used for execution is killed, and a new thread spawned for the session. While the session middleware ensures that Clojure dynamic bindings are preserved, other ThreadLocals are not. Hence, when running code intimately tied to the current thread identity, it is best to avoid interruptions."
                             :requires {"session" "The ID of the session used to start the request to be interrupted."}
                             :optional {"interrupt-id" "The opaque message ID sent with the request to be interrupted."}
                             :returns {"status" "'interrupted' if a request was identified and interruption will be attempted
'session-idle' if the session is not currently executing any request
'interrupt-id-mismatch' if the session is currently executing a request sent using a different ID than specified by the \"interrupt-id\" value
'session-ephemeral' if the session is an ephemeral session"}}
                            "close"
                            {:doc "Closes the specified session."
                             :requires {"session" "The ID of the session to be closed."}
                             :optional {}
                             :returns {}}
                            "ls-sessions"
                            {:doc "Lists the IDs of all active sessions."
                             :requires {}
                             :optional {}
                             :returns {"sessions" "A list of all available session IDs."}}}})

(defn add-stdin
  "stdin middleware.  Returns a handler that supports a \"stdin\" :op-eration, which
   adds content provided in a :stdin slot to the session's *in* Reader.  Delegates to
   the given handler for other operations.

   Requires the session middleware."
  [h]
  (fn [{:keys [op stdin session transport] :as msg}]
    (cond
      (= op "eval")
      (let [in (-> (meta session) ^LineNumberingPushbackReader (:stdin-reader))]
        (binding [*skipping-eol* true]
          (clojure.main/skip-if-eol in))
        (h msg))
      (= op "stdin")
      (let [q (-> (meta session) ^LinkedBlockingQueue (:input-queue))]
        (if (empty? stdin)
          (.put q -1)
          (locking q
            (doseq [c stdin] (.put q c))))
        (t/send transport (response-for msg :status :done)))
      :else
      (h msg))))

(set-descriptor! #'add-stdin
                 {:requires #{#'session}
                  :expects #{"eval"}
                  :handles {"stdin"
                            {:doc "Add content from the value of \"stdin\" to *in* in the current session."
                             :requires {"stdin" "Content to add to *in*."}
                             :optional {}
                             :returns {"status" "A status of \"need-input\" will be sent if a session's *in* requires content in order to satisfy an attempted read operation."}}}})
