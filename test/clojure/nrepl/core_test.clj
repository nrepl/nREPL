(ns nrepl.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.main]
   [clojure.set :as set]
   [clojure.test :refer [are deftest is testing use-fixtures]]
   [nrepl.core :as nrepl :refer [client
                                 client-session
                                 code
                                 combine-responses
                                 connect
                                 message
                                 new-session
                                 read-response-value
                                 response-seq
                                 response-values
                                 url-connect]]
   [nrepl.ack :as ack]
   [nrepl.middleware.caught :as middleware.caught]
   [nrepl.middleware.print :as middleware.print]
   [nrepl.middleware.session :as session]
   [nrepl.middleware.sideloader :as sideloader]
   [nrepl.misc :refer [uuid]]
   [nrepl.server :as server]
   [nrepl.transport :as transport])
  (:import
   (java.io File Writer)
   java.net.SocketException
   (nrepl.server Server)))

(defmacro when-require [n & body]
  (let [nn (eval n)]
    (try (require nn)
         (catch Throwable e nil))
    (when (find-ns nn)
      `(do ~@body))))

(def transport-fn->protocol
  "Add your transport-fn var here so it can be tested"
  {#'transport/bencode "nrepl"
   #'transport/edn "nrepl+edn"})

;; There is a profile that adds the fastlane dependency and test
;; its transports.
(when-require 'fastlane.core
              (def transport-fn->protocol
                (merge transport-fn->protocol
                       {(find-var 'fastlane.core/transit+msgpack) "transit+msgpack"
                        (find-var 'fastlane.core/transit+json) "transit+json"
                        (find-var 'fastlane.core/transit+json-verbose) "transit+json-verbose"})))

(def ^File project-base-dir (File. (System/getProperty "nrepl.basedir" ".")))

(def ^:dynamic ^nrepl.server.Server  *server* nil)
(def ^{:dynamic true} *transport-fn* nil)

(defn start-server-for-transport-fn
  [transport-fn f]
  (with-open [^Server server (server/start-server :transport-fn transport-fn)]
    (binding [*server* server
              *transport-fn* transport-fn]
      (testing (str (-> transport-fn meta :name) " transport")
        (f))
      (set! *print-length* nil)
      (set! *print-level* nil))))

(def transport-fns
  (keys transport-fn->protocol))

(defn repl-server-fixture
  "This iterates through each transport being tested, starts a server,
   runs the test against that server, then cleans up all sessions."
  [f]
  (doseq [transport-fn transport-fns]
    (start-server-for-transport-fn transport-fn f)
    (session/close-all-sessions!)))

(use-fixtures :each repl-server-fixture)

(defmacro def-repl-test
  [name & body]
  `(deftest ~name
     (with-open [^nrepl.transport.FnTransport
                 transport# (connect :port (:port *server*)
                                     :transport-fn *transport-fn*)]
       (let [~'transport transport#
             ~'client (client transport# Long/MAX_VALUE)
             ~'session (client-session ~'client)
             ~'timeout-client (client transport# 1000)
             ~'timeout-session (client-session ~'timeout-client)
             ~'repl-eval #(message % {:op "eval" :code %2})
             ~'repl-values (comp response-values ~'repl-eval)]
         ~@body))))

(defn- strict-transport? []
  ;; TODO: add transit here.
  (or (= *transport-fn* #'transport/edn)
      (when-require 'fastlane.core
                    (or (= *transport-fn* #'fastlane.core/transit+msgpack)
                        (= *transport-fn* #'fastlane.core/transit+json)
                        (= *transport-fn* #'fastlane.core/transit+json-verbose)))))

(defn- check-response-format
  "checks response against spec, if available it to do a spec check later"
  [resp]
  (when-require 'nrepl.spec
                (when-not (#'clojure.spec.alpha/valid? :nrepl.spec/message resp)
                  (throw (Exception. ^String (#'clojure.spec.alpha/explain-str :nrepl.spec/message resp)))))
  resp)

(defn clean-response
  "Cleans a response to help testing.

  This manually coerces bencode responses to (close) to what the raw EDN
  response is, so we can standardise testing around the richer format. It
  retains strictness on EDN transports.

  - de-identifies the response
  - ensures the status to a set of keywords
  - turn the content of truncated-keys to keywords"
  [resp]
  (let [de-identify
        (fn [resp]
          (dissoc resp :id :session))
        normalize-status
        (fn [resp]
          (if-let [status (:status resp)]
            (assoc resp :status (set (map keyword status)))
            resp))
        ;; This is a good example of a middleware details that's showing through
        keywordize-truncated-keys
        (fn [resp]
          (if (contains? resp ::middleware.print/truncated-keys)
            (update resp ::middleware.print/truncated-keys #(mapv keyword %))
            resp))]
    (cond-> resp
      true                      de-identify
      (not (strict-transport?)) normalize-status
      (not (strict-transport?)) keywordize-truncated-keys
      (strict-transport?)       check-response-format)))

(def-repl-test eval-literals
  (are [literal] (= (binding [*ns* (find-ns 'user)] ; needed for the ::keyword
                      (-> literal read-string eval list))
                    (repl-values client literal))
    "5"
    "0xff"
    "5.1"
    "-2e12"
    "1/4"
    "'symbol"
    "'namespace/symbol"
    ":keyword"
    "::local-ns-keyword"
    ":other.ns/keyword"
    "\"string\""
    "\"string\\nwith\\r\\nlinebreaks\""
    "'(1 2 3)"
    "[1 2 3]"
    "{1 2 3 4}"
    "#{1 2 3 4}")

  (is (= (->> "#\"regex\"" read-string eval list (map str))
         (->> "#\"regex\"" (repl-values client) (map str)))))

(def-repl-test simple-expressions
  (are [expr] (= [(eval expr)] (repl-values client (pr-str expr)))
    '(range 40)
    '(apply + (range 100))))

(def-repl-test defining-fns
  (repl-values client "(defn x [] 6)")
  (is (= [6] (repl-values client "(x)"))))

(defn- dumb-alternative-eval
  [form]
  (let [result (eval form)]
    (if (number? result)
      (- result)
      result)))

(def-repl-test use-alternative-eval-fn
  (is (= {:value ["-124750"]}
         (-> (message timeout-client {:op "eval" :eval "nrepl.core-test/dumb-alternative-eval"
                                      :code "(reduce + (range 500))"})
             combine-responses
             (select-keys [:value])))))

(def-repl-test source-tracking-eval
  (let [sym (name (gensym))
        request {:op "eval" :ns "user" :code (format "(def %s 1000)" sym)
                 :file "test.clj" :line 42 :column 10}
        _ (doall (message timeout-client request))
        meta (meta (resolve (symbol "user" sym)))]
    (is (= (:file meta) "test.clj"))
    (is (= (:line meta) 42))
    (is (= (:column meta) 10))))

(def-repl-test no-code
  (is (= {:status #{:error :no-code :done}}
         (-> (message timeout-client {:op "eval"})
             combine-responses
             clean-response
             (select-keys [:status])))))

(def-repl-test unknown-op
  (is (= {:op "abc" :status #{:error :unknown-op :done}}
         (-> (message timeout-client {:op "abc"})
             combine-responses
             clean-response
             (select-keys [:op :status])))))

(def-repl-test session-lifecycle
  (is (= #{:error :unknown-session :done}
         (-> (message timeout-client {:session "00000000-0000-0000-0000-000000000000"})
             combine-responses
             clean-response
             :status)))
  (let [session-id (new-session timeout-client)
        session-alive? #(contains? (-> (message timeout-client {:op "ls-sessions"})
                                       combine-responses
                                       :sessions
                                       set)
                                   session-id)]
    (is session-id)
    (is (session-alive?))
    (is (= #{:done :session-closed}
           (-> (message timeout-client {:op "close" :session session-id})
               combine-responses
               clean-response
               :status)))
    (is (not (session-alive?)))))

(def-repl-test separate-value-from-*out*
  (is (= {:value [nil] :out "5\n"}
         (-> (map read-response-value (repl-eval client "(println 5)"))
             combine-responses
             (select-keys [:value :out])))))

(def-repl-test sessionless-*out*
  (is (= "5\n:foo\n"
         (-> (repl-eval client "(println 5)(println :foo)")
             combine-responses
             :out))))

(def-repl-test session-*out*
  (is (= "5\n:foo\n"
         (-> (repl-eval session "(println 5)(println :foo)")
             combine-responses
             :out))))

(def-repl-test error-on-lazy-seq-with-side-effects
  (let [expression '(let [foo (fn [] (map (fn [x]
                                            (println x)
                                            (throw (Exception. "oops")))
                                          [1 2 3]))]
                      (foo))
        results (-> (repl-eval session (pr-str expression))
                    combine-responses)]
    (is (= "1\n" (:out results)))
    (is (re-seq #"oops" (:err results)))))

(def-repl-test cross-transport-*out*
  (let [sid (-> session meta ::nrepl/taking-until :session)]
    (with-open [^nrepl.transport.FnTransport
                transport2 (connect :port (:port *server*)
                                    :transport-fn *transport-fn*)]
      (transport/send transport2 {:op "eval" :code "(println :foo)"
                                  "session" sid})
      (is (= [{:out ":foo\n"}
              {:ns "user" :value "nil"}
              {:status #{:done}}]
             (->> (repeatedly #(transport/recv transport2 100))
                  (take-while identity)
                  (mapv clean-response)))))))

(def-repl-test streaming-out
  (is (= (for [x (range 10)]
           (str x \newline))
         (->> (repl-eval client "(dotimes [x 10] (println x))")
              (map :out)
              (remove nil?)))))

(def-repl-test session-*out*-writer-length-translation
  (is (= "#inst \"2013-02-11T12:13:44.000+00:00\"\n"
         (-> (repl-eval session
                        (code (println (doto (java.util.GregorianCalendar. 2013 1 11 12 13 44)
                                         (.setTimeZone (java.util.TimeZone/getTimeZone "GMT"))))))
             combine-responses
             :out))))

(def-repl-test streaming-out-without-explicit-flushing
  (is (= ["(0 1 "
          "2 3 4"
          " 5 6 "
          "7 8 9"
          " 10)"]
         ;; new session
         (->> (message client {:op "eval" :out-limit 5 :code "(print (range 11))"})
              (map :out)
              (remove nil?))
         ;; existing session
         (->> (message session {:op "eval" :out-limit 5 :code "(print (range 11))"})
              (map :out)
              (remove nil?)))))

(def-repl-test reader-conditional-option
  (is (= ["#?(:clj (+ 1 2) :cljs (+ 2 3))"]
         (->> (message client {:op "eval" :read-cond :preserve
                               :code "#?(:clj (+ 1 2) :cljs (+ 2 3))"})
              combine-responses
              :value)))
  (is (= ["3"]
         (->> (message client {:op "eval" :code "#?(:clj (+ 1 2) :cljs (+ 2 3))"})
              combine-responses
              :value))))

(def-repl-test ensure-whitespace-prints
  (is (= " \t \n \f \n" (->> (repl-eval client "(println \" \t \n \f \")")
                             combine-responses
                             :out))))

(def-repl-test streamed-printing
  (testing "multiple forms"
    (let [responses (->> (message client {:op "eval"
                                          :code (code (range 10)
                                                      (range 10))
                                          ::middleware.print/stream? 1})
                         (mapv clean-response))]
      (is (= [{:value "(0 1 2 3 4 5 6 7 8 9)"}
              {:ns "user"}
              {:value "(0 1 2 3 4 5 6 7 8 9)"}
              {:ns "user"}
              {:status #{:done}}]
             responses))))

  (testing "*out* still handled correctly"
    (let [responses (->> (message client {:op "eval"
                                          :code (code (->> (range 2)
                                                           (map println)))
                                          ::middleware.print/stream? 1})
                         (mapv clean-response))]
      (is (= [{:out "0\n"}
              {:out "1\n"}
              {:value "(nil nil)"}
              {:ns "user"}
              {:status #{:done}}]
             responses))))

  ;; Making the buffer size large expose the odd of interrupting half way through
  ;; a message being written, which was the cause of https://github.com/nrepl/nrepl/issues/132
  (testing "interruptible"
    (let [eval-responses (->> (message session {:op "eval"
                                                :code (code (range))
                                                ::middleware.print/stream? 1
                                                ::middleware.print/buffer-size 100000})
                              (map clean-response))
          _ (Thread/sleep 100)
          interrupt-responses (->> (message session {:op "interrupt"})
                                   (mapv clean-response))]
      ;; check the interrupt succeeded first; otherwise eval-responses will not terminate
      (is (= [{:status #{:done}}] interrupt-responses))
      (is (-> eval-responses
              first
              ^String (:value)
              (.startsWith "(0 1 2 3")))
      (is (= {:status #{:done :interrupted}} (last eval-responses))))))

(def-repl-test session-return-recall
  (testing "sessions persist across connections"
    (dorun (repl-values session (code
                                 (apply + (range 6))
                                 (str 12 \c)
                                 (keyword "hello"))))
    (with-open [^nrepl.transport.FnTransport
                separate-connection (connect :port (:port *server*)
                                             :transport-fn *transport-fn*)]
      (let [history [[15 "12c" :hello]]
            sid (-> session meta :nrepl.core/taking-until :session)
            sc-session (-> separate-connection
                           (nrepl/client 1000)
                           (nrepl/client-session :session sid))]
        (is (= history (repl-values sc-session "[*3 *2 *1]")))
        (is (= history (repl-values sc-session "*1"))))))

  (testing "without a session id, REPL-bound vars like *1 have default values"
    (is (= [nil] (repl-values client "*1")))))

(def-repl-test session-set!
  (dorun (repl-eval session (code
                             (set! *compile-path* "badpath")
                             (set! *warn-on-reflection* true))))
  (is (= [["badpath" true]] (repl-values session (code [*compile-path* *warn-on-reflection*])))))

(def-repl-test exceptions
  (let [{:keys [status ^String err value]} (-> session
                                               (repl-eval "(throw (Exception. \"bad, bad code\"))")
                                               combine-responses
                                               clean-response)]
    (is (= #{:eval-error :done} status))
    (is (nil? value))
    (is (.contains err "bad, bad code"))
    (is (= [true] (repl-values session "(.contains (str *e) \"bad, bad code\")")))))

(def-repl-test multiple-expressions-return
  (is (= [5 18] (repl-values session "5 (/ 5 0) (+ 5 6 7)"))))

(def-repl-test return-on-incomplete-expr
  (let [{:keys [out status value]} (clean-response (combine-responses (repl-eval session "(missing paren")))]
    (is (nil? value))
    (is (= #{:done :eval-error} status))
    ;; TODO avoid regex error if there's no exception found. Can be misleading
    (is (re-seq #"EOF while reading" (first (repl-values session "(-> *e Throwable->map :cause)"))))))

(def-repl-test switch-ns
  (is (= "otherns" (-> (repl-eval session "(ns otherns) (defn function [] 12)")
                       combine-responses
                       :ns)))
  (is (= [12] (repl-values session "(function)")))
  (repl-eval session "(in-ns 'user)")
  (is (= [12] (repl-values session "(otherns/function)"))))

(def-repl-test switch-ns-2
  (is (= "otherns" (-> (repl-eval session (code
                                           (ns otherns)
                                           (defn function [] 12)))
                       combine-responses
                       :ns)))
  (is (= [12] (repl-values session "(function)")))
  (repl-eval session "(in-ns 'user)")
  (is (= [12] (repl-values session "(otherns/function)")))
  (is (= "user" (-> (repl-eval session "nil") combine-responses :ns))))

(def-repl-test explicit-ns
  (is (= "user" (-> (repl-eval session "nil") combine-responses :ns)))
  (is (= "baz" (-> (repl-eval session (code
                                       (def bar 5)
                                       (ns baz)))
                   combine-responses
                   :ns)))
  (is (= [5] (response-values (message session {:op "eval" :code "bar" :ns "user"}))))
  ;; NREPL-72: :ns argument to eval shouldn't affect *ns* outside of the scope of that evaluation
  (is (= "baz" (-> (repl-eval session "5") combine-responses :ns))))

(def-repl-test error-on-nonexistent-ns
  (is (= #{:error :namespace-not-found :done}
         (-> (message timeout-client {:op "eval" :code "(+ 1 1)" :ns (name (gensym))})
             combine-responses
             clean-response
             :status))))

(def-repl-test proper-response-ordering
  (is (= [[nil "100\n"] ; printed number
          ["nil" nil] ; return val from println
          ["42" nil]  ; return val from `42`
          [nil nil]]  ; :done
         (map (juxt :value :out) (repl-eval client "(println 100) 42")))))

(def-repl-test interrupt
  (testing "ephemeral session"
    (is (= #{:error :session-ephemeral :done}
           (:status (clean-response (first (message client {:op "interrupt"}))))
           (:status (clean-response (first (message client {:op "interrupt" :interrupt-id "foo"})))))))

  (testing "registered session"
    (is (= #{:done :session-idle}
           (:status (clean-response (first (message session {:op "interrupt"}))))
           (:status (clean-response (first (message session {:op "interrupt" :interrupt-id "foo"}))))))

    (let [resp (message session {:op "eval" :code (code (do
                                                          (def halted? true)
                                                          halted?
                                                          (Thread/sleep 30000)
                                                          (def halted? false)))})]
      (Thread/sleep 100)
      (is (= #{:done :error :interrupt-id-mismatch}
             (:status (clean-response (first (message session {:op "interrupt" :interrupt-id "foo"}))))))
      (is (= #{:done} (-> session (message {:op "interrupt"}) first clean-response :status)))
      (is (= #{} (reduce disj #{:done :interrupted} (-> resp combine-responses clean-response :status))))
      (is (= [true] (repl-values session "halted?")))))
  (testing "interrupting a sleep"
    (let [resp (message session {:op "eval"
                                 :code (code
                                        (do
                                          (Thread/sleep 10000)
                                          "Done"))})]
      (Thread/sleep 100)
      (is (= #{:done}
             (->> session
                  (#(message % {:op "interrupt"}))
                  first
                  clean-response
                  :status)))
      (let [r (->> resp
                   combine-responses
                   clean-response)]
        (is (= #{:done :eval-error :interrupted}
               (:status r)))
        (is (= "class java.lang.InterruptedException"
               (:ex r))))))
  (testing "interruptable code"
    (let [resp (message session {:op "eval"
                                 :code (code
                                        (do
                                          (defn run []
                                            (if (.isInterrupted (Thread/currentThread))
                                              "Clean stop!"
                                              (recur)))
                                          (run)))})]
      (Thread/sleep 100)
      (is (= #{:done}
             (->> session
                  (#(message % {:op "interrupt"}))
                  first
                  clean-response
                  :status)))
      (let [r (->> resp
                   combine-responses
                   clean-response)]
        (is (= #{:done :interrupted}
               (:status r)))
        (is (= "Clean stop!"
               (read-string (last (:value r)))))))))

;; NREPL-66: ensure that bindings of implementation vars aren't captured by user sessions
;; (https://github.com/clojure-emacs/cider/issues/785)
(def-repl-test ensure-no-*msg*-capture
  (let [[r1 r2 :as results] (repeatedly 2 #(repl-eval session "(println :foo)"))
        [ids ids2] (map #(set (map :id %)) results)
        [out1 out2] (map #(-> % combine-responses :out) results)]
    (is (empty? (clojure.set/intersection ids ids2)))
    (is (= ":foo\n" out1 out2))))

(def-repl-test read-timeout
  (is (nil? (repl-values timeout-session "(Thread/sleep 1100) :ok")))
  ;; just getting the values off of the wire so the server side doesn't
  ;; toss a spurious stack trace when the client disconnects
  (is (= [nil :ok] (->> (repeatedly #(transport/recv transport 500))
                        (take-while (complement nil?))
                        response-values))))

(def-repl-test concurrent-message-handling
  (testing "multiple messages can be handled on the same connection concurrently"
    (let [sessions (doall (repeatedly 3 #(client-session client)))
          start-time (System/currentTimeMillis)
          elapsed-times (map (fn [session eval-duration]
                               (let [expr (pr-str `(Thread/sleep ~eval-duration))
                                     responses (message session {:op "eval" :code expr})]
                                 (future
                                   (is (= [nil] (response-values responses)))
                                   (- (System/currentTimeMillis) start-time))))
                             sessions
                             [2000 1000 0])]
      (is (apply > (map deref (doall elapsed-times)))))))

(def-repl-test ensure-transport-closeable
  (is (= [5] (repl-values session "5")))
  (is (instance? java.io.Closeable transport))
  (.close transport)
  (is (thrown? java.net.SocketException (repl-values session "5"))))

;; test is flaking on hudson, but passing locally! :-X
(def-repl-test ensure-server-closeable
  (.close *server*)
  (Thread/sleep 100)
  (is (thrown? java.net.ConnectException (connect :port (:port *server*)))))

;; wasn't added until Clojure 1.3.0
(defn- root-cause
  "Returns the initial cause of an exception or error by peeling off all of
  its wrappers"
  ^Throwable
  [^Throwable t]
  (loop [cause t]
    (if-let [cause (.getCause cause)]
      (recur cause)
      cause)))

(defn- disconnection-exception?
  [e]
  ;; thrown? should check for the root cause!
  (and (instance? SocketException (root-cause e))
       (re-matches #".*(lost.*connection|socket closed).*" (.getMessage (root-cause e)))))

(deftest transports-fail-on-disconnects
  (testing "Ensure that transports fail ASAP when the server they're connected to goes down."
    (let [^Server server (server/start-server :transport-fn *transport-fn*)
          ^nrepl.transport.FnTransport
          transport (connect :port (:port server)
                             :transport-fn *transport-fn*)]
      (transport/send transport {:op "eval" :code "(+ 1 1)"})

      (let [reader (future (while true (transport/recv transport)))]
        (Thread/sleep 100)
        (.close server)
        (Thread/sleep 100)
        (try
          (deref reader 1000 :timeout)
          (assert false "A reader started prior to the server closing should throw an error...")
          (catch Throwable e
            (is (disconnection-exception? e)))))

      (is (thrown? SocketException (transport/recv transport)))
      ;; The next `Thread/sleep` is needed or the test would be fleaky
      ;; for some transports that don't throw an exception the first time
      ;; a message is sent after the server is closed.
      (try
        (transport/send transport {:op "eval" :code "(+ 5 1)"})
        (catch Throwable t))
      (Thread/sleep 100)
      (is (thrown? SocketException (transport/send transport {:op "eval" :code "(+ 5 1)"}))))))

(deftest server-starts-with-minimal-configuration
  (testing "Ensure server starts with minimal configuration"
    (let [server (server/start-server)
          ^nrepl.transport.FnTransport
          transport (connect :port (:port server))
          client (client transport Long/MAX_VALUE)]
      (is (= ["3"]
             (-> (message client {:op "eval" :code "(- 4 1)"})
                 combine-responses
                 :value))))))

(def-repl-test clients-fail-on-disconnects
  (testing "Ensure that clients fail ASAP when the server they're connected to goes down."
    (let [resp (repl-eval client "1 2 3 4 5 6 7 8 9 10")]
      (is (= "1" (-> resp first :value)))
      (Thread/sleep 1000)
      (.close *server*)
      (Thread/sleep 1000)
      (try
        ;; these responses were on the wire before the remote transport was closed
        (is (> 20 (count resp)))
        (transport/recv transport)
        (assert false "reads after the server is closed should fail")
        (catch Throwable t
          (is (disconnection-exception? t)))))

    ;; TODO: as noted in transports-fail-on-disconnects, *sometimes* two sends are needed
    ;; to trigger an exception on send to an unavailable server
    (try (repl-eval session "(+ 1 1)") (catch Throwable _))
    (Thread/sleep 100)
    (is (thrown? SocketException (repl-eval session "(+ 1 1)")))))

(def-repl-test request-*in*
  (is (= '((1 2 3)) (response-values (for [resp (repl-eval session "(read)")]
                                       (do
                                         (when (-> resp clean-response :status (contains? :need-input))
                                           (session {:op "stdin" :stdin "(1 2 3)"}))
                                         resp)))))

  (session {:op "stdin" :stdin "a\nb\nc\n"})
  (doseq [x "abc"]
    (is (= [(str x)] (repl-values session "(read-line)")))))

(def-repl-test request-*in*-eof
  (is (= nil (response-values (for [resp (repl-eval session "(read)")]
                                (do
                                  (when (-> resp clean-response :status (contains? :need-input))
                                    (session {:op "stdin" :stdin []}))
                                  resp))))))

(def-repl-test request-multiple-read-newline-*in*
  (is (= '(:ohai) (response-values (for [resp (repl-eval session "(read)")]
                                     (do
                                       (when (-> resp clean-response :status (contains? :need-input))
                                         (session {:op "stdin" :stdin ":ohai\n"}))
                                       resp)))))

  (session {:op "stdin" :stdin "a\n"})
  (is (= ["a"] (repl-values session "(read-line)"))))

(def-repl-test request-multiple-read-with-buffered-newline-*in*
  (is (= '(:ohai) (response-values (for [resp (repl-eval session "(read)")]
                                     (do
                                       (when (-> resp clean-response :status (contains? :need-input))
                                         (session {:op "stdin" :stdin ":ohai\na\n"}))
                                       resp)))))

  (is (= ["a"] (repl-values session "(read-line)"))))

(def-repl-test request-multiple-read-objects-*in*
  (is (= '(:ohai) (response-values (for [resp (repl-eval session "(read)")]
                                     (do
                                       (when (-> resp clean-response :status (contains? :need-input))
                                         (session {:op "stdin" :stdin ":ohai :kthxbai\n"}))
                                       resp)))))

  (is (= [" :kthxbai"] (repl-values session "(read-line)"))))

(def-repl-test test-url-connect
  (with-open [^nrepl.transport.FnTransport
              conn (url-connect (str (transport-fn->protocol *transport-fn*)
                                     "://127.0.0.1:"
                                     (:port *server*)))]
    (transport/send conn {:op "eval" :code "(+ 1 1)"})
    (is (= [2] (response-values (response-seq conn 100))))))

(deftest test-ack
  (with-open [^Server s (server/start-server :transport-fn *transport-fn*
                                             :handler (-> (server/default-handler)
                                                          ack/handle-ack))]
    (ack/reset-ack-port!)
    (with-open [^Server s2 (server/start-server :transport-fn *transport-fn*
                                                :ack-port (:port s))]
      (is (= (:port s2) (ack/wait-for-ack 10000))))))

(def-repl-test agent-await
  (is (= [42] (repl-values session (code (let [a (agent nil)]
                                           (send a (fn [_] (Thread/sleep 1000) 42))
                                           (await a)
                                           @a))))))

(deftest cloned-session-*1-binding
  (let [port (:port *server*)
        ^nrepl.transport.FnTransport
        conn (connect :port port :transport-fn *transport-fn*)
        client (nrepl/client conn 1000)
        sess (nrepl/client-session client)
        sess-id (->> (sess {:op "eval"
                            :code "(+ 1 4)"})
                     last
                     :session)
        new-sess-id (->> (sess {:session sess-id
                                :op "clone"})
                         last
                         :session)
        cloned-sess (nrepl/client-session client :session new-sess-id)
        cloned-sess-*1 (->> (cloned-sess {:session new-sess-id
                                          :op "eval"
                                          :code "*1"})
                            first
                            :value)]
    (is (= "5" cloned-sess-*1))))

(def-repl-test print-namespace-maps-binding
  (when (resolve '*print-namespace-maps*)
    (let [set-true (dorun (repl-eval session "(set! *print-namespace-maps* true)"))
          true-val (first (repl-values session "*print-namespace-maps*"))
          set-false (dorun (repl-eval session "(set! *print-namespace-maps* false)"))
          false-val (first (repl-values session "*print-namespace-maps*"))]
      (is (= true true-val))
      (is (= false false-val)))))

(def-repl-test interrupt-load-file
  (let [resp (message session {:op "load-file"
                               :file (slurp (File. project-base-dir "load-file-test/nrepl/load_file_sample2.clj"))
                               :file-path "nrepl/load_file_sample2.clj"
                               :file-name "load_file_sample2.clj"})]
    (Thread/sleep 100)
    (is (= #{:done}
           (->> session
                (#(message % {:op "interrupt"}))
                first
                clean-response
                :status)))
    (is (= #{:done :eval-error :interrupted}
           (->> resp
                combine-responses
                clean-response
                :status)))))

(def-repl-test stdout-stderr
  (are [result expr] (= result (-> (repl-eval client expr)
                                   (combine-responses)
                                   (select-keys [:ns :out :err :value])))
    {:ns "user" :out "5 6 7 \n 8 9 10\n" :value ["nil"]}
    (code (println 5 6 7 \newline 8 9 10))

    {:ns "user" :err "user/foo\n" :value ["nil"]}
    (code (binding [*out* *err*]
            (prn 'user/foo)))

    {:ns "user" :err "problem" :value [":value"]}
    (code (do (.write *err* "problem")
              :value)))

  (is (re-seq #"Divide by zero" (:err (first (repl-eval client (code (/ 1 0))))))))

(def-repl-test read-error-short-circuits-execution
  (testing "read error prevents the remaining code from being read and executed"
    (let [{:keys [err] :as resp} (-> (repl-eval client "(comment {:a} (println \"BOOM!\"))")
                                     (combine-responses))]
      (if (and (= (:major *clojure-version*) 1)
               (<= (:minor *clojure-version*) 9))
        (is (re-matches #"(?s)^RuntimeException Map literal must contain an even number of forms[^\n]+\n$" err))
        (is (re-matches #"(?s)^Syntax error reading source at[^\n]+\nMap literal must contain an even number of forms\n" err)))
      (is (not (contains? resp :out)))
      (is (not (contains? resp :value)))))

  (testing "exactly one read error is produced even if there is remaining code in the message"
    (let [{:keys [err] :as resp} (-> (repl-eval client ")]} 42")
                                     (combine-responses))]
      (is (re-find #"Unmatched delimiter: \)" err))
      (is (not (re-find #"Unmatched delimiter: \]" err)))
      (is (not (re-find #"Unmatched delimiter: \}" err)))
      (is (not (contains? resp :out)))
      (is (not (contains? resp :value))))))

(defn custom-repl-caught
  [^Throwable t]
  (binding [*out* *err*]
    (println "foo" (type t))))

(def-repl-test caught-options
  (testing "bad symbol should fall back to default"
    (let [[resp1 resp2 resp3 resp4] (->> (message session {:op "eval"
                                                           :code (code (first 1))
                                                           ::middleware.caught/caught "my.missing.ns/repl-caught"})
                                         (mapv clean-response))]
      (is (= {::middleware.caught/error "Couldn't resolve var my.missing.ns/repl-caught"
              :status #{:nrepl.middleware.caught/error}}
             resp1))
      (is (re-find #"IllegalArgumentException" (:err resp2)))
      (is (= {:status #{:eval-error}
              :ex "class java.lang.IllegalArgumentException"
              :root-ex "class java.lang.IllegalArgumentException"}
             resp3))
      (is (= {:status #{:done}}
             resp4))))

  (testing "custom symbol should be used"
    (is (= [{:err "foo java.lang.IllegalArgumentException\n"}
            {:status #{:eval-error}
             :ex "class java.lang.IllegalArgumentException"
             :root-ex "class java.lang.IllegalArgumentException"}
            {:status #{:done}}]
           (->> (message session {:op "eval"
                                  :code (code (first 1))
                                  ::middleware.caught/caught `custom-repl-caught})
                (mapv clean-response)))))

  (testing "::print? option"
    (let [[resp1 resp2 resp3] (->> (message session {:op "eval"
                                                     :code (code (first 1))
                                                     ::middleware.caught/print? 1})
                                   (mapv clean-response))]
      (is (re-find #"IllegalArgumentException" (:err resp1)))
      (is (re-find #"IllegalArgumentException" (::middleware.caught/throwable resp2)))
      (is (= {:status #{:eval-error}
              :ex "class java.lang.IllegalArgumentException"
              :root-ex "class java.lang.IllegalArgumentException"}
             (dissoc resp2 ::middleware.caught/throwable)))
      (is (= {:status #{:done}}
             resp3)))

    (let [[resp1 resp2 resp3] (->> (message session {:op "eval"
                                                     :code (code (first 1))
                                                     ::middleware.caught/caught `custom-repl-caught
                                                     ::middleware.caught/print? 1})
                                   (mapv clean-response))]
      (is (= {:err "foo java.lang.IllegalArgumentException\n"}
             resp1))
      (is (re-find #"IllegalArgumentException" (::middleware.caught/throwable resp2)))
      (is (= {:status #{:eval-error}
              :ex "class java.lang.IllegalArgumentException"
              :root-ex "class java.lang.IllegalArgumentException"}
             (dissoc resp2 ::middleware.caught/throwable)))
      (is (= {:status #{:done}}
             resp3)))))

(defn custom-session-repl-caught
  [^Throwable t]
  (binding [*out* *err*]
    (println "bar" (.getMessage t))))

(def-repl-test session-caught-options
  (testing "setting *caught-fn* works"
    (is (= [{:ns "user" :value "#'nrepl.core-test/custom-session-repl-caught"}
            {:status #{:done}}]
           (->> (message session {:op "eval"
                                  :code (code (set! nrepl.middleware.caught/*caught-fn* (resolve `custom-session-repl-caught)))})
                (mapv clean-response))))

    (is (= [{:err "bar Divide by zero\n"}
            {:status #{:eval-error}
             :ex "class java.lang.ArithmeticException"
             :root-ex "class java.lang.ArithmeticException"}
            {:status #{:done}}]
           (->> (message session {:op "eval"
                                  :code (code (/ 1 0))})
                (mapv clean-response)))))

  (testing "request can still override *caught-fn*"
    (is (= [{:err "foo java.lang.ArithmeticException\n"}
            {:status #{:eval-error}
             :ex "class java.lang.ArithmeticException"
             :root-ex "class java.lang.ArithmeticException"}
            {:status #{:done}}]
           (->> (message session {:op "eval"
                                  :code (code (/ 1 0))
                                  ::middleware.caught/caught `custom-repl-caught})
                (mapv clean-response)))))

  (testing "request can still provide ::print? option"
    (let [[resp1 resp2 resp3] (->> (message session {:op "eval"
                                                     :code (code (/ 1 0))
                                                     ::middleware.caught/print? 1})
                                   (mapv clean-response))]
      (is (re-find #"Divide by zero" (:err resp1)))
      (is (re-find #"Divide by zero" (::middleware.caught/throwable resp2)))
      (is (= {:status #{:eval-error}
              :ex "class java.lang.ArithmeticException"
              :root-ex "class java.lang.ArithmeticException"}
             (dissoc resp2 ::middleware.caught/throwable)))
      (is (= {:status #{:done}}
             resp3)))

    (let [[resp1 resp2 resp3] (->> (message session {:op "eval"
                                                     :code (code (/ 1 0))
                                                     ::middleware.caught/caught `custom-repl-caught
                                                     ::middleware.caught/print? 1})
                                   (mapv clean-response))]
      (is (= {:err "foo java.lang.ArithmeticException\n"}
             resp1))
      (is (re-find #"Divide by zero" (::middleware.caught/throwable resp2)))
      (is (= {:status #{:eval-error}
              :ex "class java.lang.ArithmeticException"
              :root-ex "class java.lang.ArithmeticException"}
             (dissoc resp2 ::middleware.caught/throwable)))
      (is (= {:status #{:done}}
             resp3)))))

(def sideloader-tests-lookup
  {["resource" "hello"]
   "Hello nREPL"

   ["resource" "foo/bar.clj"]
   "(ns foo.bar) (defn hello [] \"Hello nREPL\")"

   ["class" "nrepl.HelloFactory"]
   (File. project-base-dir "test-resources/HelloFactory.class")})

(defn string->content
  [^String str]
  (if str
    (-> str
        (.getBytes "UTF-8")
        java.io.ByteArrayInputStream.
        sideloader/base64-encode)
    ""))

(defn file->content
  [file]
  (if file
    (-> file
        io/input-stream
        sideloader/base64-encode)
    ""))

(defn message-with-sideloader
  [a-client msg sl-lookup]
  (let [session-id    (new-session a-client)
        sideloader-id (uuid)]
    (future (->> (a-client {:id      sideloader-id
                            :session session-id
                            :op      "sideloader-start"})
                 (filter #(= (:id %) sideloader-id))
                 (run! (fn [{:keys [status type name]}]
                         (when (contains? (set (map keyword status))
                                          :sideloader-lookup)
                           (a-client {:session session-id
                                      :id      sideloader-id
                                      :op      "sideloader-provide"
                                      :content (case type
                                                 "resource"
                                                 (string->content (get sl-lookup [type name]))
                                                 "class"
                                                 (file->content (get sl-lookup [type name])))
                                      :type    type
                                      :name    name}))))))
    (Thread/sleep 100)
    (message a-client (assoc msg :session session-id))))

(defn eval-with-sideloader
  [a-client code sideloader-lookup]
  (->> (message-with-sideloader a-client
                                {:op "eval" :code code}
                                sideloader-lookup)
       (map clean-response)
       (map read-response-value)
       combine-responses))

(def-repl-test sideloader-test
  (testing "Loading resources"
    (let [code-snippet (code (slurp (.. (Thread/currentThread)
                                        (getContextClassLoader)
                                        (getResourceAsStream "hello"))))
          rsp          (eval-with-sideloader timeout-client
                                             code-snippet
                                             sideloader-tests-lookup)]
      (is (= ["Hello nREPL"]
             (:value rsp)))))
  (testing "Loading source as resource (clj)"
    (let [code-snippet (code
                        (require '[foo.bar :as bar])
                        (bar/hello))
          rsp          (eval-with-sideloader timeout-client
                                             code-snippet
                                             sideloader-tests-lookup)]
      (is (= [nil "Hello nREPL"]
             (:value rsp)))))
  (testing "Loading class"
    (let [code-snippet (code
                        (do
                          (import 'nrepl.HelloFactory)
                          nil)
                        (nrepl.HelloFactory/hello))
          rsp          (eval-with-sideloader timeout-client
                                             code-snippet
                                             sideloader-tests-lookup)]
      (is (= [nil "Hello nREPL"]
             (:value rsp))))))

(def dynamic-test-lookup
  {["resource" "foo.clj"]
   "(ns foo) (def bar :bar)"

   ["resource" "ping.clj"]
   (slurp
    (File. project-base-dir "test-resources/ping.clj"))

   ["resource" "ping_imp.clj"]
   (slurp
    (File. project-base-dir "test-resources/ping_imp.clj"))})

(def-repl-test dynamic-middleware-test
  (let [rsp (->> (message client {:op "ls-middleware"})
                 (map clean-response)
                 combine-responses)]
    (is (contains? (set (:middleware rsp))
                   "#'nrepl.middleware.session/session"))))

(def-repl-test dynamic-middleware+sideloading
  (let [rsp1 (->> (message-with-sideloader client
                                           {:op         "add-middleware"
                                            :middleware ["ping/wrap-ping"]}
                                           dynamic-test-lookup)
                  (map clean-response)
                  combine-responses)
        rsp2 (->> (message client {:op "ping"})
                  (map clean-response)
                  combine-responses)]
    (is (= {:status #{:done}} rsp1))
    (is (= {:status #{:done} :pong "pong"} rsp2))))

(def-repl-test dynamic-middleware+deferred+sideloading
  (let [rsp1 (->> (message-with-sideloader client
                                           {:op               "add-middleware"
                                            :middleware       ["ping/wrap-ping"]
                                            :extra-namespaces ["ping-imp"]}
                                           dynamic-test-lookup)
                  (map clean-response)
                  combine-responses)
        rsp2 (->> (message client {:op "deferred-ping"})
                  (map clean-response)
                  combine-responses)]
    (is (= {:status #{:done}} rsp1))
    (is (= {:status #{:done} :pong "pong-deferred"} rsp2))))

;; This test checks if, within the same session, the ContextClassLoader and
;; RT/baseLoader have a common DynamicClassLoader ancestor. This is what
;; pomegranate (and other classpath modifying libs?) target to enable hotloading

;; This is from pomegranate
(defn classloader-hierarchy
  "Returns a seq of classloaders, with the tip of the hierarchy first.
   Uses the current thread context ClassLoader as the tip ClassLoader
   if one is not provided."
  ([] (classloader-hierarchy (.. Thread currentThread getContextClassLoader)))
  ([tip]
   (->> tip
        (iterate #(.getParent ^ClassLoader %))
        (take-while boolean))))

;; This test is broken for Java 8 at the moment.
(def-repl-test ^{:min-java-version "11.0"} hotloading-common-classloader-test
  (testing "Check if RT/baseLoader and ContexClassLoader have a common DCL ancestor"
    (let [dcls (fn [str] ;; parses the string output of classloader-hierarch
                 (set (map (fn [[_ id]] id)
                           (re-seq #"DynamicClassLoader@([0-9a-f]{8})" (or str "")))))]
      (let [resp1 (->> (message session {:op   "eval"
                                         :code "(#'nrepl.core-test/classloader-hierarchy (clojure.lang.RT/baseLoader))"})
                       (map clean-response)
                       combine-responses)
            resp2 (->> (message session {:op   "eval"
                                         :code "(#'nrepl.core-test/classloader-hierarchy (.. Thread currentThread getContextClassLoader))"})
                       (map clean-response)
                       combine-responses)]
        (is (not-empty
             (set/intersection
              (dcls (first (:value resp2)))
              (dcls (first (:value resp1))))))))))
