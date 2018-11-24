(ns nrepl.core-test
  (:require
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
   [nrepl.server :as server]
   [nrepl.transport :as transport])
  (:import
   java.io.File
   java.net.SocketException))

(def project-base-dir (File. (System/getProperty "nrepl.basedir" ".")))

(def ^{:dynamic true} *server* nil)

(defn call-with-bound-repl-server
  ([f] (call-with-bound-repl-server nil f))
  ([args f]
   (with-open [server (apply server/start-server args)]
     (binding [*server* server]
       (f))
     (set! *print-length* nil)
     (set! *print-level* nil))))

(def repl-server-fixture call-with-bound-repl-server)

(use-fixtures :each repl-server-fixture)

(defmacro def-repl-test
  [name & body]
  `(deftest ~(with-meta name {:private true})
     (with-open [transport# (connect :port (:port *server*))]
       (let [~'transport transport#
             ~'client (client transport# Long/MAX_VALUE)
             ~'session (client-session ~'client)
             ~'timeout-client (client transport# 1000)
             ~'timeout-session (client-session ~'timeout-client)
             ~'repl-eval #(message % {:op :eval :code %2})
             ~'repl-values (comp response-values ~'repl-eval)]
         ~@body))))

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
         (-> (message timeout-client {:op :eval :eval "nrepl.core-test/dumb-alternative-eval"
                                      :code "(reduce + (range 500))"})
             combine-responses
             (select-keys [:value])))))

(def-repl-test source-tracking-eval
  (let [sym (name (gensym))
        request {:op :eval :ns "user" :code (format "(def %s 1000)" sym)
                 :file "test.clj" :line 42 :column 10}
        _ (doall (message timeout-client request))
        meta (meta (resolve (symbol "user" sym)))]
    (is (= (:file meta) "test.clj"))
    (is (= (:line meta) 42))
    (is (= (:column meta) 10))))

(def-repl-test no-code
  (is (= {:status #{"error" "no-code" "done"}}
         (-> (message timeout-client {:op "eval"}) combine-responses (select-keys [:status])))))

(def-repl-test unknown-op
  (is (= {:op "abc" :status #{"error" "unknown-op" "done"}}
         (-> (message timeout-client {:op :abc}) combine-responses (select-keys [:op :status])))))

(def-repl-test session-lifecycle
  (is (= #{"error" "unknown-session" "done"}
         (-> (message timeout-client {:session "abc"}) combine-responses :status)))
  (let [session-id (new-session timeout-client)
        session-alive? #(contains? (-> (message timeout-client {:op :ls-sessions})
                                       combine-responses
                                       :sessions
                                       set)
                                   session-id)]
    (is session-id)
    (is (session-alive?))
    (is (= #{"done" "session-closed"} (-> (message timeout-client {:op :close :session session-id})
                                          combine-responses
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
  (let [sid (-> session meta ::nrepl/taking-until :session)
        transport2 (nrepl.core/connect :port (:port *server*))]
    (transport/send transport2 {"op" "eval" "code" "(println :foo)"
                                "session" sid})
    (is (->> (repeatedly #(transport/recv transport2 1000))
             (take-while identity)
             (some #(= ":foo\n" (:out %)))))))

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
         (->> (message client {:op :eval :out-limit 5 :code "(print (range 11))"})
              (map :out)
              (remove nil?))
         ;; existing session
         (->> (message session {:op :eval :out-limit 5 :code "(print (range 11))"})
              (map :out)
              (remove nil?)))))

(def-repl-test ensure-whitespace-prints
  (is (= " \t \n \f \n" (->> (repl-eval client "(println \" \t \n \f \")")
                             combine-responses
                             :out))))

(defn custom-printer
  [value opts]
  (format "<foo %s %s>" value (or (:sub opts) "...")))

(def-repl-test value-printing
  (testing "bad symbol should fall back to default printer"
    (is (= ["42"]
           (-> (message client {:op :eval
                                :code "(+ 34 8)"
                                :printer 'my.missing.ns/printer})
               (combine-responses)
               (:value)))))
  (testing "custom printing function symbol should be used"
    (is (= ["<foo true ...>"]
           (-> (message client {:op :eval
                                :code "true"
                                :printer `custom-printer})
               (combine-responses)
               (:value)))))
  (testing "options should be passed to printer"
    (is (= ["<foo 3 bar>"]
           (-> (message client {:op :eval
                                :code "3"
                                :printer `custom-printer
                                :print-options {:sub "bar"}})
               (combine-responses)
               (:value))))))

(def-repl-test session-return-recall
  (testing "sessions persist across connections"
    (repl-values session (code
                          (apply + (range 6))
                          (str 12 \c)
                          (keyword "hello")))
    (with-open [separate-connection (connect :port (:port *server*))]
      (let [history [[15 "12c" :hello]]
            sid (-> session meta :nrepl.core/taking-until :session)
            sc-session (-> separate-connection
                           (nrepl/client 1000)
                           (nrepl/client-session :session sid))]
        (is (= history (repl-values sc-session "[*3 *2 *1]")))
        (is (= history (repl-values sc-session "*1")))))) (testing "without a session id, REPL-bound vars like *1 have default values"
                                                            (is (= [nil] (repl-values client "*1")))))

(def-repl-test session-set!
  (repl-eval session (code
                      (set! *compile-path* "badpath")
                      (set! *warn-on-reflection* true)))
  (is (= [["badpath" true]] (repl-values session (code [*compile-path* *warn-on-reflection*])))))

(def-repl-test exceptions
  (let [{:keys [status err value]} (combine-responses (repl-eval session "(throw (Exception. \"bad, bad code\"))"))]
    (is (= #{"eval-error" "done"} status))
    (is (nil? value))
    (is (.contains err "bad, bad code"))
    (is (= [true] (repl-values session "(.contains (str *e) \"bad, bad code\")")))))

(def-repl-test multiple-expressions-return
  (is (= [5 18] (repl-values session "5 (/ 5 0) (+ 5 6 7)"))))

(def-repl-test return-on-incomplete-expr
  (let [{:keys [out status value]} (combine-responses (repl-eval session "(missing paren"))]
    (is (nil? value))
    (is (= #{"done" "eval-error"} status))
    (is (re-seq #"EOF while reading" (first (repl-values session "(.getMessage *e)"))))))

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
  (is (= [5] (response-values (message session {:op :eval :code "bar" :ns "user"}))))
  ;; NREPL-72: :ns argument to eval shouldn't affect *ns* outside of the scope of that evaluation
  (is (= "baz" (-> (repl-eval session "5") combine-responses :ns))))

(def-repl-test error-on-nonexistent-ns
  (is (= #{"error" "namespace-not-found" "done"}
         (-> (message timeout-client {:op :eval :code "(+ 1 1)" :ns (name (gensym))})
             combine-responses
             :status))))

(def-repl-test proper-response-ordering
  (is (= [[nil "100\n"] ; printed number
          ["nil" nil] ; return val from println
          ["42" nil]  ; return val from `42`
          [nil nil]]  ; :done
         (map (juxt :value :out) (repl-eval client "(println 100) 42")))))

(def-repl-test interrupt
  (is (= #{"error" "interrupt-id-mismatch" "done"}
         (-> (message client {:op :interrupt :interrupt-id "foo"})
             first
             :status
             set)))

  (let [resp (message session {:op :eval :code (code (do
                                                       (def halted? true)
                                                       halted?
                                                       (Thread/sleep 30000)
                                                       (def halted? false)))})]
    (Thread/sleep 100)
    (is (= #{"done"} (-> session (message {:op :interrupt}) first :status set)))
    (is (= #{"done" "interrupted"} (-> resp combine-responses :status)))
    (is (= [true] (repl-values session "halted?")))))

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
                                     responses (message session {:op :eval :code expr})]
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
#_(def-repl-test ensure-server-closeable
    (.close *server*)
    (is (thrown? java.net.ConnectException (connect :port (:port *server*)))))

;; wasn't added until Clojure 1.3.0
(defn- root-cause
  "Returns the initial cause of an exception or error by peeling off all of
  its wrappers"
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
    (let [server (server/start-server)
          transport (connect :port (:port server))]
      (transport/send transport {"op" "eval" "code" "(+ 1 1)"})

      (let [reader (future (while true (transport/recv transport)))]
        (Thread/sleep 1000)
        (.close server)
        (Thread/sleep 1000)
        (try
          (deref reader 10000 :timeout)
          (assert false "A reader started prior to the server closing should throw an error...")
          (catch Throwable e
            (is (disconnection-exception? e)))))

      (is (thrown? SocketException (transport/recv transport)))
      ;; TODO: no idea yet why two sends are *sometimes* required to get a failure
      (try
        (transport/send transport {"op" "eval" "code" "(+ 5 1)"})
        (catch Throwable t))
      (is (thrown? SocketException (transport/send transport {"op" "eval" "code" "(+ 5 1)"}))))))

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
    (try (repl-eval session "(+ 1 1)") (catch Throwable t))
    (is (thrown? SocketException (repl-eval session "(+ 1 1)")))))

(def-repl-test request-*in*
  (is (= '((1 2 3)) (response-values (for [resp (repl-eval session "(read)")]
                                       (do
                                         (when (-> resp :status set (contains? "need-input"))
                                           (session {:op :stdin :stdin "(1 2 3)"}))
                                         resp)))))

  (session {:op :stdin :stdin "a\nb\nc\n"})
  (doseq [x "abc"]
    (is (= [(str x)] (repl-values session "(read-line)")))))

(def-repl-test request-*in*-eof
  (is (= nil (response-values (for [resp (repl-eval session "(read)")]
                                (do
                                  (when (-> resp :status set (contains? "need-input"))
                                    (session {:op :stdin :stdin []}))
                                  resp))))))

(def-repl-test request-multiple-read-newline-*in*
  (is (= '(:ohai) (response-values (for [resp (repl-eval session "(read)")]
                                     (do
                                       (when (-> resp :status set (contains? "need-input"))
                                         (session {:op :stdin :stdin ":ohai\n"}))
                                       resp)))))

  (session {:op :stdin :stdin "a\n"})
  (is (= ["a"] (repl-values session "(read-line)"))))

(def-repl-test request-multiple-read-with-buffered-newline-*in*
  (is (= '(:ohai) (response-values (for [resp (repl-eval session "(read)")]
                                     (do
                                       (when (-> resp :status set (contains? "need-input"))
                                         (session {:op :stdin :stdin ":ohai\na\n"}))
                                       resp)))))

  (is (= ["a"] (repl-values session "(read-line)"))))

(def-repl-test request-multiple-read-objects-*in*
  (is (= '(:ohai) (response-values (for [resp (repl-eval session "(read)")]
                                     (do
                                       (when (-> resp :status set (contains? "need-input"))
                                         (session {:op :stdin :stdin ":ohai :kthxbai\n"}))
                                       resp)))))

  (is (= [" :kthxbai"] (repl-values session "(read-line)"))))

(def-repl-test test-url-connect
  (with-open [conn (url-connect (str "nrepl://127.0.0.1:" (:port *server*)))]
    (transport/send conn {:op :eval :code "(+ 1 1)"})
    (is (= [2] (response-values (response-seq conn 100))))))

(deftest test-ack
  (with-open [s (server/start-server :handler (ack/handle-ack (server/default-handler)))]
    (ack/reset-ack-port!)
    (with-open [s2 (server/start-server :ack-port (:port s))]
      (is (= (:port s2) (ack/wait-for-ack 10000))))))

(def-repl-test agent-await
  (is (= [42] (repl-values session (code (let [a (agent nil)]
                                           (send a (fn [_] (Thread/sleep 1000) 42))
                                           (await a)
                                           @a))))))

(deftest cloned-session-*1-binding
  (let [port (:port *server*)
        conn (nrepl/connect :port port)
        client (nrepl/client conn 1000)
        sess (nrepl/client-session client)
        eval (sess {:op :eval
                    :code "(+ 1 4)"})
        new-sess-id (->> (sess {:id 1
                                :op :clone})
                         (filter (fn [x] (= 1 (:id x))))
                         first
                         :new-session)
        cloned-sess (nrepl.core/client-session client :session new-sess-id)
        cloned-sess-*1 (->> (cloned-sess {:id 2
                                          :op :eval
                                          :code "*1"})
                            (filter (fn [x] (and (= 2 (:id x))
                                                 (contains? x :value))))
                            first
                            :value)]
    (is (= cloned-sess-*1 "5"))))

(def-repl-test print-namespace-maps-binding
  (when (resolve '*print-namespace-maps*)
    (let [set-true (repl-eval session "(set! *print-namespace-maps* true)")
          true-val (first (repl-values session "*print-namespace-maps*"))
          set-false (repl-eval session "(set! *print-namespace-maps* false)")
          false-val (first (repl-values session "*print-namespace-maps*"))]
      (is (= true true-val))
      (is (= false false-val)))))

(def-repl-test interrupt-load-file
  (let [resp (message session {:op "load-file"
                               :file (slurp (File. project-base-dir "load-file-test/nrepl/load_file_sample2.clj"))
                               :file-path "nrepl/load_file_sample2.clj"
                               :file-name "load_file_sample2.clj"})]
    (Thread/sleep 100)
    (is (= #{"done"} (-> session (message {:op :interrupt}) first :status set)))
    (is (= #{"done" "interrupted"} (-> resp combine-responses :status)))))
