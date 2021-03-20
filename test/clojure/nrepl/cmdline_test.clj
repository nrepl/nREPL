(ns nrepl.cmdline-test
  {:author "Chas Emerick"}
  (:require
   [clojure.test :refer :all]
   [nrepl.ack :as ack]
   [nrepl.cmdline :as cmd]
   [nrepl.core :as nrepl]
   [nrepl.core-test :refer [*server* *transport-fn* transport-fns]]
   [nrepl.server :as server]
   [nrepl.transport :as transport])
  (:import
   (com.hypirion.io Pipe ClosingPipe)
   (nrepl.server Server)))

(defn- start-server-for-transport-fn
  [transport-fn f]
  (with-open [^Server server (server/start-server
                              :transport-fn transport-fn
                              :handler (ack/handle-ack (server/default-handler)))]
    (binding [*server* server
              *transport-fn* transport-fn]
      (testing (str (-> transport-fn meta :name) " transport")
        (ack/reset-ack-port!)
        (f))
      (set! *print-length* nil)
      (set! *print-level* nil))))

(defn- server-cmdline-fixture
  [f]
  (doseq [transport-fn transport-fns]
    (start-server-for-transport-fn transport-fn f)))

(use-fixtures :each server-cmdline-fixture)

(defn- var->str
  [sym]
  (subs (str sym) 2))

(defn- sh
  "A version of clojure.java.shell/sh that streams in/out/err.
  Taken and edited from https://github.com/technomancy/leiningen/blob/f7e1adad6ff5137d6ea56bc429c3b620c6f84128/leiningen-core/src/leiningen/core/eval.clj"
  ^Process
  [& cmd]
  (let [proc (.exec (Runtime/getRuntime) ^"[Ljava.lang.String;" (into-array String cmd))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn [] (.destroy proc))))
    (with-open [out (.getInputStream proc)
                err (.getErrorStream proc)
                in (.getOutputStream proc)]
      (let [pump-out (doto (Pipe. out System/out) .start)
            pump-err (doto (Pipe. err System/err) .start)
            pump-in (ClosingPipe. System/in in)]
        proc))))

(deftest repl-intro
  (is (re-find #"nREPL" (cmd/repl-intro))))

(deftest help
  (is (re-find #"Usage:" (cmd/help))))

(deftest parse-cli-values
  (is (= {:other "string"
          :middleware :middleware
          :handler :handler
          :transport :transport}
         (cmd/parse-cli-values {:other "string"
                                :middleware ":middleware"
                                :handler ":handler"
                                :transport ":transport"}))))

(deftest args->cli-options
  (is (= [{:middleware :middleware :repl "true"} ["extra" "args"]]
         (cmd/args->cli-options ["-m" ":middleware" "-r" "true" "extra" "args"]))))

(deftest connection-opts
  (is (= {:port 5000
          :host "0.0.0.0"
          :socket nil
          :transport #'transport/bencode
          :repl-fn #'nrepl.cmdline/run-repl}
         (cmd/connection-opts {:port "5000"
                               :host "0.0.0.0"
                               :transport nil}))))

(deftest server-opts
  (is (= {:bind "0.0.0.0"
          :port 5000
          :transport #'transport/bencode
          :handler #'clojure.core/identity
          :repl-fn #'clojure.core/identity
          :greeting nil
          :ack-port 2000}
         (select-keys
          (cmd/server-opts {:bind "0.0.0.0"
                            :port 5000
                            :ack 2000
                            :handler 'clojure.core/identity
                            :repl-fn 'clojure.core/identity})
          [:bind :port :transport :greeting :handler :ack-port :repl-fn]))))

(deftest ack-server
  (with-redefs [ack/send-ack (fn [_ _ _] true)]
    (let [output (with-out-str
                   (cmd/ack-server {:port 6000}
                                   {:ack-port 8000
                                    :transport #'transport/bencode
                                    :verbose true}))]
      (is (= "ack'ing my port 6000 to other server running on port 8000\n"
             output)))
    (let [output (with-out-str
                   (cmd/ack-server {:port 6000}
                                   {:ack-port 8000
                                    :transport #'transport/bencode}))]
      (is (= "" output)))))

(deftest server-started-message
  (with-open [^Server server (server/start-server
                              :transport-fn #'transport/bencode
                              :handler server/default-handler)]
    (is (re-find #"nREPL server started on port \d+ on host .* - .*//.*:\d+"
                 (cmd/server-started-message
                  server
                  {:transport #'transport/bencode})))))

(deftest ^:slow ack
  (let [ack-port (:port *server*)
        ^Process
        server-process (apply sh ["java" "-Dnreplacktest=y"
                                  "-cp" (System/getProperty "java.class.path")
                                  "nrepl.main"
                                  "--ack" (str ack-port)
                                  "--transport" (var->str *transport-fn*)])
        acked-port (ack/wait-for-ack 10000)]
    (try
      (is acked-port "Timed out waiting for ack")
      (when acked-port
        (with-open [^nrepl.transport.FnTransport
                    transport-2 (nrepl/connect :port acked-port
                                               :transport-fn *transport-fn*)]
          (let [client (nrepl/client transport-2 1000)]
            ;; just a sanity check
            (is (= "y"
                   (-> (nrepl/message client {:op "eval"
                                              :code "(System/getProperty \"nreplacktest\")"})
                       first
                       nrepl/read-response-value
                       :value))))))
      (finally
        (.destroy server-process)))))

(deftest ^:slow explicit-port-argument
  (let [ack-port (:port *server*)
        free-port (with-open [ss (java.net.ServerSocket.)]
                    (.bind ss nil)
                    (.getLocalPort ss))
        ^Process
        server-process (apply sh ["java" "-Dnreplacktest=y"
                                  "-cp" (System/getProperty "java.class.path")
                                  "nrepl.main"
                                  "--port" (str free-port)
                                  "--ack" (str ack-port)
                                  "--transport" (var->str *transport-fn*)])
        acked-port (ack/wait-for-ack 10000)]
    (try
      (is (= acked-port free-port))
      (finally
        (Thread/sleep 2000)
        (.destroy server-process)))))

;; The following tests ignore the server started in the fixture, as they only test
;; the TTY transport.

(deftest ^:slow tty-server
  (let [^int free-port (with-open [ss (java.net.ServerSocket.)]
                         (.bind ss nil)
                         (.getLocalPort ss))
        ack-port       (:port *server*)
        ^Process
        server-process (apply sh ["java" "-Dnreplacktest=y"
                                  "-cp" (System/getProperty "java.class.path")
                                  "nrepl.main"
                                  "--port" (str free-port)
                                  "--ack" (str ack-port)
                                  "--transport" "nrepl.transport/tty"])
        acked-port     (ack/wait-for-ack 10000)]
    (try
      (let [c    (org.apache.commons.net.telnet.TelnetClient.)
            _    (.connect c "localhost" free-port)
            out  (java.io.PrintStream. (.getOutputStream c))
            br   (java.io.BufferedReader. (java.io.InputStreamReader. (.getInputStream c)))
            _    (doto out
                   (.println "(System/getProperty \"nreplacktest\")")
                   (.flush))
            resp (doall (repeatedly 3 #(.readLine br)))
            _    (.disconnect c)]
        (is (= "user=> \"y\""
               (last resp))))
      (finally
        (.destroy server-process)))))

(deftest no-tty-client
  (testing "Trying to connect with the tty transport should fail."
    (with-open [^Server server (server/start-server :transport-fn #'transport/tty)]
      (let [options (cmd/connection-opts {:port      (:port server)
                                          :host      "localhost"
                                          :transport 'nrepl.transport/tty})]
        (is (thrown? clojure.lang.ExceptionInfo
                     (cmd/interactive-repl server options)))))))
