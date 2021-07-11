(ns nrepl.cmdline-test
  {:author "Chas Emerick"}
  (:require
   [clojure.java.io :refer [as-file]]
   [clojure.test :refer :all]
   [nrepl.ack :as ack]
   [nrepl.bencode :refer [write-bencode]]
   [nrepl.cmdline :as cmd]
   [nrepl.core :as nrepl]
   [nrepl.core-test :refer [*server* *transport-fn* transport-fns]]
   [nrepl.server :as server]
   [nrepl.socket :refer [find-class unix-domain-flavor unix-socket-address]]
   [nrepl.transport :as transport])
  (:import
   (com.hypirion.io Pipe ClosingPipe)
   (java.lang ProcessBuilder$Redirect)
   (java.net Socket SocketAddress)
   (java.nio.channels Channels SocketChannel)
   (java.nio.file Files)
   (java.nio.file.attribute PosixFilePermissions)
   (nrepl.server Server)))

(defn create-tmpdir
  "Creates a temporary directory in parent (something clojure.java.io/as-path
  can handle) with the specified permissions string (something
  PosixFilePermissions/asFileAttribute can handle i.e. \"rw-------\") and
  returns its Path."
  [parent prefix permissions]
  (let [nio-path (.toPath (as-file parent))
        perms (PosixFilePermissions/fromString permissions)
        attr (PosixFilePermissions/asFileAttribute perms)]
    (Files/createTempDirectory nio-path prefix (into-array [attr]))))

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

;;; Unix domain socket tests

(defn send-jdk-socket-message [message path]
  (let [^SocketAddress addr (unix-socket-address path)
        sock (SocketChannel/open addr)]
    ;; Assume it's safe to use Channels input/output streams here since
    ;; we're never reading and writing at the same time.
    ;; (cf. https://bugs.openjdk.java.net/browse/JDK-4509080 - not fixed
    ;; as of at least JDK 16).
    (with-open [out (Channels/newOutputStream sock)]
      (write-bencode out message))))

(defn send-junixsocket-message [message path]
  (let [^Class sock-class (find-class 'org.newsclub.net.unix.AFUNIXSocket)
        new-instance (.getDeclaredMethod sock-class "newInstance" nil)
        addr (unix-socket-address path)]
    (with-open [^Socket sock (.invoke new-instance nil nil)]
      (.connect sock addr)
      (write-bencode (.getOutputStream sock) message))))

(deftest ^:slow basic-fs-socket-behavior

  (if-not unix-domain-flavor
    (binding [*out* *err*]
      (println "Skipping UNIX domain socket tests for JDK < 16 without junixsocket dependency"))
    (let [tmpdir (create-tmpdir "target" "socket-test-" "rwx------")
          sock-path (str tmpdir "/socket")
          sock-file (as-file sock-path)]
      (try
        ;; Use a Process rather than sh so we can see server errors
        (let [cmd (into-array ["java"
                               "-cp" (System/getProperty "java.class.path")
                               "nrepl.main" "-s" sock-path])
              server (.start (doto (ProcessBuilder. ^"[Ljava.lang.String;" cmd)
                               (.redirectOutput ProcessBuilder$Redirect/INHERIT)
                               (.redirectError ProcessBuilder$Redirect/INHERIT)))]
          (try
            (while (not (.exists sock-file))
              (Thread/sleep 100))
            (case unix-domain-flavor
              :jdk
              (send-jdk-socket-message {:code "(System/exit 42)" :op :eval}
                                       sock-path)
              :junixsocket
              (send-junixsocket-message {:code "(System/exit 42)" :op :eval}
                                        sock-path))
            (is (= 42 (.waitFor server)))
            (finally
              (.destroy server))))
        (finally
          (.delete sock-file)
          (Files/delete tmpdir))))))
