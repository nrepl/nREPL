(ns nrepl.cmdline
  "A proof-of-concept command-line client for nREPL.  Please see
  e.g. REPL-y for a proper command-line nREPL client @
  https://github.com/trptcolin/reply/"
  {:author "Chas Emerick"}
  (:require
   [clojure.java.io :as io]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [nrepl.config :as config]
   [nrepl.core :as nrepl]
   [nrepl.ack :refer [send-ack]]
   [nrepl.misc :refer [noisy-future]]
   [nrepl.server :as nrepl-server]
   [nrepl.socket :as socket]
   [nrepl.transport :as transport]
   [nrepl.version :as version])
  (:import
   [java.net URI]))

(defn- clean-up-and-exit
  "Performs any necessary clean up and calls `(System/exit status)`."
  [status]
  (shutdown-agents)
  (flush)
  (binding [*out* *err*] (flush))
  (System/exit status))

(defn exit
  "Requests that the process exit with the given `status`.  Does not
  return."
  [status]
  ;; :nrepl/kind is our shared (ns independent) ExceptionInfo discriminator
  (throw (ex-info nil {:nrepl/kind ::exit ::status status})))

(defn die
  "`Print`s items in `msg` to *err* and then exits with a status of 2."
  [& msg]
  (binding [*out* *err*]
    (doseq [m msg] (print m)))
  (exit 2))

(defmacro ^{:author "Colin Jones"} set-signal-handler!
  [signal f]
  (if (try (Class/forName "sun.misc.Signal")
           (catch Throwable _e))
    `(try
       (sun.misc.Signal/handle
        (sun.misc.Signal. ~signal)
        (proxy [sun.misc.SignalHandler] []
          (handle [signal#] (~f signal#))))
       (catch Throwable e#))
    `(println "Unable to set signal handlers.")))

(def colored-output
  {:err #(binding [*out* *err*]
           (print "\033[31m")
           (print %)
           (print "\033[m")
           (flush))
   :out print
   :value (fn [x]
            (print "\033[34m")
            (print x)
            (println "\033[m")
            (flush))})

(def running-repl (atom {:transport nil
                         :client nil}))

(defn- done?
  [input]
  (some (partial = input)
        ['exit 'quit '(exit) '(quit)]))

(defn repl-intro
  "Returns nREPL interactive repl intro copy and version info as a new-line
  separated string."
  []
  (format "nREPL %s
Clojure %s
%s %s
Interrupt: Control+C
Exit:      Control+D or (exit) or (quit)"
          (:version-string version/version)
          (clojure-version)
          (System/getProperty "java.vm.name")
          (System/getProperty "java.runtime.version")))

(defn- run-repl
  ([host port]
   (run-repl host port nil))
  ([host port {:keys [prompt err out value transport]
               :or {prompt #(print (str % "=> "))
                    err print
                    out print
                    value println
                    transport #'transport/bencode}}]
   (let [transport (nrepl/connect :host host :port port :transport-fn transport)
         client (nrepl/client transport Long/MAX_VALUE)]
     (println (repl-intro))
     ;; We take 50ms to listen to any greeting messages, and display the value
     ;; in the `:out` slot.
     (noisy-future (->> (client)
                        (take-while #(nil? (:id %)))
                        (run! #(when-let [msg (:out %)] (print msg)))))
     (Thread/sleep 50)
     (let [session (nrepl/client-session client)
           ns (atom "user")]
       (swap! running-repl assoc :transport transport)
       (swap! running-repl assoc :client session)
       (loop []
         (prompt @ns)
         (flush)
         (let [input (read *in* false 'exit)]
           (if (done? input)
             (System/exit 0)
             (do (doseq [res (nrepl/message session {:op "eval" :code (pr-str input)})]
                   (when (:value res) (value (:value res)))
                   (when (:out res) (out (:out res)))
                   (when (:err res) (err (:err res)))
                   (when (:ns res) (reset! ns (:ns res))))
                 (recur)))))))))

(def #^{:private true} option-shorthands
  {"-i" "--interactive"
   "-r" "--repl"
   "-f" "--repl-fn"
   "-c" "--connect"
   "-b" "--bind"
   "-h" "--host"
   "-p" "--port"
   "-s" "--socket"
   "-m" "--middleware"
   "-t" "--transport"
   "-n" "--handler"
   "-v" "--version"})

(def #^{:private true} unary-options
  #{"--interactive"
    "--connect"
    "--color"
    "--help"
    "--version"
    "--verbose"})

(defn- expand-shorthands
  "Expand shorthand options into their full forms."
  [args]
  (map (fn [arg] (or (option-shorthands arg) arg)) args))

(defn- keywordize-options [options]
  (reduce-kv
   #(assoc %1 (keyword (str/replace-first %2 "--" "")) %3)
   {}
   options))

(defn- split-args
  "Convert `args` into a map of options + a list of args.
  Unary options are set to true during this transformation.
  Returns a vector combining the map and the list."
  [args]
  (loop [[arg & rem-args :as args] args
         options {}]
    (if-not (and arg (re-matches #"-.*" arg))
      [options args]
      (if (unary-options arg)
        (recur rem-args
               (assoc options arg true))
        (recur (rest rem-args)
               (assoc options arg (first rem-args)))))))

(defn help
  []
  (str "Usage:

  -i/--interactive            Start nREPL and connect to it with the built-in client.
  -c/--connect                Connect to a running nREPL with the built-in client.
  -C/--color                  Use colors to differentiate values from output in the REPL. Must be combined with --interactive.
  -b/--bind ADDR              Bind address, by default \"127.0.0.1\".
  -h/--host ADDR              Host address to connect to when using --connect. Defaults to \"127.0.0.1\".
  -p/--port PORT              Start nREPL on PORT. Defaults to 0 (random port) if not specified.
  -s/--socket PATH            Start nREPL on filesystem socket at PATH.
  --ack ACK-PORT              Acknowledge the port of this server to another nREPL server running on ACK-PORT.
  -n/--handler HANDLER        The nREPL message handler to use for each incoming connection; defaults to the result of `(nrepl.server/default-handler)`.
  -m/--middleware MIDDLEWARE  A sequence of vars, representing middleware you wish to mix in to the nREPL handler.
  -t/--transport TRANSPORT    The transport to use. By default that's nrepl.transport/bencode.
  --help                      Show this help message.
  -v/--version                Display the nREPL version.
  --verbose                   Show verbose output."))

(defn- require-and-resolve
  "Attempts to resolve the config `key`'s `value` as a namespaced symbol
  and returns the related var if successful.  Otherwise calls `die`."
  [key sym]
  (when-not (symbol? sym)
    (die (format "nREPL %s: %s is not a symbol\n" (name key) (pr-str sym))))
  (let [space (some-> (namespace sym) symbol)]
    (when-not space
      (die (format "nREPL %s: %s has no namespace\n" (name key) sym)))
    (require space)
    (or (ns-resolve space (-> sym name symbol))
        (die (format "nREPL %s: unable to resolve %s\n" (name key) sym)))))

(def ^:private resolve-mw-xf
  (comp (map #(require-and-resolve :middleware %))
        (keep identity)))

(defn- handle-seq-var
  [var]
  (let [x @var]
    (if (sequential? x)
      (into [] resolve-mw-xf x)
      [var])))

(defn- handle-interrupt
  [_signal]
  (let [transport (:transport @running-repl)
        client (:client @running-repl)]
    (if (and transport client)
      (doseq [res (nrepl/message client {:op "interrupt"})]
        (when (= ["done" "session-idle"] (:status res))
          (System/exit 0)))
      (System/exit 0))))

(def ^:private mw-xf
  (comp (map symbol)
        resolve-mw-xf
        (mapcat handle-seq-var)))

(defn- ->mw-list
  [middleware-var-strs]
  (into [] mw-xf middleware-var-strs))

(defn- build-handler
  "Build an nREPL handler from `middleware`.
  `middleware` is a sequence of vars or string which can be resolved
  to vars, representing middleware you wish to mix in to the nREPL
  handler. Vars can resolve to a sequence of vars, in which case
  they'll be flattened into the list of middleware."
  [middleware]
  (apply nrepl.server/default-handler (->mw-list middleware)))

(defn- ->int [x]
  (cond
    (nil? x) x
    (number? x) x
    :else (Integer/parseInt x)))

(defn- sanitize-middleware-option
  "Sanitize the middleware option.  In the config it can be either a
  symbol or a vector of symbols."
  [mw-opt]
  (if (symbol? mw-opt)
    [mw-opt]
    mw-opt))

(defn parse-cli-values
  "Converts relevant command line argument values to their config
  representation."
  [options]
  (reduce-kv (fn [result k v]
               (case k
                 (:handler :transport :middleware) (assoc result k (edn/read-string v))
                 result))
             options
             options))

(defn args->cli-options
  "Takes CLI args list and returns vector of parsed options map and
  remaining args."
  [args]
  (let [[options _args] (split-args (expand-shorthands args))
        merge-config (partial merge config/config)
        options (-> options
                    (keywordize-options)
                    (parse-cli-values)
                    (merge-config))]
    [options _args]))

(defn display-help
  "Prints the help copy to the screen and exits the program with exit code 0."
  []
  (println (help))
  (exit 0))

(defn display-version
  "Prints nREPL version to the screen and exits the program with exit code 0."
  []
  (println (:version-string version/version))
  (exit 0))

(defn- options->transport
  "Takes a map of nREPL CLI options.
  Returns either a default transport or the value of :transport."
  [options]
  (or (some->> options
               (:transport)
               (require-and-resolve :transport))
      #'transport/bencode))

(defn- options->handler
  "Takes a map of nREPL CLI options and list of middleware.
  Returns a request handler function.
  If some handler was explicitly passed we'll use it, otherwise we'll build
  one from whatever was passed via --middleware"
  [options middleware]
  (or (some->> options
               (:handler)
               (require-and-resolve :handler))
      (build-handler middleware)))

(defn- options->ack-port
  "Takes a map of nREPL CLI options.
  Returns integer ack port or nil."
  [options]
  (some-> options
          (:ack)
          (->int)))

(defn- options->repl-fn
  "Takes a map of nREPL CLI options.
  Returns either the :repl-fn config option or uses run-repl."
  [options]
  (or (some->> options
               (:repl-fn)
               (symbol)
               (require-and-resolve :repl-fn))
      #'run-repl))

(defn- options->greeting
  "Takes a map of nREPL CLI options and the selected transport for the server.
  Returns a greeting function or nil."
  [_options transport]
  (when (= transport #'transport/tty)
    #'transport/tty-greeting))

(defn connection-opts
  "Takes map of nREPL CLI options
  Returns map of processed options used to connect or start a nREPL server."
  [options]
  {:port (->int (:port options))
   :host (:host options)
   :socket (:socket options)
   :transport (options->transport options)
   :repl-fn (options->repl-fn options)})

(defn server-opts
  "Takes a map of nREPL CLI options
  Returns map of processed options to start an nREPL server."
  [options]
  (let [middleware (sanitize-middleware-option (:middleware options))
        {:keys [host port socket transport]} (connection-opts options)]
    (merge options
           {:host host
            :port port
            :socket socket
            :transport transport
            :bind (:bind options)
            :middleware middleware
            :handler (options->handler options middleware)
            :greeting (options->greeting options transport)
            :ack-port (options->ack-port options)
            :repl-fn (options->repl-fn options)})))

(defn interactive-repl
  "Runs an interactive repl if :interactive CLI option is true otherwise
  puts the current thread to sleep
  Takes nREPL server map and processed CLI options map.
  Returns nil."
  [server options]
  (let [transport (:transport options)
        repl-fn (:repl-fn options)
        host (:host server)
        port (:port server)]
    (when (= transport #'transport/tty)
      (die "The built-in client does not support the tty transport. Consider using `nc` or `telnet`.\n"))
    (repl-fn host port (merge (when (:color options) colored-output)
                              {:transport transport}))))

(defn connect-to-server
  "Connects to a running nREPL server and runs a REPL. Exits program when REPL
  is closed.
  Takes a map of nREPL CLI options."
  [{:keys [host port _transport] :as options}]
  (interactive-repl {:host host
                     :port port}
                    options)
  (exit 0))

(defn ack-server
  "Acknowledge the port of this server to another nREPL server running on
  :ack port.
  Takes nREPL server map and processed CLI options map.
  Prints a message describing the acknowledgement between servers.
  Returns nil."
  [server options]
  (when-let [ack-port (:ack-port options)]
    (let [port (:port server)
          transport (:transport options)]
      (when (:verbose options)
        (println (format "ack'ing my port %d to other server running on port %d"
                         port ack-port)))
      (send-ack port ack-port transport))))

(defn server-started-message
  "Returns nREPL server started message that some tools rely on to parse the
  connection details from.
  Takes nREPL server map and processed CLI options map.
  Returns connection header string."
  [server options]
  (let [transport (:transport options)
        ^java.net.ServerSocket ssocket (:server-socket server)]
    ;; The format here is important, as some tools (e.g. CIDER) parse the string
    ;; to extract from it the host and the port to connect to
    (let [^URI uri (socket/as-nrepl-uri ssocket (transport/uri-scheme transport))]
      (if-let [host (.getHost uri)]
        (format "nREPL server started on port %d on host %s - %s"
                (.getPort uri) host uri)
        (str "nREPL server started on socket " (.toASCIIString uri))))))

(defn save-port-file
  "Writes a file relative to project classpath with port number so other tools
  can infer the nREPL server port.
  Takes nREPL server map and processed CLI options map.
  Returns nil."
  [server _options]
  ;; Many clients look for this file to infer the port to connect to
  (let [port (:port server)
        port-file (io/file ".nrepl-port")]
    (.deleteOnExit port-file)
    (spit port-file port)))

(defn start-server
  "Creates an nREPL server instance.
  Takes map of CLI options.
  Returns nREPL server map."
  [{:keys [port bind socket handler transport greeting]}]
  (nrepl-server/start-server
   :port port
   :bind bind
   :socket socket
   :handler handler
   :transport-fn transport
   :greeting-fn greeting))

(defn dispatch-commands
  "Look at options to dispatch a specified command.
  Takes CLI options map. May return a server map, nil, or exit."
  [options]
  (cond (:help options)    (display-help)
        (:version options) (display-version)
        (:connect options) (connect-to-server (connection-opts options))
        :else (let [options (server-opts options)
                    server (start-server options)]
                (ack-server server options)
                (println (server-started-message server options))
                (save-port-file server options)
                (if (:interactive options)
                  (interactive-repl server options)
                  ;; need to hold process open with a non-daemon thread
                  ;;   -- this should end up being super-temporary
                  (Thread/sleep Long/MAX_VALUE)))))

(defn -main
  [& args]
  (try
    (set-signal-handler! "INT" handle-interrupt)
    (let [[options _args] (args->cli-options args)]
      (dispatch-commands options))
    (catch clojure.lang.ExceptionInfo ex
      (let [{:keys [:nrepl/kind ::status]} (ex-data ex)]
        (case kind
          ::exit (clean-up-and-exit status)
          (:nrepl.server/no-filesystem-sockets
           :nrepl.server/invalid-start-request)
          (do
            (binding [*out* *err*]
              (println (.getMessage ex)))
            (clean-up-and-exit 2))
          (throw ex))))))
