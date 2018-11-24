(ns nrepl.cmdline
  "A proof-of-concept command-line client for nREPL.  Please see
  e.g. REPL-y for a proper command-line nREPL client @
  https://github.com/trptcolin/reply/"
  {:author "Chas Emerick"}
  (:require
   [clojure.java.io :as io]
   [clojure.edn :as edn]
   [nrepl.config :as config]
   [nrepl.core :as nrepl]
   [nrepl.ack :refer [send-ack]]
   [nrepl.server :refer [start-server]]
   [nrepl.transport :as transport]
   [nrepl.version :as version]))

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
  (throw (ex-info nil {::kind ::exit ::status status})))

(defn die
  "`Print`s items in `msg` to *err* and then exits with a status of 2."
  [& msg]
  (binding [*out* *err*]
    (doseq [m msg] (print m)))
  (exit 2))

(defmacro ^{:author "Colin Jones"} set-signal-handler!
  [signal f]
  (if (try (Class/forName "sun.misc.Signal")
           (catch Throwable e))
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

(defn- run-repl
  ([host port]
   (run-repl host port nil))
  ([host port {:keys [prompt err out value]
               :or {prompt #(print (str % "=> "))
                    err print
                    out print
                    value println}}]
   (let [transport (nrepl/connect :host host :port port)
         client (nrepl/client-session (nrepl/client transport Long/MAX_VALUE))
         ns (atom "user")]
     (swap! running-repl assoc :transport transport)
     (swap! running-repl assoc :client client)
     (println (format "nREPL %s" (:version-string version/version)))
     (println (str "Clojure " (clojure-version)))
     (println (System/getProperty "java.vm.name") (System/getProperty "java.runtime.version"))
     (println (str "Interrupt: Control+C"))
     (println (str "Exit:      Control+D or (exit) or (quit)"))
     (loop []
       (prompt @ns)
       (flush)
       (let [input (read *in* false 'exit)]
         (if (done? input)
           (System/exit 0)
           (do (doseq [res (nrepl/message client {:op "eval" :code (pr-str input)})]
                 (when (:value res) (value (:value res)))
                 (when (:out res) (out (:out res)))
                 (when (:err res) (err (:err res)))
                 (when (:ns res) (reset! ns (:ns res))))
               (recur))))))))

(def #^{:private true} option-shorthands
  {"-i" "--interactive"
   "-r" "--repl"
   "-c" "--connect"
   "-b" "--bind"
   "-h" "--host"
   "-p" "--port"
   "-m" "--middleware"
   "-t" "--transport"
   "-n" "--handler"
   "-v" "--version"})

(def #^{:private true} unary-options
  #{"--interactive"
    "--connect"
    "--color"
    "--help"
    "--version"})

(defn- expand-shorthands
  "Expand shorthand options into their full forms."
  [args]
  (map (fn [arg] (or (option-shorthands arg) arg)) args))

(defn- keywordize-options [options]
  (reduce-kv
   #(assoc %1 (keyword (clojure.string/replace-first %2 "--" "")) %3)
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

(defn- display-help
  []
  (println "Usage:

  -i/--interactive            Start nREPL and connect to it with the built-in client.
  -c/--connect                Connect to a running nREPL with the built-in client.
  -C/--color                  Use colors to differentiate values from output in the REPL. Must be combined with --interactive.
  -b/--bind ADDR              Bind address, by default \"127.0.0.1\".
  -h/--host ADDR              Host address to connect to when using --connect. Defaults to \"127.0.0.1\".
  -p/--port PORT              Start nREPL on PORT. Defaults to 0 (random port) if not specified.
  --ack ACK-PORT              Acknowledge the port of this server to another nREPL server running on ACK-PORT.
  --auth-wrapper VAR          Wrapper whose returned handler will be called with
                              each incoming message, before doing anything else.
                              When no wrapper is provided, all messages are accepted.
  -n/--handler HANDLER        The nREPL message handler to use for each incoming connection; defaults to the result of `(nrepl.server/default-handler)`.
  -m/--middleware MIDDLEWARE  A sequence of vars, representing middleware you wish to mix in to the nREPL handler.
  -t/--transport TRANSPORT    The transport to use. By default that's nrepl.transport/bencode.
  --help                      Show this help message.
  -v/--version                Display the nREPL version."))

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
  [signal]
  (let [transport (:transport @running-repl)
        client (:client @running-repl)]
    (if (and transport client)
      (doseq [res (nrepl/message client {:op :interrupt})]
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

(defn- url-scheme [transport]
  (if (= transport #'transport/tty)
    "telnet"
    "nrepl"))

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

(defn- parse-cli-values
  "Converts relevant command line argument values to their config
  representation."
  [options]
  (reduce-kv (fn [result k v]
               (case k
                 (:handler :transport :middleware :auth-wrapper)
                 (assoc result k (edn/read-string v))

                 result))
             options
             options))

(defn- run
  [args]
  (set-signal-handler! "INT" handle-interrupt)
  (let [[options _args] (split-args (expand-shorthands args))
        options (keywordize-options options)
        options (parse-cli-values options)
        options (merge config/config options)]
    ;; we have to check for --help first, as it's special
    (when (:help options)
      (display-help)
      (exit 0))
    (when (:version options)
      (println (:version-string version/version))
      (exit 0))
    ;; then we check for --connect
    (let [port (->int (:port options))
          host (:host options)]
      (when (:connect options)
        (run-repl host port)
        (exit 0))
      ;; otherwise we assume we have to start an nREPL server
      (let [bind (:bind options)
            auth-wrapper (some->> (options :auth-wrapper)
                                  (require-and-resolve :auth-wrapper))
            ;; if some handler was explicitly passed we'll use it, otherwise we'll build one
            ;; from whatever was passed via --middleware
            middleware (sanitize-middleware-option (:middleware options))
            handler (some->> (:handler options) (require-and-resolve :handler))
            handler (or handler (build-handler middleware))
            transport (some->> (:transport options) (require-and-resolve :transport))
            greeting-fn (if (= transport #'transport/tty) #'transport/tty-greeting)
            server (start-server :port port :bind bind :handler handler
                                 :transport-fn transport :greeting-fn greeting-fn
                                 :auth-wrapper auth-wrapper)]
        (when-let [ack-port (:ack options)]
          (binding [*out* *err*]
            (println (format "ack'ing my port %d to other server running on port %d"
                             (:port server) ack-port)
                     (:status (send-ack (:port server) ack-port)))))
        (let [port (:port server)
              ^java.net.ServerSocket ssocket (:server-socket server)
              host (.getHostName (.getInetAddress ssocket))]
          ;; The format here is important, as some tools (e.g. CIDER) parse the string
          ;; to extract from it the host and the port to connect to
          (println (format "nREPL server started on port %d on host %s - %s://%s:%d"
                           port host (url-scheme transport) host port))
          ;; Many clients look for this file to infer the port to connect to
          (let [port-file (io/file ".nrepl-port")]
            (.deleteOnExit port-file)
            (spit port-file port))
          (if (:interactive options)
            (run-repl host port (when (:color options) colored-output))
            ;; need to hold process open with a non-daemon thread -- this should end up being super-temporary
            (Thread/sleep Long/MAX_VALUE)))))))

(defn -main
  [& args]
  (try
    (run args)
    (catch clojure.lang.ExceptionInfo ex
      (let [{:keys [::kind ::status]} (ex-data ex)]
        (when (= kind ::exit)
          (clean-up-and-exit status))
        (throw ex)))))
