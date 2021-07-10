(ns nrepl.middleware.print
  "Support for configurable printing. See the docstring of `wrap-print` and the
  Pretty Printing section of the Middleware documentation for more information."
  {:author "Michael Griffiths"
   :added  "0.6"}
  (:refer-clojure :exclude [print])
  (:require
   [nrepl.config :as config]
   [nrepl.middleware :refer [set-descriptor!]]
   [nrepl.misc :as misc]
   [nrepl.transport :as transport])
  (:import
   (java.io BufferedWriter PrintWriter StringWriter Writer)
   (nrepl QuotaExceeded)
   (nrepl.transport Transport)))

;; private in clojure.core
(defn- pr-on
  [x w]
  (if *print-dup*
    (print-dup x w)
    (print-method x w))
  nil)

(def ^:dynamic *print-fn*
  "Function to use for printing. Takes two arguments: `value`, the value to print,
  and `writer`, the `java.io.PrintWriter` to print on.

  Defaults to the equivalent of `clojure.core/pr`."
  pr-on)

(def ^:dynamic *stream?*
  "If logical true, the result of printing each value will be streamed to the
  client over one or more messages. Defaults to false."
  false)

(def ^:dynamic *buffer-size*
  "The size of the buffer to use when streaming results. Defaults to 1024."
  1024)

(def ^:dynamic *quota*
  "A hard limit on the number of bytes printed for each value. Defaults to nil. No
  limit will be used if not set."
  nil)

(def default-bindings
  {#'*print-fn* *print-fn*
   #'*stream?* *stream?*
   #'*buffer-size* *buffer-size*
   #'*quota* *quota*})

(defn- bound-configuration
  "Returns a map, suitable for merging into responses handled by this middleware,
  of the currently-bound dynamic vars used for configuration."
  []
  {::print-fn *print-fn*
   ::stream? *stream?*
   ::buffer-size *buffer-size*
   ::quota *quota*})

(def configuration-keys
  [::print-fn ::stream? ::buffer-size ::quota ::keys])

(def providable-keys
  [::print ::options ::stream? ::buffer-size ::quota ::keys])

(defn- to-char-array
  ^chars
  [x]
  (cond
    (string? x) (.toCharArray ^String x)
    (integer? x) (char-array [(char x)])
    :else x))

(defn with-quota-writer
  "Returns a `java.io.Writer` that wraps `writer` and throws `QuotaExceeded` once
  it has written more than `quota` bytes."
  ^java.io.Writer
  [^Writer writer quota]
  (if-not quota
    writer
    (let [total (volatile! 0)]
      (proxy [Writer] []
        (toString []
          (.toString writer))
        (write
          ([x]
           (let [cbuf (to-char-array x)]
             (.write ^Writer this cbuf (int 0) (count cbuf))))
          ([x off len]
           (locking total
             (let [cbuf (to-char-array x)
                   rem (- quota @total)]
               (vswap! total + len)
               (.write writer cbuf ^int off ^int (min len rem))
               (when (neg? (- rem len))
                 (throw (QuotaExceeded.)))))))
        (flush []
          (.flush writer))
        (close []
          (.close writer))))))

(defn replying-PrintWriter
  "Returns a `java.io.PrintWriter` suitable for binding as `*out*` or `*err*`. All
  of the content written to that `PrintWriter` will be sent as messages on the
  transport of `msg`, keyed by `key`."
  ^java.io.PrintWriter
  [key {:keys [transport] :as msg} {:keys [::buffer-size ::quota]}]
  (-> (proxy [Writer] []
        (write
          ([x]
           (let [cbuf (to-char-array x)]
             (.write ^Writer this cbuf (int 0) (count cbuf))))
          ([x off len]
           (let [cbuf (to-char-array x)
                 text (str (doto (StringWriter.)
                             (.write cbuf ^int off ^int len)))]
             (when (pos? (count text))
               (transport/send transport (misc/response-for msg key text))))))
        (flush [])
        (close []))
      (BufferedWriter. (or buffer-size 1024))
      (with-quota-writer quota)
      (PrintWriter. true)))

(defn- send-streamed
  [{:keys [transport] :as msg}
   resp
   {:keys [::print-fn ::keys] :as opts}]
  (let [print-key (fn [key]
                    (let [value (get resp key)]
                      (try
                        (with-open [writer (replying-PrintWriter key msg opts)]
                          (print-fn value writer))
                        (catch QuotaExceeded _
                          (transport/send
                           transport
                           (misc/response-for msg :status ::truncated))))))]
    (run! print-key keys))
  (transport/send transport (apply dissoc resp keys)))

(defn- send-nonstreamed
  [{:keys [transport]}
   resp
   {:keys [::print-fn ::quota ::keys]}]
  (let [print-key (fn [key]
                    (let [value (get resp key)
                          writer (-> (StringWriter.)
                                     (with-quota-writer quota))
                          truncated? (volatile! false)]
                      (try
                        (print-fn value writer)
                        (catch QuotaExceeded _
                          (vreset! truncated? true)))
                      [key (str writer) @truncated?]))
        rf (completing
            (fn [resp [key printed-value truncated?]]
              (cond-> (assoc resp key printed-value)
                truncated? (update ::truncated-keys (fnil conj []) key))))
        resp (transduce (map print-key) rf resp keys)]
    (transport/send transport (cond-> resp
                                (::truncated-keys resp)
                                (update :status #(set (conj % ::truncated)))))))

(defn- printing-transport
  [{:keys [transport] :as msg} opts]
  (reify Transport
    (recv [this]
      (transport/recv transport))
    (recv [this timeout]
      (transport/recv transport timeout))
    (send [this resp]
      (let [{:keys [::stream?] :as opts} (-> (merge msg (bound-configuration) resp opts)
                                             (select-keys configuration-keys))
            resp (apply dissoc resp configuration-keys)]
        (if stream?
          (send-streamed msg resp opts)
          (send-nonstreamed msg resp opts)))
      this)))

(defn- resolve-print
  [{:keys [::print transport] :as msg}]
  (when-let [var-sym (some-> print (symbol))]
    (let [print-var (misc/requiring-resolve var-sym)]
      (when-not print-var
        (let [resp {:status ::error
                    ::error (str "Couldn't resolve var " var-sym)}]
          (transport/send transport (misc/response-for msg resp))))
      print-var)))

(defn wrap-print
  "Middleware that provides printing functionality to other middlewares.

  Returns a handler which transforms any slots specified by
  `:nrepl.middleware.print/keys` in messages sent via the request's transport to
  strings using the provided printing function and options.

  Supports the following options:

  * `::print` – a fully-qualified symbol naming a var whose function to use for
  printing. Must point to a function with signature [value writer options].

  * `::options` – a map of options to pass to the printing function. Defaults to
  `nil`.

  * `::print-fn` – the function to use for printing. In requests, will be
  resolved from the above two options (if provided). Defaults to the equivalent
  of `clojure.core/pr`. Must have signature [writer options].

  * `::stream?` – if logical true, the result of printing each value will be
  streamed to the client over one or more messages.

  * `::buffer-size` – the size of the buffer to use when streaming results.
  Defaults to 1024.

  * `::quota` – a hard limit on the number of bytes printed for each value.

  * `::keys` – a seq of the keys in the response whose values should be printed.

  The options may be specified in either the request or the responses sent on
  its transport. If any options are specified in both, those in the request will
  be preferred."
  [handler]
  (fn [msg]
    (let [msg* (merge (select-keys config/config providable-keys) msg)
          print-var (resolve-print msg*)
          print (fn [value writer]
                  (if print-var
                    (print-var value writer (::options msg*))
                    (pr-on value writer)))
          msg** (assoc msg* ::print-fn print)
          opts (cond-> (select-keys msg** configuration-keys)
                 ;; no print-fn provided in the request, so defer to the response
                 (nil? print-var)
                 (dissoc ::print-fn)
                 ;; in bencode empty list is logical false
                 (contains? msg** ::stream?)
                 (update ::stream? #(if (= [] %) false (boolean %))))]
      (handler (assoc msg** :transport (printing-transport msg** opts))))))

(set-descriptor! #'wrap-print {:requires #{}
                               :expects #{}
                               :handles {}})

(def wrap-print-optional-arguments
  {"nrepl.middleware.print/print" "A fully-qualified symbol naming a var whose function to use for printing. Must point to a function with signature [value writer options]."
   "nrepl.middleware.print/options" "A map of options to pass to the printing function. Defaults to `nil`."
   "nrepl.middleware.print/stream?" "If logical true, the result of printing each value will be streamed to the client over one or more messages."
   "nrepl.middleware.print/buffer-size" "The size of the buffer to use when streaming results. Defaults to 1024."
   "nrepl.middleware.print/quota" "A hard limit on the number of bytes printed for each value."
   "nrepl.middleware.print/keys" "A seq of the keys in the response whose values should be printed."})
