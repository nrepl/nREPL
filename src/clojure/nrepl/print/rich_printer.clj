(ns nrepl.print.rich-printer
  "A printer that produces a pure EDN representation of a Clojure value.
  Features elisions, MIME attachments and general purpose object browsing.
  Initially developed for unrepl."
  (:refer-clojure :exclude [pr pr-str])
  (:require
   [clojure.core :as core]
   [clojure.string :as str]
   [clojure.edn :as edn]
   [clojure.main :as main]))

; even with limits on breadth and depth of printing, we can easily get big values:
; the biggest tree would have fitting in these lilites would have breadth-limit^depth-limit
; so even with small defaults such as 8 for depth and 10 for breadth we get 100 millions of leaves.
; That's why we introduce the notion of budget, which is breadth-limits*depth-limit.
; (this is a heuristic which could be tuned)
; Once the budget is exhausted everything is elided.
(def ^:dynamic ^:private *print-budget*)

(def ^:dynamic *elide*
  "Function of one argument (the value to elide) which returns its elision
  (as a map with at least a :get key containing a form to evaluate to retrieve the elided value)
  or nil when the elision is definitive."
  (constantly nil))

(def ^:dynamic *realize-on-print*
  "Set to false to avoid realizing lazy sequences."
  true)

(def ^:dynamic *string-length* 72)

(def defaults {#'*print-length* 10
               #'*print-level* 8
               #'*string-length* 72})

(defn- bump [n m]
  (if (< n (- Long/MAX_VALUE m))
    (+ n m)
    Long/MAX_VALUE))

(defn ensure-defaults [bindings]
  (let [bindings (merge-with #(or %1 %2) bindings defaults)]
    (assoc bindings #'*print-budget*
           (long (min (* 1N (bindings #'*print-level*) (bindings #'*print-length*)) Long/MAX_VALUE)))))

(defprotocol MachinePrintable
  (-print-on [x write rem-depth]))

;; clojure 1.10 support
(defn- really-satisfies? [protocol x]
  (when (class x)
    (let [default (get (:impls protocol) Object)
          impl (find-protocol-impl protocol x)]
      (not (identical? impl default)))))

(def ^:private datafiable?
  (if-some [Datafiable (some-> 'clojure.core.protocols/Datafiable resolve deref)]
    #(or (get (meta %) 'clojure.core.protocols/datafy) (really-satisfies? Datafiable %))
    (constantly false)))

(def ^:private datafy
  (or (some-> 'clojure.core.protocols/datafy resolve deref)
      (clojure.lang.Var$Unbound. #'datafy)))

(def ^:private navigable?
  (if-some [Navigable (some-> 'clojure.core.protocols/Navigable resolve deref)]
    #(or (get (meta %) 'clojure.core.protocols/nav) (really-satisfies? Navigable %))
    (constantly false)))

(def ^:private nav
  (or (some-> 'clojure.core.protocols/nav resolve deref)
      (clojure.lang.Var$Unbound. #'nav)))

(when (bound? #'datafy)
  (require 'clojure.datafy))

(defn- browsify
  "only for datafiables"
  [x]
  (let [d (datafy x)]
    (if (and (navigable? x) (or (map? d) (vector? d)))
      (reduce-kv (fn [d k v] (assoc d k (tagged-literal 'unrepl/browsable [v #(nav x k v)]))) d d)
      d)))
;; end of 1.10

(defn print-on
  [write x rem-depth]
  (let [rem-depth (dec rem-depth)
        budget (set! *print-budget* (dec *print-budget*))]
    (if (and (or (neg? rem-depth) (neg? budget)) (pos? (or *print-length* 1)))
      ; the (pos? (or *print-length* 1)) is here to prevent stack overflows
      (binding [*print-length* 0]
        (print-on write x 0))

      (do
        (when (datafiable? x)
          (write "#unrepl/browsable ["))
        (when (and *print-meta* (meta x))
          (write "#unrepl/meta [")
          (-print-on (meta x) write rem-depth)
          (write " "))
        (-print-on x write rem-depth)
        (when (and *print-meta* (meta x))
          (write "]"))
        (when (datafiable? x)
          (write " ")
          (set! *print-budget* (bump *print-budget* 1))
          (print-on write (tagged-literal 'unrepl/... (*elide* (lazy-seq [(list (browsify x))]))) (inc rem-depth))
          (write "]"))))))

(defn base64-encode [^java.io.InputStream in]
  (let [table "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        sb (StringBuilder.)]
    (loop [shift 4 buf 0]
      (let [got (.read in)]
        (if (neg? got)
          (do
            (when-not (= shift 4)
              (let [n (bit-and (bit-shift-right buf 6) 63)]
                (.append sb (.charAt table n))))
            (cond
              (= shift 2) (.append sb "==")
              (= shift 0) (.append sb \=))
            (str sb))
          (let [buf (bit-or buf (bit-shift-left got shift))
                n (bit-and (bit-shift-right buf 6) 63)]
            (.append sb (.charAt table n))
            (let [shift (- shift 2)]
              (if (neg? shift)
                (do
                  (.append sb (.charAt table (bit-and buf 63)))
                  (recur 4 0))
                (recur shift (bit-shift-left buf 6))))))))))

(defn base64-decode [^String s]
  (let [table "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        in (java.io.StringReader. s)
        bos (java.io.ByteArrayOutputStream.)]
    (loop [bits 0 buf 0]
      (let [got (.read in)]
        (when-not (or (neg? got) (= 61 #_\= got))
          (let [buf (bit-or (.indexOf table got) (bit-shift-left buf 6))
                bits (+ bits 6)]
            (if (<= 8 bits)
              (let [bits (- bits 8)]
                (.write bos (bit-shift-right buf bits))
                (recur bits (bit-and 63 buf)))
              (recur bits buf))))))
    (.toByteArray bos)))

(defmacro ^:private blame-seq [& body]
  `(try (seq ~@body)
        (catch Throwable t#
          (list (tagged-literal 'unrepl/lazy-error t#)))))

(defn- may-print? [s]
  (or *realize-on-print* (not (instance? clojure.lang.IPending s)) (realized? s)))

(declare ->ElidedKVs)

(defn- print-kvs
  [write kvs rem-depth]
  (let [print-length *print-length*]
    (loop [kvs kvs i 0]
      (if (and (< i print-length) (pos? *print-budget*))
        (when-some [[[k v] & kvs] (seq kvs)]
          (when (pos? i) (write ", "))
          (print-on write k rem-depth)
          (write " ")
          (print-on write v rem-depth)
          (recur kvs (inc i)))
        (when (seq kvs)
          (when (pos? i) (write ", "))
          (write "#unrepl/... nil ")
          (print-on write (tagged-literal 'unrepl/... (*elide* (->ElidedKVs kvs))) rem-depth))))))

(defn- print-vs
  [write vs rem-depth]
  (let [print-length *print-length*]
    (loop [vs vs i 0]
      (when-some [[v :as vs] (blame-seq vs)]
        (when (pos? i) (write " "))
        (if (and (< i print-length) (pos? *print-budget*) (may-print? vs))
          (if (and (tagged-literal? v) (= (:tag v) 'unrepl/lazy-error))
            (print-on write v rem-depth)
            (do
              (print-on write v rem-depth)
              (recur (rest vs) (inc i))))
          (print-on write (tagged-literal 'unrepl/... (*elide* vs)) rem-depth))))))

(defrecord ElidedKVs [s]
  MachinePrintable
  (-print-on [_ write rem-depth]
    (write "{")
    (print-kvs write s rem-depth)
    (write "}")))

(def atomic? (some-fn nil? true? false? char? string? symbol? keyword? #(and (number? %) (not (ratio? %)))))

(defn- roundtrippable? [x]
  (try
    (= x (-> x core/pr-str read-string))
    (catch Exception e
      false)))

(defn- as-str
  "Like pr-str but escapes all ASCII control chars."
  [x]
  ;hacky
  (cond
    (string? x) (str/replace (core/pr-str x) #"\p{Cntrl}"
                             #(format "\\u%04x" (int (.charAt ^String % 0))))
    (char? x) (str/replace (core/pr-str x) #"\p{Cntrl}"
                           #(format "u%04x" (int (.charAt ^String % 0))))
    (and (or (symbol? x) (keyword? x)) (not (roundtrippable? x)))
    (str (if (keyword? x) "#unrepl/bad-keyword [" "#unrepl/bad-symbol [")
         (as-str (namespace x)) " " (as-str (name x)) "]") ; as-str in case they are really really bad
    :else (core/pr-str x)))

(defmacro ^:private latent-fn [& fn-body]
  `(let [d# (delay (binding [*ns* (find-ns '~(ns-name *ns*))] (eval '(fn ~@fn-body))))]
     (fn
       ([] (@d#))
       ([x#] (@d# x#))
       ([x# & xs#] (apply @d# x# xs#)))))

(defrecord MimeContent [mk-in]
  MachinePrintable
  (-print-on [_ write rem-depth]
    (with-open [in (mk-in)]
      (write "#unrepl/base64 \"")
      (write (base64-encode in))
      (write "\""))))

(defn- mime-content [mk-in]
  (when-some [e (*elide* (MimeContent. mk-in))]
    {:content (tagged-literal 'unrepl/... e)}))

(def ^:dynamic *object-representations*
  "map of classes to functions returning their representation component (3rd item in #unrepl/object [class id rep])"
  {clojure.lang.IDeref
   (fn [x]
     (let [pending? (and (instance? clojure.lang.IPending x) ; borrowed from https://github.com/brandonbloom/fipp/blob/8df75707e355c1a8eae5511b7d73c1b782f57293/src/fipp/ednize.clj#L37-L51
                         (not (.isRealized ^clojure.lang.IPending x)))
           [ex val] (when-not pending?
                      (try [false @x]
                           (catch Throwable e
                             [true e])))
           failed? (or ex (and (instance? clojure.lang.Agent x)
                               (agent-error x)))
           status (cond
                    failed? :failed
                    pending? :pending
                    :else :ready)]
       {:unrepl.ref/status status :unrepl.ref/val val}))

   clojure.lang.AFn
   (fn [x]
     (-> x class .getName main/demunge))

   java.io.File (fn [^java.io.File f]
                  (into {:path (.getPath f)}
                        (when (.isFile f)
                          {:attachment (tagged-literal 'unrepl/mime
                                                       (into {:content-type "application/octet-stream"
                                                              :content-length (.length f)}
                                                             (mime-content #(java.io.FileInputStream. f))))})))

   java.awt.Image (latent-fn [^java.awt.Image img]
                             (let [w (.getWidth img nil)
                                   h (.getHeight img nil)]
                               (into {:width w, :height h}
                                     {:attachment
                                      (tagged-literal 'unrepl/mime
                                                      (into {:content-type "image/png"}
                                                            (mime-content #(let [bos (java.io.ByteArrayOutputStream.)]
                                                                             (when (javax.imageio.ImageIO/write
                                                                                    (doto (java.awt.image.BufferedImage. w h java.awt.image.BufferedImage/TYPE_INT_ARGB)
                                                                                      (-> .getGraphics (.drawImage img 0 0 nil)))
                                                                                    "png" bos)
                                                                               (java.io.ByteArrayInputStream. (.toByteArray bos)))))))})))

   Object (fn [x]
            (if (-> x class .isArray)
              (seq x)
              (str x)))})

; datafy and friends shoud be but to good use there
(defn- object-representation [x]
  (reduce-kv (fn [_ class f]
               (when (instance? class x) (reduced (f x)))) nil *object-representations*)) ; todo : cache

(defn- class-form [^Class x]
  (if (.isArray x) [(-> x .getComponentType class-form)] (symbol (.getName x))))

(def unreachable (tagged-literal 'unrepl/... nil))

(defn- print-tag-lit-on [write tag form rem-depth]
  (write (str "#" tag " "))
  (print-on write form rem-depth))

(defn- sat-inc [n]
  (if (= Long/MAX_VALUE n)
    n
    (unchecked-inc n)))

(defn- print-trusted-tag-lit-on [write tag form rem-depth]
  (print-tag-lit-on write tag form (sat-inc rem-depth)))

;; --
;; Throwable->map backport from Clojure 1.9
;;
;; The behavior of clojure.core/Throwable->map changed from 1.8 to 1.9.
;; We need the (more correct) behavior in 1.9.
;;
;; https://github.com/clojure/clojure/blob/master/changes.md#33-other-fixes

(defn StackTraceElement->vec'
  "Constructs a data representation for a StackTraceElement"
  {:added "1.9"}
  [^StackTraceElement o]
  [(symbol (.getClassName o)) (symbol (.getMethodName o)) (.getFileName o) (.getLineNumber o)])

(defn Throwable->map'
  "Constructs a data representation for a Throwable."
  {:added "1.7"}
  [^Throwable o]
  (let [base (fn [^Throwable t]
               (merge {:type (symbol (.getName (class t)))
                       :message (.getLocalizedMessage t)}
                      (when-let [ed (ex-data t)]
                        {:data ed})
                      (let [st (.getStackTrace t)]
                        (when (pos? (alength st))
                          {:at (StackTraceElement->vec' (aget st 0))}))))
        via (loop [via [], ^Throwable t o]
              (if t
                (recur (conj via t) (.getCause t))
                via))
        ^Throwable root (peek via)
        m {:cause (.getLocalizedMessage root)
           :via (vec (map base via))
           :trace (vec (map StackTraceElement->vec'
                            (.getStackTrace (or root ^Throwable o))))}
        data (ex-data root)]
    (if data
      (assoc m :data data)
      m)))

;; use standard implementation if running in Clojure 1.9 or above,
;; backported version otherwise

(def Throwable->map''
  (if (neg? (compare (mapv *clojure-version* [:major :minor]) [1 9]))
    Throwable->map'
    Throwable->map))

;; --

(extend-protocol MachinePrintable
  clojure.lang.TaggedLiteral
  (-print-on [x write rem-depth]
    (case (:tag x)
      unrepl/... (binding ; don't elide the elision 
                  [*print-length* Long/MAX_VALUE
                   *print-level* Long/MAX_VALUE
                   *print-budget* Long/MAX_VALUE
                   *string-length* Long/MAX_VALUE]
                   (write (str "#" (:tag x) " "))
                   (print-on write (:form x) Long/MAX_VALUE))
      unrepl/browsable (let [[x thunk] (:form x)
                             rem-depth (inc rem-depth)]
                         (set! *print-budget* (bump *print-budget* 2))
                         (write (str "#" (:tag x) " ["))
                         (print-on write (:form x) rem-depth)
                         (write " ")
                         (print-on write (tagged-literal 'unrepl/... (*elide* (lazy-seq [(thunk)]))) rem-depth))
      (print-tag-lit-on write (:tag x) (:form x) rem-depth)))

  clojure.lang.Ratio
  (-print-on [x write rem-depth]
    (print-trusted-tag-lit-on write "unrepl/ratio"
                              [(.numerator x) (.denominator x)] rem-depth))

  clojure.lang.Var
  (-print-on [x write rem-depth]
    (print-tag-lit-on write "clojure/var"
                      (when-some [ns (:ns (meta x))] ; nil when local var
                        (symbol (name (ns-name ns)) (name (:name (meta x)))))
                      rem-depth))

  Throwable
  (-print-on [t write rem-depth]
    (print-tag-lit-on write "error" (Throwable->map'' t) rem-depth))

  Class
  (-print-on [x write rem-depth]
    (print-tag-lit-on write "unrepl.java/class" (class-form x) rem-depth))

  java.util.Date (-print-on [x write rem-depth] (write (core/pr-str x)))
  java.util.Calendar (-print-on [x write rem-depth] (write (core/pr-str x)))
  java.sql.Timestamp (-print-on [x write rem-depth] (write (core/pr-str x)))
  clojure.lang.Namespace
  (-print-on [x write rem-depth]
    (print-tag-lit-on write "unrepl/ns" (ns-name x) rem-depth))
  java.util.regex.Pattern
  (-print-on [x write rem-depth]
    (print-tag-lit-on write "unrepl/pattern" (str x) rem-depth))
  String
  (-print-on [x write rem-depth]
    (if (<= (count x) *string-length*)
      (write (as-str x))
      (let [i (if (and (Character/isHighSurrogate (.charAt ^String x (dec *string-length*)))
                       (Character/isLowSurrogate (.charAt ^String x *string-length*)))
                (inc *string-length*) *string-length*)
            prefix (subs x 0 i)
            rest (subs x i)]
        (if (= rest "")
          (write (as-str x))
          (do
            (write "#unrepl/string [")
            (write (as-str prefix))
            (write " ")
            (print-on write (tagged-literal 'unrepl/... (*elide* rest)) rem-depth)
            (write "]")))))))

(defn- print-coll [open close write x rem-depth]
  (write open)
  (print-vs write x rem-depth)
  (write close))

(extend-protocol MachinePrintable
  nil
  (-print-on [_ write _] (write "nil"))
  Object
  (-print-on [x write rem-depth]
    (cond
      (atomic? x) (write (as-str x))
      (map? x)
      (do
        (when (record? x)
          (write "#") (write (.getName (class x))) (write " "))
        (write "{")
        (print-kvs write x rem-depth)
        (write "}"))
      (vector? x) (print-coll "[" "]" write x rem-depth)
      (seq? x) (print-coll "(" ")" write x rem-depth)
      (set? x) (print-coll "#{" "}" write x rem-depth)
      :else
      (print-trusted-tag-lit-on write "unrepl/object"
                                [(class x) (format "0x%x" (System/identityHashCode x)) (object-representation x)
                                 {:bean {unreachable (tagged-literal 'unrepl/... (*elide* (ElidedKVs. (bean x))))}}]
                                (sat-inc rem-depth))))) ; is very trusted

(defn pr [x]
  (let [^java.io.Writer out *out*
        write (fn [^String s] (.write out s))
        bindings (select-keys (get-thread-bindings) [#'*print-length* #'*print-level* #'*string-length*])]
    (with-bindings (into (ensure-defaults bindings) {#'*print-readably* true})
      (print-on write x *print-level*))))

(defn pr-str [x]
  (with-out-str (pr x)))

(defn full-pr-str "no elision" [x]
  (binding [*print-length* Long/MAX_VALUE
            *print-level* Long/MAX_VALUE
            *string-length* Integer/MAX_VALUE]
    (pr-str x)))
