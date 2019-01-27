(ns nrepl.print.elisions
  "Provides an eliding printer function and a fetch function to help resolving elisions."
  {:added "0.6"}
  (:require [nrepl.print.rich-printer :as rp]))

(defn- soft-store
  "creates a soft-reference based store for elided content."
  [make-form]
  (let [ids-to-refs (atom {})
        refs-to-ids (atom {})
        refq (java.lang.ref.ReferenceQueue.)
        NULL (Object.)]
    (.start (Thread. (fn []
                       (let [ref (.remove refq)
                             id (@refs-to-ids ref)]
                         (swap! refs-to-ids dissoc ref)
                         (swap! ids-to-refs dissoc id))
                       (recur))
                     "soft-elisions-collector"))
    {:put (fn [x]
            (let [x (if (nil? x) NULL x)
                  id (keyword (gensym))
                  ref (java.lang.ref.SoftReference. x refq)]
              (swap! refs-to-ids assoc ref id)
              (swap! ids-to-refs assoc id ref)
              {:get (make-form id)}))
     :get (fn [id]
            (when-some [^java.lang.ref.Reference r (@ids-to-refs id)]
              (let [x (.get r)]
                (if (= NULL x) nil x))))}))

(defonce ^:private elision-store (soft-store #(list `fetch %)))

(defn fetch
  "Resolves an elision id to the elided value."
  [id]
  (if-some [x ((:get elision-store) id)]
    (cond
      (instance? nrepl.print.rich_printer.ElidedKVs x) x
      (string? x) x
      (instance? nrepl.print.rich_printer.MimeContent x) x
      :else (seq x))
    rp/unreachable))

(defn printer
  "Custom printer for the pr-values middleware.
   This printer produces unrepl edn representations of values."
  [x {session :nrepl/session :as opts}]
  (when-not (get @session #'rp/*elide*)
    (swap! session assoc #'rp/*elide* (:put elision-store)))
  (with-bindings @session
    (rp/pr-str x)))
