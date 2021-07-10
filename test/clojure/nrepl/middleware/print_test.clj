(ns nrepl.middleware.print-test
  "Tests for the print middleware. This does not depend on, or use the `eval`
   middleware. Instead, it sends the values to be printed directly in the
   `:value` slot, and uses an echo handler to send it back via the printing
   transport.

   These tests are transport agnostic, and do not deal with sessions, session
   IDs, and message IDs"
  (:refer-clojure :exclude [print])
  (:require [clojure.test :refer [deftest is testing]]
            [nrepl.config :as config]
            [nrepl.core :refer [combine-responses]]
            [nrepl.middleware.print :as print]
            [nrepl.transport :as t])
  (:import [java.io Writer]))

(defn echo-handler
  [{:keys [transport] :as msg}]
  (t/send transport (select-keys msg (::print/keys msg))))

(defn test-transport
  "This transport just collects messages sent into an atom"
  [queue]
  (t/fn-transport
   nil
   #(swap! queue conj %)))

(defmacro testing-print
  "Macro for print tests. Exposes a function `handle`, which takes a msg.
   the `:value` in the message will be printed, using whatever extra print
   arguments supplied."
  {:style/indent 1}
  [doc & body]
  `(testing ~doc
     (let [~'handle (fn [msg#]
                      (let [print-handler#  (print/wrap-print echo-handler)
                            resps#          (atom [])
                            resp-transport# (test-transport resps#)]
                        (print-handler# (assoc msg#
                                               :transport resp-transport#
                                               ::print/keys (or (get msg# ::print/keys)
                                                                #{:value})))
                        (doall @resps#)))]
       ~@body)))

(defn custom-printer
  [value ^Writer writer opts]
  (.write writer (format "<foo %s %s>" value (or (:sub opts) "..."))))

(deftest value-printing
  (testing-print "bad symbol should fall back to default printer"
    (is (= [{:status        #{::print/error}
             ::print/error "Couldn't resolve var my.missing.ns/printer"}
            {:value "42"}]
           (handle {:value        42
                    ::print/keys  #{:value}
                    ::print/print 'my.missing.ns/printer}))))
  (testing-print "custom printing function symbol should be used"
    (is (= [{:value "<foo true ...>"}]
           (handle {::print/keys  #{:value}
                    :value        true
                    ::print/print `custom-printer}))))
  (testing-print "empty print options are ignored"
    (is (= [{:value "<foo 42 ...>"}]
           (handle {:value          42
                    ::print/print   `custom-printer
                    ::print/keys    #{:value}
                    ::print/options {}}))))
  (testing-print "options should be passed to printer"
    (is (= [{:value "<foo 3 bar>"}]
           (handle {:value          3
                    ::print/print   `custom-printer
                    ::print/keys    #{:value}
                    ::print/options {:sub "bar"}})))))

(deftest override-value-printing
  (testing-print "custom ::print/keys"
    (is (= [{:out "[1 2 3 4 5]"}]
           (handle {:value       "value"
                    :out         [1 2 3 4 5]
                    ::print/keys [:out]}))))
  (testing-print "empty ::print/keys"
    (is (= [{}]
           (handle {:value       [1 2 3 4 5]
                    ::print/keys []})))))

(deftest streamed-printing
  (testing-print "value response arrives before ns response"
    (let [responses (handle {:value          (range 10)
                             ::print/stream? 1})]
      (is (= [{:value "(0 1 2 3 4 5 6 7 8 9)"} {}]
             responses))))
  (testing-print "large output should be streamed"
    (let [[resp1 resp2 resp3]
          (handle {:value          (range 512)
                   ::print/stream? 1})]
      (is (-> resp1
              ^String (:value)
              (.startsWith "(0 1 2 3")))
      (is (= {} (dissoc resp1 :value)))
      (is (-> resp2
              ^String (:value)
              (.endsWith "510 511)")))
      (is (= {} (dissoc resp2 :value)))
      (is (= {} resp3))))
  (testing-print "respects buffer-size option"
    (is (= [{:value "(0 1 2 3"}
            {:value " 4 5 6 7"}
            {:value " 8 9 10 "}
            {:value "11 12 13"}
            {:value " 14 15)"}
            {}]
           (handle {:value              (range 16)
                    ::print/stream?     1
                    ::print/buffer-size 8}))))
  (testing-print "works with custom printer"
    (let [[resp1 resp2 resp3] (handle {:value          (range 512)
                                       ::print/stream? 1
                                       ::print/print   `custom-printer})]
      (is (-> resp1
              ^String (:value)
              (.startsWith "<foo (0 1 2 3")))
      (is (= {} (dissoc resp1 :value)))
      (is (-> resp2
              ^String (:value)
              (.endsWith "510 511) ...>")))
      (is (= {} (dissoc resp2 :value)))
      (is (= {} resp3))))
  (testing-print "works with custom printer and print-options"
    (let [[resp1 resp2 resp3] (handle {:value          (range 512)
                                       ::print/stream? 1
                                       ::print/print   `custom-printer
                                       ::print/options {:sub "bar"}})]
      (is (-> resp1
              ^String (:value)
              (.startsWith "<foo (0 1 2 3")))
      (is (= {} (dissoc resp1 :value)))
      (is (-> resp2
              ^String (:value)
              (.endsWith "510 511) bar>")))
      (is (= {} (dissoc resp2 :value)))
      (is (= {} resp3)))))

(deftest multiple-keys
  (testing-print "respects buffer-size option"
    (is (= [{:value "(0 1 2 3"}
            {:value " 4 5 6)"}
            {:out "(6 5 4 3"}
            {:out " 2 1 0)"}
            {}]
           (handle {:value              (range 7)
                    :out                (reverse (range 7))
                    ::print/stream?     1
                    ::print/buffer-size 8
                    ::print/keys        [:value :out]})))))

(deftest print-quota
  (testing-print "quota option respected"
    (is (= [{:value                 "(0 1 2 3"
             :status                #{::print/truncated}
             ::print/truncated-keys [:value]}]
           (handle {:value        (range 512)
                    ::print/quota 8}))))
  (testing-print "works with streamed printing"
    (is (= [{:value "(0 1 2 3"}
            {:status #{::print/truncated}}
            {}]
           (handle {:value          (range 512)
                    ::print/stream? 1
                    ::print/quota   8}))))

  (testing-print "works with custom printer"
    (is (= [{:value                 "<foo (0 "
             :status                #{::print/truncated}
             ::print/truncated-keys [:value]}]
           (handle {:value        (range 512)
                    ::print/print `custom-printer
                    ::print/quota 8}))))
  (testing-print "works with custom printer and streamed printing"
    (is (= [{:value "<foo (0 "}
            {:status #{::print/truncated}}
            {}]
           (handle {:value          (range 512)
                    ::print/print   `custom-printer
                    ::print/stream? 1
                    ::print/quota   8})))))

(defn custom-printer-2
  [value ^Writer writer]
  (.write writer (format "<bar %s>" value)))

;; These tests used to the `session-print-configuration` tests from `core-test`.
;; here, we are simply testing the use of dynamic vars to configure printing
;; behaviour, thus are not going via the `session` middleware to do that.

(deftest dynamic-var-print-configuration
  (testing-print "setting *print-fn* works"
    (is (= [{:value "<bar (0 1 2 3 4 5 6 7 8 9)>"}]
           (binding [print/*print-fn* custom-printer-2]
             (handle {:value (range 10)})))))
  (testing-print "request can still override *print-fn*"
    (is (= [{:value "<foo (0 1 2 3 4 5 6 7 8 9) ...>"}]
           (binding [print/*print-fn* custom-printer-2]
             (handle {:value        (range 10)
                      ::print/print `custom-printer})))))
  (testing-print "setting stream options works"
    (is (= [{:value "<bar (0 "}
            {:value "1 2 3 4 "}
            {:value "5 6 7 8 "}
            {:value "9)>"}
            {}]
           (binding [print/*print-fn*    custom-printer-2
                     print/*stream?*     true
                     print/*buffer-size* 8]
             (handle {:value (range 10)})))))
  (testing-print "request can still override stream options"
    (is (= [{:value "<bar (0 1 2 3 4 5 6 7 8 9)>"}]
           (binding [print/*print-fn*    custom-printer-2
                     print/*stream?*     true
                     print/*buffer-size* 8]
             (handle {:value          (range 10)
                      ::print/stream? nil}))))

    (is (= [{:value "<bar (0 1 2 3 4 "}
            {:value "5 6 7 8 9)>"}
            {}]
           (binding [print/*print-fn*    custom-printer-2
                     print/*stream?*     true
                     print/*buffer-size* 8]
             (handle {:value              (range 10)
                      ::print/buffer-size 16})))))
  (testing-print "setting *quota* works"
    (is (= [{:value                 "<bar (0 "
             :status                #{:nrepl.middleware.print/truncated}
             ::print/truncated-keys [:value]}]
           (binding [print/*print-fn* custom-printer-2
                     print/*quota*    8]
             (handle {:value (range 512)})))))
  (testing-print "request can still override *quota*"
    (is (= [{:value                 "<bar (0 1 2 3 4 "
             :status                #{:nrepl.middleware.print/truncated}
             ::print/truncated-keys [:value]}]
           (binding [print/*print-fn* custom-printer-2
                     print/*quota*    8]
             (handle {:value        (range 512)
                      ::print/quota 16}))))))

(deftest print-fn-var-only
  (testing-print "setting *print-fn* works, but ::print-fn in the message does not"
    (is (not= (handle {:value (range 10)
                       ::print/print-fn custom-printer-2})
              (binding [print/*print-fn* custom-printer-2]
                (handle {:value (range 10)}))))))

(deftest config-override
  (testing-print "config file can be used to set default printer"
    (with-redefs [config/config {::print/print   `custom-printer
                                 ::print/options {:sub "bar"}}]
      (is (= (handle {:value          3
                      ::print/print   `custom-printer
                      ::print/options {:sub "bar"}})
             (handle {:value 3})))))
  (testing-print "config file can be used to set streaming"
    (with-redefs [config/config {::print/stream? 1
                                 ::print/buffer-size 8}]
      (is (= [{:value "(0 1 2 3"}
              {:value " 4 5 6 7"}
              {:value " 8 9)"}
              {}]
             (handle {:value (range 10)}))))))

(deftest print-utils
  (testing-print "Using the built-in util-fn to pretty print"
    (is (= [{:value "(0\n 1\n 2\n 3\n 4\n 5\n 6\n 7\n 8\n 9\n 10\n 11\n 12\n 13\n 14\n 15\n 16\n 17\n 18\n 19\n 20\n 21\n 22\n 23\n 24\n 25\n 26\n 27\n 28\n 29)"}]
           (handle {:value (range 30)
                    ::print/print `nrepl.util.print/pprint})))))
