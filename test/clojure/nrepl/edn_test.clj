(ns nrepl.edn-test
  (:require [clojure.test :refer [deftest is testing]]
            [nrepl.core :as nrepl]
            [nrepl.server :as server]
            [nrepl.transport :as transport])
  (:import
   (nrepl.server Server)))

(defn return-evaluation
  [message]
  (with-open [^Server server (server/start-server :transport-fn transport/edn)]
    (with-open [^nrepl.transport.FnTransport
                conn (nrepl/connect :transport-fn transport/edn
                                    :port (:port server))]
      (-> (nrepl/client conn 1000)
          (nrepl/message message)
          nrepl/response-values))))

(deftest edn-transport-communication
  (testing "op as a string value"
    (is (= (return-evaluation {:op "eval" :code "(+ 2 3)"})
           [5])))
  (testing "op as a keyword value"
    (is (= (return-evaluation {:op :eval :code "(+ 2 3)"})
           [5])))
  (testing "simple expressions"
    (is (= (return-evaluation {:op "eval" :code "(range 40)"})
           [(eval '(range 40))]))))
