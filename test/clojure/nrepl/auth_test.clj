(ns nrepl.auth-test
  "Authentication/authorization system tests (though at the moment,
  there is only support for authorization).  Should not run tests that
  expose local nrepl access unless NREPL_TEST_TRUST_ALL_LOCAL_ACCESS
  is set to true, yes, or 1 in the environment."
  (:require
   [clojure.test :refer [deftest is]]
   [nrepl.core :as nrepl]
   [nrepl.core-test :refer [*server* call-with-bound-repl-server]]
   [nrepl.misc :refer [response-for]]
   [nrepl.server :refer [start-server]]
   [nrepl.transport :as t]))

(def trust-all-local-access?
  (#{"yes" "true" "1"} (System/getenv "NREPL_TEST_TRUST_ALL_LOCAL_ACCESS")))

(defn random-string [n]
  (let [sr (java.security.SecureRandom.)
        b (byte-array n)
        ;; Note: mutates b
        random-bytes (apply concat (repeatedly #(do (.nextBytes sr b) (vec b))))]
    (->> random-bytes
         (filter #(and (> % 34) (< % 127)))
         (take n)
         (map char)
         (apply str))))

(defmacro is-unauthorized [expected-complaint response]
  `(let [status# (:status ~response)]
     (is (status# "done"))
     (is (status# "error"))
     (is (status# "unauthorized"))
     (is (status# ~expected-complaint))))

(defmacro is-authorized [response]
  `(let [status# (:status ~response)]
     (is (status# "done"))
     (is (not (status# "unauthorized")))))

(defn message-response [client message]
  (-> client (nrepl/message message) nrepl/combine-responses))

(defn tokenized-authorization-wrapper
  [token]
  (fn [handle-message]
    (fn [{:keys [transport auth] :as msg}]
      (if (= auth token)
        (handle-message msg)
        (t/send transport
                (response-for msg :status
                              #{:error :unauthorized :invalid-auth-token :done}))))))

(defmacro with-server [args & body]
  `(call-with-bound-repl-server ~args #(do ~@body)))

(when trust-all-local-access?
  (deftest access-allowed-without-auth-wrapper
    (with-server []
      (with-open [transport (nrepl/connect :port (:port *server*))]
        (let [client (nrepl/client transport Long/MAX_VALUE)]
          (is-authorized (message-response client {:op :eval :code ":foo"})))))))

(deftest access-can-be-controlled-by-auth-wrapper
  (let [token (random-string 64)]
    (with-server [:auth-wrapper (tokenized-authorization-wrapper token)]
      (with-open [transport (nrepl/connect :port (:port *server*))]
        (let [client (nrepl/client transport Long/MAX_VALUE)]
          (is-unauthorized "invalid-auth-token"
                           (message-response client {:op :eval :code ":foo"}))
          (is-unauthorized "invalid-auth-token"
                           (message-response client {:op :eval :code ":foo" :auth "foo"}))
          (is-authorized (message-response client {:op :eval :code ":foo" :auth token})))))))
