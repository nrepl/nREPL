(ns nrepl.middleware.auth
  (:require
   [nrepl.misc :refer [response-for]]
   [nrepl.transport :as t])
  (:import
   (java.io File FileNotFoundException)))

(defn find-token
  []
  (or (System/getenv "NREPL_AUTHORIZATION_TOKEN")
      (some-> (System/getenv "NREPL_AUTHORIZATION_TOKEN_FILE") slurp)
      (try
        (slurp (File. (System/getProperty "user.home")
                      ".nrepl/authorization-token"))
        (catch FileNotFoundException ex nil))))

(defn require-nrepl-authorization-token
  "Requires that all messages have an :auth key whose value matches
  either the value of the NREPL_AUTHORIZATION_TOKEN environment
  variable when set, or the content of the file indicated by the
  NREPL_AUTHORIZATION_TOKEN_FILE environment variable when set, or the
  content of ~/.nrepl/authorization-token if it exists.  Returns a
  handler that always responds with a :status including :unauthorized,
  :no-auth-token, and :error if a token can not be found by any of
  these methods, or a :status including :unauthorized,
  :invalid-auth-token, and :error if an incoming message does not
  contain a matching :auth token."
  [handle-message]
  (let [token (find-token)]
    (if-not token
      (fn [{:keys [transport] :as msg}]
        (t/send transport
                (response-for msg :status
                              #{:error :unauthorized :no-auth-token :done})))
      (fn [{:keys [transport auth] :as msg}]
        (if (= auth token)
          (handle-message msg)
          (t/send transport
                  (response-for msg :status
                                #{:error :unauthorized :invalid-auth-token :done})))))))
