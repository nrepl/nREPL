(ns nrepl.middleware.auth-test
  "Authentication/authorization system tests (though at the moment,
  there is only support for authorization).  Should not run tests that
  expose local nrepl access unless NREPL_TEST_TRUST_ALL_LOCAL_ACCESS
  is set to true, yes, or 1 in the environment."
  (:require
   [clojure.java.io :as io]
   [clojure.java.shell :refer [sh]]
   [clojure.test :refer [deftest is]]
   [nrepl.core :as nrepl]
   [nrepl.server :refer [start-server]])
  (:import
   (java.lang ProcessBuilder ProcessBuilder$Redirect)
   (java.io File)
   (java.nio.file Files Path Paths StandardOpenOption)
   (java.nio.file.attribute FileAttribute PosixFilePermissions)
   (java.util HashSet)))

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

(defn as-file [x] (if (instance? Path x) (.toFile x) (io/as-file x)))
(defn as-path [x] (if (instance? Path x) x (.toPath (io/as-file x))))

(defn mkdirs
  ([path] (mkdirs path (make-array FileAttribute 0)))
  ([path attrs] (Files/createDirectories path attrs)))

(defn- posix-file-attr [s]
  (-> s PosixFilePermissions/fromString PosixFilePermissions/asFileAttribute))

(defn- private-file [path]
  (let [options (HashSet. [StandardOpenOption/CREATE_NEW
                           StandardOpenOption/APPEND])]
    (Files/newByteChannel (as-path path) options
                          (into-array [(posix-file-attr "rw-------")]))))

(defn call-with-private-temp-dir [prefix f]
  (let [tmp (Files/createTempDirectory (as-path "target") prefix
                                       (into-array [(posix-file-attr "rwx------")]))]
    (try
      (f tmp)
      (finally
        ;; Life's too short?
        (let [status (sh "rm" "-r" (-> tmp .toAbsolutePath str))]
          (when-not (zero? (:exit status))
            (throw (Exception. status))))))))

(defmacro with-private-temp-dir [prefix dir-var & body]
  `(call-with-private-temp-dir ~prefix #(let [~dir-var %] ~@body)))

(defn nrepl-proc
  ([home env] (nrepl-proc home env {:require-auth? true}))
  ([home env {:keys [require-auth?]}]
   (let [home (as-file home)
         pb (ProcessBuilder.
             (concat
              ["java" "-cp" (System/getProperty "java.class.path")
               (str "-Duser.home=" (.getAbsolutePath home))
               "nrepl.main"]
              (when require-auth?
                ["--auth-wrapper" "nrepl.middleware.auth/require-nrepl-authorization-token"])))]
     (.redirectOutput pb ProcessBuilder$Redirect/INHERIT)
     (doto (.environment pb) .clear (.putAll env))
     (let [proc (.start pb)
           err (line-seq (io/reader (.getErrorStream proc)))]
       (loop [err err]
         (when (seq err)
           (if-let [[_ port] (re-matches #"nREPL listening on (\d+)" (first err))]
             (do
               (future (doseq [line err] (binding [*out* *err*] (println line))))
               [proc (Integer. port)])
             (do
               (binding [*out* *err*] (println (first err)))
               (recur (rest err))))))))))

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

(when trust-all-local-access?
  (deftest access-allowed-without-auth-wrapper
    (with-private-temp-dir "auth-test" home
      (let [home (as-file home)
            env (dissoc (into {} (System/getenv))
                        "NREPL_AUTHORIZATION_TOKEN"
                        "NREPL_AUTHORIZATION_TOKEN_FILE")
            [proc port] (nrepl-proc home env {:require-auth? false})]
        (try
          (with-open [transport (nrepl/connect :port port)]
            (let [client (nrepl/client transport Long/MAX_VALUE)]
              (is-authorized (message-response client {:op :eval :code ":foo"}))))
          (finally
            (.destroy proc)))))))

(deftest access-denied-when-no-token
  (with-private-temp-dir "auth-test" home
    (let [home (.toFile home)
          env (dissoc (into {} (System/getenv))
                      "NREPL_AUTHORIZATION_TOKEN"
                      "NREPL_AUTHORIZATION_TOKEN_FILE")
          [proc port] (nrepl-proc home env)]
      (try
        (with-open [transport (nrepl/connect :port port)]
          (let [client (nrepl/client transport Long/MAX_VALUE)]
            (is-unauthorized "no-auth-token"
                             (message-response client {:op :eval :code ":foo"}))
            (is-unauthorized "no-auth-token"
                             (message-response client {:op :eval :code ":foo" :auth "foo"}))))
        (finally
          (.destroy proc))))))

;;; ??? (deftest access-denied-for-bad-token ...)

(deftest home-token-used-if-no-other-specification
  (with-private-temp-dir "auth-test" home
    (let [token (random-string 64)
          home (.toFile home)
          config (doto (File. home ".nrepl") (.mkdir))
          _ (spit (File. config "authorization-token") token)
          env (dissoc (into {} (System/getenv))
                      "NREPL_AUTHORIZATION_TOKEN"
                      "NREPL_AUTHORIZATION_TOKEN_FILE")
          [proc port] (nrepl-proc home env)]
      (try
        (with-open [transport (nrepl/connect :port port)]
          (let [client (nrepl/client transport Long/MAX_VALUE)]
            (is-unauthorized "invalid-auth-token"
                             (message-response client {:op :eval :code ":foo"}))
            (is-authorized (message-response client {:op :eval :code ":foo"
                                                     :auth token}))))
        (finally
          (.destroy proc))))))

(deftest env-file-token-preferred-over-home
  (with-private-temp-dir "auth-test" home
    (let [home-token (random-string 64)
          file-token (random-string 64)
          home (.toFile home)
          token-file (File. home "alternate-token")
          config (doto (File. home ".nrepl") (.mkdir))
          _ (spit (File. config "authorization-token") home-token)
          _ (spit token-file file-token)
          env (-> (into {} (System/getenv))
                  (dissoc "NREPL_AUTHORIZATION_TOKEN")
                  (assoc "NREPL_AUTHORIZATION_TOKEN_FILE" (.getAbsolutePath token-file)))
          [proc port] (nrepl-proc home env)]
      (try
        (with-open [transport (nrepl/connect :port port)]
          (let [client (nrepl/client transport Long/MAX_VALUE)]
            (is-unauthorized "invalid-auth-token"
                             (message-response client {:op :eval :code ":foo"}))
            (is-unauthorized "invalid-auth-token"
                             (message-response client {:op :eval :code ":foo"
                                                       :auth home-token}))
            (is-authorized (message-response client {:op :eval :code ":foo"
                                                     :auth file-token}))))
        (finally
          (.destroy proc))))))

(deftest env-var-token-preferred-over-home-or-env-file-token
  (with-private-temp-dir "auth-test" home
    (let [home-token (random-string 64)
          file-token (random-string 64)
          env-token (random-string 64)
          home (.toFile home)
          token-file (File. home "alternate-token")
          config (doto (File. home ".nrepl") (.mkdir))
          _ (spit (File. config "authorization-token") home-token)
          _ (spit token-file file-token)
          env (assoc (into {} (System/getenv))
                     "NREPL_AUTHORIZATION_TOKEN" env-token
                     "NREPL_AUTHORIZATION_TOKEN_FILE" (.getAbsolutePath token-file))
          [proc port] (nrepl-proc home env)]
      (try
        (with-open [transport (nrepl/connect :port port)]
          (let [client (nrepl/client transport Long/MAX_VALUE)]
            (is-unauthorized "invalid-auth-token"
                             (message-response client {:op :eval :code ":foo"}))
            (is-unauthorized "invalid-auth-token"
                             (message-response client {:op :eval :code ":foo"
                                                       :auth home-token}))
            (is-unauthorized "invalid-auth-token"
                             (message-response client {:op :eval :code ":foo"
                                                       :auth file-token}))
            (is-authorized (message-response client {:op :eval :code ":foo"
                                                     :auth env-token}))))
        (finally
          (.destroy proc))))))
