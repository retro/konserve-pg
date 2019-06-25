(ns konserve-pg.core
  "PostgreSQL store implemented with HugSQL."
  (:require [konserve.serializers :as ser]
            [konserve.protocols :refer [-serialize -deserialize]]
            [hasch.core :refer [uuid]]
            [hugsql.core :as hugsql]
            [hugsql-adapter-case.adapters :refer [kebab-adapter]]
            [clojure.java.jdbc :as j]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go go-loop]]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [to-jdbc-uri.core :refer [to-jdbc-uri]]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get-in -update-in
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget]]))

(hugsql/def-db-fns "konserve_pg/db.sql" {:adapter (kebab-adapter)})

(defn connection-uri [uri]
  {:connection-uri (to-jdbc-uri uri)})

(defn create-table! [db]
  (db-create-table db))

(defn record-exists? [db id]
  (-> (db-record-exists db {:id id})
      first
      :exists))

(defn get-record-edn-value [db id]
  (first (db-get-record-edn-value db {:id id})))

(defn upsert-record-edn-value [db id data]
  (first (db-upsert-record-edn-value db (assoc data :id id))))

(defn delete-record [db id]
  (db-delete-record db {:id id}))

(defn get-record-attachment [db id]
  (first (db-get-record-attachment db {:id id})))

(defn upsert-record-attachment [db id attachment]
  (first (db-upsert-record-attachment db {:id id :attachment attachment})))

(defrecord PgStore [db serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [id (str (uuid key))]
      (go (try (record-exists? db id)
               (catch Exception e
                 (ex-info "Could not access edn value."
                          {:type :access-error
                           :id id
                           :key key
                           :exception e}))))))
  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          id (str (uuid fkey))]
      (go (try (get-in (->> id
                            (get-record-edn-value db)
                            :edn-value
                            (-deserialize serializer read-handlers)
                            second)
                       rkey)
               (catch Exception e
                 (ex-info "Could not read edn value."
                          {:type :read-error
                           :id id
                           :key fkey
                           :exception e}))))))

  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))

  (-update-in [this key-vec up-fn]
    (go (try
          (let [[fkey & rkey] key-vec
                id (str (uuid fkey))
                doc (get-record-edn-value db id)]
            ((fn trans [doc attempt]
               (let [old (->> doc :edn-value (-deserialize serializer read-handlers) second)
                     new (if-not (empty? rkey)
                           (update-in old rkey up-fn)
                           (up-fn old))]
                 (cond (and (not doc) new)
                       [nil (get-in (->> (upsert-record-edn-value db id 
                                                                  {:edn-value #_(pr-str [key-vec new])
                                                                   (with-out-str
                                                                     (-serialize serializer
                                                                                 *out*
                                                                                 write-handlers
                                                                                 [key-vec new]))})
                                         :edn-value
                                         (-deserialize serializer read-handlers)
                                         second)
                                    rkey)]

                                        ;                       (and (not doc) (not new))
                                        ;                       [nil nil]

                                        ;                       (not new)
                                        ;                       (do (cl/delete-document db doc) [(get-in old rkey) nil])

                       :else
                       (let [old* (get-in old rkey)
                             new (try (upsert-record-edn-value
                                       db
                                       id
                                       (assoc doc
                                              :edn-value (with-out-str
                                                           (-serialize
                                                            serializer
                                                            *out*
                                                            write-handlers
                                                            [key-vec
                                                             (if-not (empty? rkey)
                                                               (update-in (-deserialize
                                                                           serializer
                                                                           read-handlers
                                                                           (:edn-value doc))
                                                                          rkey
                                                                          up-fn)
                                                               (up-fn (-deserialize
                                                                       serializer
                                                                       read-handlers
                                                                       (:edn-value doc))))]))))
                                      (catch clojure.lang.ExceptionInfo e
                                        (if (< attempt 10)
                                          (trans (get-record-edn-value db id) (inc attempt))
                                          (throw e))))
                             new* (-> (-deserialize serializer read-handlers (:edn-value new))
                                      second
                                      (get-in rkey))]
                         [old* new*])))) doc 0))
          (catch Exception e
            (ex-info "Could not write edn value."
                     {:type :write-error
                      :key key-vec
                      :exception e})))))
  (-dissoc [this key]
    (go
      (let [id (str (uuid key))]
        (try
          (delete-record db id)
          nil
          (catch Exception e
            (ex-info "Cannot delete key."
                     {:type :delete-error
                      :key key
                      :exception e}))))))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (go (locked-cb
         {:input-stream (:attachment (get-record-attachment db (str (uuid key))))})))
  (-bassoc [this key val]
    (let [id (str (uuid key))]
      (go
        (upsert-record-attachment db id val)))))


(defn new-pg-store
  "Constructs a PostgreSQL store either with URL for db or a PostgreSQL connection
  object and optionally read and write handlers for custom types according to
  incognito and a serialization protocol according to konserve."
  [db & {:keys [serializer read-handlers write-handlers]
         :or {serializer (ser/string-serializer)
              read-handlers (atom {})
              write-handlers (atom {})}}]
  (let [db (if (string? db) (connection-uri db) db)]
    (go (try
          (create-table! db)
          (map->PgStore {:db db
                         :serializer serializer
                         :read-handlers read-handlers
                         :write-handlers write-handlers
                         :locks (atom {})})
          (catch Exception e
            (ex-info "Cannot connect to PostgreSQL."
                     {:type :db-error
                      :db db
                      :exception e}))))))




