(ns konserve-pg.core
  "PostgreSQL store implemented with HugSQL."
  (:require [konserve.serializers :as ser]
            [konserve.protocols :refer [-serialize -deserialize]]
            [hasch.core :refer [uuid]]
            [hugsql.core :as hugsql]
            [hugsql-adapter-case.adapters :refer [kebab-adapter]]
            [clojure.java.jdbc :as j]
            [clojure.core.async :as async
             :refer [<!! <! >! timeout chan alt! go put! go-loop close!]]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [to-jdbc-uri.core :refer [to-jdbc-uri]]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get-in -update-in
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

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
  (first (db-upsert-record-edn-value db {:id id :edn-value data})))

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
          id (str (uuid fkey))
          val (:edn-value (get-record-edn-value db id))]
      (if (= val nil)
        (go nil)
        (let [res-ch (chan)]
          (try
            (let [bais (ByteArrayInputStream. val)]
              (if-let [res (get-in
                              (second (-deserialize serializer read-handlers bais))
                              rkey)]
                (put! res-ch res)))
            (catch Exception e
              (put! res-ch (ex-info "Could not read key."
                                   {:type :read-error
                                    :key fkey
                                    :exception e})))
            (finally
              (close! res-ch)))
          res-ch)))) 

  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          id (str (uuid fkey))]
      (let [res-ch (chan)]
        (try
          (let [old-bin (:edn-value (get-record-edn-value db id))
                old (when old-bin
                      (let [bais (ByteArrayInputStream. old-bin)]
                        (second (-deserialize serializer write-handlers bais))))
                new (if (empty? rkey)
                      (up-fn old)
                      (update-in old rkey up-fn))]
            (let [baos (ByteArrayOutputStream.)]
              (-serialize serializer baos write-handlers [key-vec new])
              (upsert-record-edn-value db id (.toByteArray baos)))
            (put! res-ch [(get-in old rkey)
                          (get-in new rkey)]))
          (catch Exception e
            (put! res-ch (ex-info "Could not write key."
                                  {:type :write-error
                                   :key fkey
                                   :exception e})))
          (finally
            (close! res-ch)))
        res-ch)))
  
  (-assoc-in [this key-vec val] 
    (-update-in this key-vec (fn [_] val)))
  
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
         :or {serializer (ser/fressian-serializer)
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




