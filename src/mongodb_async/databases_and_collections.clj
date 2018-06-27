(ns mongodb-async.databases-and-collections
  (:import [org.bson.conversions Bson]
           [com.mongodb Block]
           [com.mongodb.async SingleResultCallback]
           [com.mongodb.async.client
            MongoClients
            MongoDatabase
            MongoCollection]
           [com.mongodb.client.model
            CreateCollectionOptions
            ValidationOptions
            Filters]))

;; Databases and Collections
;; http://mongodb.github.io/mongo-java-driver/3.7/driver-async/tutorials/databases-collections/

;; Prerequisites

(def ^SingleResultCallback callback-when-finished
  (proxy [SingleResultCallback] []
    (onResult [_ _]
      (println "Operation Finished!"))))

;; Connect to a MongoDB Deployment

(defn connect []
  (MongoClients/create))

;; Access a Database

(defn get-db [^MongoClients client db-name]
  (.getDatabase client db-name))

;; Get A List of Databases

(defn list-database-names [^MongoClients conn]
  (.forEach (.listDatabaseNames conn)
            (proxy [Block] []
              (apply [s]
                (println s)))
            callback-when-finished))

;; Drop A Database

(defn drop-database [^MongoDatabase db]
  (.drop db callback-when-finished))

;; Access a Collection

(defn get-collection [^MongoDatabase db coll-name]
  (.getCollection db coll-name))

;; Explicitly Create a Collection

(defn create-collection [^MongoDatabase db coll-name]
  (let [^CreateCollectionOptions opts (-> (CreateCollectionOptions.)
                                          (.capped true)
                                          (.sizeInBytes 0x100000))]
    (.createCollection db opts callback-when-finished)))


(defn create-collection-with-validation [^MongoDatabase db coll-name]
  (let [^ValidationOptions coll-opts (.validator (ValidationOptions.)
                                                 (Filters/or
                                                  (into-array
                                                   Bson
                                                   [(Filters/exists "email")
                                                    (Filters/exists "phone")])))
        ^CreateCollectionOptions opts (.validationOptions (CreateCollectionOptions.)
                                                          coll-opts)]
    (.createCollection db coll-name opts callback-when-finished)))

;; Get A List of Collections

(defn list-collection-names [^MongoDatabase db]
  (.forEach (.listCollectionNames db)
            (proxy [Block] []
              (apply [s]
                (println s)))
            callback-when-finished))

;; Drop a Collection

(defn drop-collection [^MongoCollection coll]
  (.drop coll callback-when-finished))
