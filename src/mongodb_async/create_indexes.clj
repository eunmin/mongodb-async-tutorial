(ns mongodb-async.create-indexes
  (:require [mongodb-async.databases-and-collections :refer [callback-when-finished
                                                             connect
                                                             get-db
                                                             get-collection]])
  (:import [com.mongodb Block]
           [org.bson Document]
           [com.mongodb.async.client
            MongoClients
            MongoDatabase
            MongoCollection]
           [com.mongodb.client.model Indexes IndexOptions Filters]))

;; Ascending Index
(defn create-index
  ([^MongoCollection coll ^Indexes indexes]
   (create-index coll indexes (IndexOptions.)))
  ([^MongoCollection coll ^Indexes indexes ^IndexOptions opts]
   (.createIndex coll indexes opts callback-when-finished)))

(defn ascending [field-names]
  (Indexes/ascending (into-array String field-names)))

;; Descending Index

(defn descending [field-names]
  (Indexes/descending (into-array String field-names)))

;; Compound Indexes

(defn compound [& indexes]
  (Indexes/compoundIndex (into-array Indexes indexes)))

;; Text Indexes

(defn text [^String field-name]
  (Indexes/text field-name))

;; Hashed Index

(defn hashed [^String field-name]
  (Indexes/hashed field-name))

;; Geospatial Indexes
(defn geo2dsphere [field-names]
  (Indexes/geo2dsphere (into-array String field-names)))

(defn geo2d [field-names]
  (Indexes/geo2d (into-array String field-names)))

(defn geoHaystack [field-name additional]
  (let [^IndexOptions opts (.bucketSize (IndexOptions.) 1.0)]
    (Indexes/geoHaystack field-name additional)))

;; IndexOptions

(def uniq (.unique (IndexOptions.) true))

(defn partial-filter [^String exists]
  (.partialFilterExpression (IndexOptions.) (Filters/exists exists)))

;; Get a List of Indexes on a Collection

(defn list-indexes [^MongoCollection coll]
  (.forEach (.listIndexes coll)
            (proxy [Block] []
              (apply [^Document doc]
                (println (.toJson doc))))
            callback-when-finished))
