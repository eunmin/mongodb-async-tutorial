(ns mongodb-async.aggregation
  (:require [mongodb-async.databases-and-collections :refer [callback-when-finished
                                                             connect
                                                             get-db
                                                             get-collection]]
            [mongodb-async.read-operations :refer [eq]])
  (:import [com.mongodb Block]
           [org.bson.conversions Bson]
           [org.bson Document]
           [org.bson.types ObjectId]
           [com.mongodb.async SingleResultCallback]
           [com.mongodb.async.client
            MongoClients
            MongoDatabase
            MongoCollection
            FindIterable
            MongoClientSettings]
           [com.mongodb.client.model Aggregates Accumulators Projections]
           [com.mongodb.client.model Updates]
           [com.mongodb.client.result UpdateResult DeleteResult]))

;; Perform Aggregation

(defn aggregate [^MongoCollection coll]
  (.forEach (.aggregate coll [(Aggregates/match (eq "categories" "Bakery"))
                              (Aggregates/group "$stars" (Accumulators/sum "count" 1))])
            (proxy [Block] []
              (apply [^Document doc]
                (println (.toJson doc))))
            callback-when-finished))

(defn aggregate2 [^MongoCollection coll]
  (.forEach (.aggregate coll (Aggregates/project
                              (Projections/fields
                               (into-array Bson
                                           (Projections/excludeId)
                                           (Projections/include "name")
                                           (Projections/computed "firstCategory"
                                                                 (Document. "$arrayElemAt"
                                                                            ["$categories" 0]))))))
            (proxy [Block] []
              (apply [^Document doc]
                (println (.toJson doc))))
            callback-when-finished))
