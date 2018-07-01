(ns mongodb-async.write-operations
  (:require [mongodb-async.databases-and-collections :refer [callback-when-finished
                                                             connect
                                                             get-db
                                                             get-collection]]
            [mongodb-async.read-operations :refer [eq]])
  (:import [com.mongodb Block ReadPreference ReadConcern WriteConcern]
           [org.bson.conversions Bson]
           [org.bson Document]
           [org.bson.types ObjectId]
           [com.mongodb.connection ClusterSettings]
           [com.mongodb.async SingleResultCallback]
           [com.mongodb.async.client
            MongoClients
            MongoDatabase
            MongoCollection
            FindIterable
            MongoClientSettings]
           [com.mongodb.client.model Filters Projections Sorts UpdateOptions]
           [com.mongodb.client.model Updates]
           [com.mongodb.client.result UpdateResult DeleteResult]))

(def single-result-callback
  (proxy [SingleResultCallback] []
    (onResult [result t]
      (println "Inserted!"))))

;; Insert New Document
(def example-doc
  (-> (Document. "name" "Cafe Con Leche")
      (.append "contact" (-> (Document. "phone" "228-555-0149")
                             (.append "email" "cafeconleche@example.com")
                             (.append "location" [-73.92502 40.8279556])))
      (.append "stars" 3)
      (.append "categories" ["Bakery" "Coffee" "Pastries"])))

(defn insert-one [^MongoCollection coll ^Document doc]
  (.insertOne coll doc single-result-callback))

(def example-doc1
  (-> (Document. "name" "Amarcord Pizzeria")
      (.append "contact" (-> (Document. "phone" "264-555-0193")
                             (.append "email" "amarcord.pizzeria@example.net")
                             (.append "location" [-73.88502 40.749556])))
      (.append "stars" 2)
      (.append "categories" ["Pizzeria", "Italian", "Pasta"])))

(def example-doc2
  (-> (Document. "name" "Blue Coffee Bar")
      (.append "contact" (-> (Document. "phone" "604-555-0102")
                             (.append "email" "bluecoffeebar@example.com")
                             (.append "location" [-73.97902, 40.8479556])))
      (.append "stars" 5)
      (.append "categories" ["Coffee", "Pastries"])))

(defn insert-many [^MongoCollection coll docs]
  (.insertMany coll docs single-result-callback))

;; Update Existing Documents

(defn update-one [^MongoCollection coll]
  (.updateOne coll
              (eq "_id" (ObjectId. "57506d62f57802807471dd41"))
              (Updates/combine
               (into-array Bson
                           [(Updates/set "stars" 1)
                            (Updates/set "contact.phone" "228-555-9999")
                            (Updates/currentDate "lastModified")]))
              (proxy [SingleResultCallback] []
                (onResult [^UpdateResult result t]
                  (println (.getModifiedCount result))))))

(defn update-many [^MongoCollection coll]
  (.updateMany coll
               (eq "stars" 2)
               (Updates/combine
                (into-array Bson
                            [(Updates/set "stars" 0)
                             (Updates/currentDate "lastModified")]))
               (proxy [SingleResultCallback] []
                 (onResult [^UpdateResult result t]
                   (println (.getModifiedCount result))))))

(defn update-one-with-update-options [^MongoCollection coll]
  (.updateOne coll
              (eq "_id" (ObjectId. "57506d62f57802807471dd41"))
              (Updates/combine
               (into-array Bson
                           [(Updates/set "stars" 1)
                            (Updates/set "contact.phone" "228-555-9999")
                            (Updates/currentDate "lastModified")]))
              (-> (UpdateOptions.)
                  (.upsert true)
                  (.bypassDocumentValidation true))
              (proxy [SingleResultCallback] []
                (onResult [^UpdateResult result t]
                  (println (.getModifiedCount result))))))

;; Replace an Existing Document

(defn replace-one [^MongoCollection coll]
  (.replaceOne coll
               (eq "_id" (ObjectId. "57506d62f57802807471dd41"))
               (-> (Document. "name" "Green Salads Buffet")
                   (.append "contact" "TBD")
                   (.append "categories" ["Salads" "Health Foods" "Buffet"]))
               (proxy [SingleResultCallback] []
                 (onResult [^UpdateResult result t]
                   (println (.getModifiedCount result))))))

(defn replace-one-with-update-options [^MongoCollection coll]
  (.replaceOne coll
               (eq "_id" (ObjectId. "57506d62f57802807471dd41"))
               (-> (Document. "name" "Green Salads Buffet")
                   (.append "contact" "TBD")
                   (.append "categories" ["Salads" "Health Foods" "Buffet"]))
               (-> (UpdateOptions.)
                   (.upsert true)
                   (.bypassDocumentValidation true))
               (proxy [SingleResultCallback] []
                 (onResult [^UpdateResult result t]
                   (println (.getModifiedCount result))))))

;; Delete Documents

(defn delete-one [^MongoCollection coll]
  (.deleteOne coll
              (eq "_id" (ObjectId. "57506d62f57802807471dd41"))
              (proxy [SingleResultCallback] []
                (onResult [^DeleteResult result t]
                  (println (.getDeleteCount result))))))

(defn delete-many [^MongoCollection coll]
  (.deleteMany coll
               (eq "start" 4)
               (proxy [SingleResultCallback] []
                 (onResult [^DeleteResult result t]
                   (println (.getDeleteCount result))))))

;; Write Concern

(defn with-write-concern [^MongoCollection coll ^WriteConcern write-concern]
  (.withWriteConcern coll write-concern))
