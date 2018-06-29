(ns mongodb-async.write-operations
  (:require [mongodb-async.databases-and-collections :refer [callback-when-finished
                                                             connect
                                                             get-db
                                                             get-collection]])
  (:import [com.mongodb Block ReadPreference ReadConcern WriteConcern]
           [org.bson.conversions Bson]
           [org.bson Document]
           [com.mongodb.connection ClusterSettings]
           [com.mongodb.async SingleResultCallback]
           [com.mongodb.async.client
            MongoClients
            MongoDatabase
            MongoCollection
            FindIterable
            MongoClientSettings]
           [com.mongodb.client.model Filters Projections Sorts]))

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


;; Replace an Existing Document


;; Delete Documents
