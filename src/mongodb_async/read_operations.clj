(ns mongodb-async.read-operations
  (:require [mongodb-async.databases-and-collections :refer [callback-when-finished
                                                             connect
                                                             get-db
                                                             get-collection]])
  (:import [com.mongodb Block ReadPreference ReadConcern WriteConcern]
           [org.bson.conversions Bson]
           [org.bson Document]
           [com.mongodb.connection ClusterSettings]
           [com.mongodb.async.client
            MongoClients
            MongoDatabase
            MongoCollection
            FindIterable
            MongoClientSettings]
           [com.mongodb.client.model Filters Projections Sorts]))

(def print-block (proxy [Block] []
                   (apply [^Document doc]
                     (println (.toJson doc)))))

;; Query a Collection

(defn find
  ([^MongoCollection coll]
   (find coll {}))
  ([^MongoCollection coll {:keys [query-filter
                                  projection
                                  sorts]
                           :or {query-filter (Document.)}}]
   (let [^FindIterable find-iterable (.find coll query-filter)]
     (when sorts
       (.sort find-iterable sorts))
     (when projection
       (.projection find-iterable projection))
     (.forEach find-iterable print-block callback-when-finished))))

;; (find coll (Filters/eq "name" "456 Cookies Shop"))

;; Query Filters

;; Empty Filter
;; (find coll)

;; (find coll {:query-filter (Document. "stars" (-> (Document. "$gte" 2)
;;                                                  (.append "$lt" 5)
;;                                                  (.append "categories" "Bakery")))})

(defn and [& filters]
  (Filters/and (into-array Bson filters)))

(defn gte [field-name value]
  (Filters/gte field-name value))

(defn lt [field-name value]
  (Filters/lt field-name value))

(defn eq [field-name value]
  (Filters/eq field-name value))

;; (find coll (:query-filter (and (gte "stars" 2)
;;                                (lt "stars" 5)
;;                                (eq "categories" "Bakery"))))

;; FindIterable

;; Projections
;; (find coll {:query-filter (and (gte "stars" 2)
;;                                (lt "stars" 5)
;;                                (eq "categories" "Bakery"))
;;             :projection (-> (Document. "name" 1)
;;                             (.append "stars" 1)
;;                             (.append "categories" 1)
;;                             (.append "_id" 0))})

(defn fields [& projections]
  (Projections/fields (into-array Bson projections)))

(defn include [& field-names]
  (Projections/include (into-array String field-names)))

(def exclude-id (Projections/excludeId))

;; (find coll {:query-filter (and (gte "stars" 2)
;;                                (lt "stars" 5)
;;                                (eq "categories" "Bakery"))
;;             :projection (fields (include "name" "stars" "categories")
;;                                 exclude-id)})

;; Sorts

(defn ascending [& field-name]
  (Sorts/ascending (into-array String field-name)))

;; (find coll {:query-filter (and (gte "stars" 2)
;;                                (lt "stars" 5)
;;                                (eq "categories" "Bakery"))
;;             :projection (fields (include "name" "stars" "categories")
;;                                 exclude-id)
;;             :sort (ascending "name")})


;; Read Preference

(defn settings [hosts]
  (let [^ClusterSettings cluster-setting (-> (ClusterSettings/builder)
                                             (.hosts hosts)
                                             (.build))]
    (-> (MongoClientSettings/builder)
        (.clusterSettings cluster-setting)
        (.readPreference (ReadPreference/secondary))
        (.build))))

;; (create (settings [(ServerAddress. "localhost")]))

;; (create "mongodb://host1:27017,host2:27017,host3:27017?readPreference=secondary")

(defmulti with-read-preference (fn [v _] (class v)))

(defmethod with-read-preference MongoDatabase [^MongoDatabase db ^ReadPreference read-preference]
  (.withReadPreference db read-preference))

(defmethod with-read-preference MongoCollection [^MongoCollection coll ^ReadPreference read-preference]
  (.withReadPreference coll read-preference))

;; (def db (with-read-preference (get-db "test") (ReadPreference/secondary)))

;; For example, in the following, the collectionWithReadPref instance has the read preference
;; of primaryPreferred whereas the read preference of the collection is unaffected. ????

;; (def coll (with-read-preference (get-collection db "restaurants") (ReadPreference/secondary)))

;; Read Concern

(defn settings [hosts]
  (let [^ClusterSettings cluster-setting (-> (ClusterSettings/builder)
                                             (.hosts hosts)
                                             (.build))]
    (-> (MongoClientSettings/builder)
        (.clusterSettings cluster-setting)
        (.readConcern ReadConcern/DEFAULT)
        (.build))))

;; (create "mongodb://host1:27017,host2:27017,host3:27017?readConcernLevel=majority")

(defmulti with-read-concern (fn [v _] (class v)))

(defmethod with-read-concern MongoDatabase [^MongoDatabase db ^ReadConcern read-concern]
  (.withReadConcern db read-concern))

(defmethod with-read-concern MongoCollection [^MongoCollection coll ^ReadConcern read-concern]
  (.withReadConcern coll read-concern))

;; (def db (with-read-concern (get-db "test") (ReadConcern/DEFAULT)))

;; (def coll (with-read-concern (get-collection db "restaurants") (ReadConcern/DEFAULT)))

(defn with-write-concern [^MongoCollection coll ^WriteConcern write-concern]
  (.withWriteConcern coll write-concern))

;; (def coll (-> (get-collection db "restaurants")
;;               (with-read-preference (ReadPreference/secondary))
;;               (with-read-concern (ReadConcern/DEFAULT))
;;               (with-write-concern (WriteConcern/MAJORITY))))
