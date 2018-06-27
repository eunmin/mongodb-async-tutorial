(ns mongodb-async.connect-to-mongodb
  (:import [com.mongodb ConnectionString ServerAddress]
           [com.mongodb.async.client MongoClients MongoClientSettings]
           [com.mongodb.connection ClusterSettings]
           [com.mongodb.connection.netty NettyStreamFactoryFactory]))

;; Connect to MongoDB
;; http://mongodb.github.io/mongo-java-driver/3.7/driver-async/tutorials/connect-to-mongodb/

;; Connect to a Standalone MongoDB Instance
(defn create
  ([]
   (MongoClients/create))
  ([param]
   (MongoClients/create param)))

;; (create "mongodb://localhost")
;; (create (ConnectionString. "mongodb://localhost"))

(defn settings [hosts]
  (let [^ClusterSettings cluster-setting (-> (ClusterSettings/builder)
                                             (.hosts hosts)
                                             (.build))]
    (-> (MongoClientSettings/builder)
        (.clusterSettings cluster-setting)
        (.build))))

;; (create (settings [(ServerAddress. "localhost")]))

;; Connect to a Replica Set

;; (create "mongodb://host1:27017,host2:27017,host3:27017")
;; (create "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet")
;; (create (ConnectionString. "mongodb://host1:27017,host2:27017,host3:27017")
;; (create (ConnectionString. "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet")
;; (create (settings [(ServerAddress. "host1" 27017) (ServerAddress. "host2" 27017)]))

;; Connect to a Sharded Cluster

;; with mongos

;; Connection Options

;; (create "mongodb://localhost/?streamType=netty")

;; (defn stream-factory-settings []
;;   (let [^ClusterSettings cluster-setting (-> (ClusterSettings/builder)
;;                                              (.hosts [(ServerAddress.)])
;;                                              (.build))]
;;     (-> (MongoClientSettings/builder)
;;         (.clusterSettings cluster-setting)
;;         (.streamFactoryFactory (.build (NettyStreamFactoryFactory/builder)))
;;         (.build))))
