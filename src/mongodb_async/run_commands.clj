(ns mongodb-async.run-commands
  (:require [mongodb-async.databases-and-collections :refer [connect get-db]])
  (:import
   [org.bson Document]
   [com.mongodb.async SingleResultCallback]
   [com.mongodb.async.client MongoDatabase]))

(defn run-command [^MongoDatabase db ^Document cmd]
  (.runCommand db cmd (proxy [SingleResultCallback] []
                        (onResult [doc t]
                          (println doc)))))
