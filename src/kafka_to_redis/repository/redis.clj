(ns kafka-to-redis.repository.redis
  (:require [taoensso.carmine :as car]
            [kafka-to-redis.utils.encoders :refer [get-encode-fn]]
            [taoensso.timbre :refer [error]]))


(defn get-redis-writer [redis-host encoder]
  {:pool {} :spec {:host redis-host} :encoder encoder})

(defn write-value [writer redis-key value]
  (let [encode-fn (get-encode-fn (:encoder writer))
        v (encode-fn value)]
    (try
      (car/wcar writer (car/set redis-key v))
      (catch Exception ex
        (error ex)))))
