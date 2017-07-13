(ns kafka-to-redis.worker
  (:require [kafka-to-redis.repository.redis :refer [get-redis-writer write-value]]
            [kafka-to-redis.repository.kafkaconsumer :refer [get-kafka-consumer get-messages]]
            [kafka-to-redis.utils.encoders :refer [get-decode-fn]]
            [clojure.core.match :refer [match]]))


(defn hash-map-merge-worker [redis-key kafka-consumer redis-writer parse-kafka-msg]
  (loop [raw-msgs (get-messages kafka-consumer)
         state {}
         changed? false]
    (let [msgs (map parse-kafka-msg raw-msgs)
          one-val (reduce into {} msgs)
          new-state (merge state one-val)]
      (when (and (empty? raw-msgs)
                 changed?)
        (write-value redis-writer redis-key state))
      (recur (get-messages kafka-consumer) new-state (not (empty? raw-msgs))))))

(defn key-by-key-worker [kafka-consumer redis-writer parse-kafka-msg]
  (loop [raw-msgs (get-messages kafka-consumer)]
      (doseq [raw-msg raw-msgs
              :let [value (parse-kafka-msg raw-msg)
                    k (:key raw-msg)]]
        (write-value redis-writer k value))
      (recur (get-messages kafka-consumer))))

(defn work [opts]
  (let [{:keys [bootstrap-servers topic redis-host redis-key write-policy encoder parse-fn]} opts
        parse-fn (or parse-fn identity)
        decode-fn (get-decode-fn encoder)
        parse-kafka-msg (fn [raw-msg] (-> raw-msg
                                          :value
                                          decode-fn
                                          parse-fn))
        kafka-consumer (get-kafka-consumer bootstrap-servers topic encoder)
        redis-writer (get-redis-writer redis-host encoder)
        worker (match [write-policy]
                      [:key-by-key] key-by-key-worker
                      [:hash-map-merge] (partial hash-map-merge-worker redis-key))]
    (worker kafka-consumer redis-writer parse-kafka-msg)))
