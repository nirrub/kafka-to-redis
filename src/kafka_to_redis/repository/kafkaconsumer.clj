(ns kafka-to-redis.repository.kafkaconsumer
  (:require [kafka-to-redis.utils.encoders :refer [get-kafka-deserializer]])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.common.serialization Deserializer]))

(defn get-kafka-consumer [bootstrap-servers topic encoder]
  (let [config {"bootstrap.servers" bootstrap-servers
                "group.id" (first (clojure.string/split (.. java.net.InetAddress getLocalHost getHostName) #"\."))
                "enable.auto.commit" "false"
                "auto.offset.reset" "earliest"}
        deserializer (get-kafka-deserializer encoder)
        consumer (KafkaConsumer. ^java.util.Map config ^Deserializer deserializer ^Deserializer deserializer)]
    (.subscribe ^KafkaConsumer consumer (vector topic))
    consumer))

(defn get-messages [consumer]
  (let [consumer-records (.poll ^KafkaConsumer consumer 1000)
        parse-fn (fn [^ConsumerRecord rec]
                   {:topic (.topic rec)
                    :partition (.partition rec)
                    :offest (.offset rec)
                    :key (.key rec)
                    :value (.value rec)})]
    (map parse-fn consumer-records)))
