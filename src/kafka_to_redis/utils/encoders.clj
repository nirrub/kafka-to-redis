(ns kafka-to-redis.utils.encoders
  (:require [cheshire.core :as json :refer [generate-string parse-string]]
            [taoensso.nippy :as nippy :refer [freeze thaw]]
            [clojure.core.match :refer [match]])
  (:import [org.apache.kafka.common.serialization ByteArrayDeserializer StringDeserializer]))


(defn get-kafka-deserializer [encoder]
  (match [encoder]
         [:json] (StringDeserializer.)
         [:nippy] (ByteArrayDeserializer.)))

(defn get-decode-fn [encoder]
  (match [encoder]
         [:json] (fn [x] (parse-string x true))
         [:nippy] thaw))

(defn get-encode-fn [encoder]
  (match [encoder]
         [:json] generate-string
         [:nippy] freeze))
