(ns kafka-to-redis.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.match :refer [match]]
            [kafka-to-redis.worker :refer [work]])
  (:gen-class))

(defonce ^:private WRITE_POLICY #{:hash-map-merge :key-by-key})
(defonce ^:private ENCODERS #{:json :nippy})
(defonce ^:private REQUIRED_OPTIONS [:bootstrap-servers :topic :redis-host :write-policy])

(def cli-options
  [["-b" "--bootstrap-servers Bootstrap Servers" "Kafka Bootstrap Servers"
    :parse-fn (fn [x] (->> (clojure.string/split x #",")
                           (map #(str % ":9092"))
                           (clojure.string/join ",")))]
   ["-t" "--topic Kafka Topic" "The topic to read from"]
   ["-r" "--redis-host Redis Host" "The Redis Host Destination"]
   ["-w" "--write-policy Redis Write Policy" (format "The Write Policy into Redis (%s)" (clojure.string/join ", " WRITE_POLICY))
    :parse-fn keyword
    :validate [(fn [x] (contains? WRITE_POLICY x)) (str "The Write Policy must be one of these values: " (clojure.string/join ", " WRITE_POLICY))]]
   ["-p" "--parse-fn Kafka value parse fucntion" "An optional parse-fn (in clojure) to invoke on the Kafka value"
    :parse-fn load-string]
   ["-k" "--redis-key Redis Write Key" "The Redis Key for hash-map-merge policy"]
   ["-e" "--encoder Value Encoder" (format "The Encoder/Decoder used to read from Kafka and write to Redis (%s)" (clojure.string/join ", " ENCODERS))
    :default :json
    :parse-fn keyword
    :validate [(fn [x] (contains? ENCODERS x)) (str "The Encoder must be one of these values: " (clojure.string/join ", " ENCODERS))]]
   ["-h" "--help"]])

(defn -main [& args]
  (let [opts (parse-opts args cli-options)
        help? (get-in opts [:options :help])
        errors? (not (empty? (:errors opts)))
        populated? (not (every? (:options opts) REQUIRED_OPTIONS))
        missing-redis-key-on-merge? (and (= (get-in opts [:options :write-policy]) :hash-map-merge)
                                         (clojure.string/blank? (get-in opts [:options :redis-key])))]
    (match [help? errors?  populated? missing-redis-key-on-merge?]
           [true _ _ _] (println (:summary opts))
           [_ true _ _] (doseq [e (:errors opts)] (println e))
           [_ _ true _] (println "Not all required options provided. The options are:" (:summary opts))
           [_ _ _ true] (println "For hash-map-merge policy you must provide a redis-key parameter")
           :else (work (:options opts)))))
