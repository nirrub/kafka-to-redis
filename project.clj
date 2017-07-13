(defproject kafka-to-redis "0.1.0"
  :description "A project that loads kafka stream into redis"
  :url ""
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.taoensso/nippy "2.13.0"]
                 [cheshire "5.7.1"]
                 [com.taoensso/timbre "4.10.0"]
                 [com.taoensso/carmine "2.16.0"] 
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.apache.kafka/kafka_2.11 "0.10.2.0"]
                 [org.apache.kafka/kafka-clients "0.10.2.0"]]
  :main kafka-to-redis.core)
