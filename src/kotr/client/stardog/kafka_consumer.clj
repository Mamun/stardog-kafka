(ns kotr.client.stardog.kafka-consumer
  (:require [clojure.algo.generic.functor :refer [fmap]]
            [jackdaw.serdes :as serdes]
            [kotr.kafka.client :as repl]
            [kotr.kafka.client.ksql-api]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [jackdaw.serdes.resolver :as resolver]))


(defn format-data [m]
  (reduce-kv (fn [acc k v]
               (let [k (-> (name k)
                           (clojure.string/lower-case)
                           (clojure.string/replace "-" "_"))]
                 (assoc acc k v))
               ) {} m))


(defn add-data-listener [context sink-connector-name callback]
  (let [schema-atom (atom {})]
    (fn [key value]

      (let [{:keys [ds-name entity_name key]} value]
        (let [;topic-name (clojure.string/upper-case entity_name)  #_(get m :name)
              stream-schema (repl/invoke context {:op "describe-stream" :request entity_name})
              avro-schema (-> (repl/convert-stream-to-avro-schema stream-schema)
                              (assoc :key key :name entity_name))
              config (repl/topic-config-for-data context entity_name)]

          ;(clojure.pprint/pprint config)
          (when (= (clojure.string/lower-case ds-name)
                   (clojure.string/lower-case sink-connector-name))
            (repl/consume context config (fn [data-coll]
                                           (let [o (into [] (comp (map :value)
                                                                  (map format-data)) data-coll)]
                                             (when-not (empty? o)
                                               (callback avro-schema o)))))))))))



(defn add-metadata-listener [context topic-name callback]
  (let [c (repl/topic-config-for-metadata topic-name)]
    (repl/consume context c (fn [w-coll]
                              (doseq [{:keys [key value]} w-coll]
                                (when (and key value)
                                  (log/info "Got new metadata " key)
                                  (callback key value)))))))


(comment
  ;  (take 1 [1 2 3])

  ;  (first (list 2 3 4 ))

  (let [[t-name t-v] (first @schema-atom)
        t-config (build-topic-data-config t-name t-v)]
    ;  (println t-v)

    (repl/consume t-config println))


  (+ 1 1)

  @repl/consume-state-atom




  (def +topic-metadata+
    {"input"
     {:topic-name         "input"
      :partition-count    1
      :replication-factor 1
      :key-serde          {:serde-keyword :jackdaw.serdes.edn/serde}
      :value-serde        {:serde-keyword :jackdaw.serdes.edn/serde}}

     "output"
     {:topic-name         "output"
      :partition-count    1
      :replication-factor 1
      :key-serde          {:serde-keyword :jackdaw.serdes.edn/serde}
      :value-serde        {:serde-keyword :jackdaw.serdes.edn/serde}}})

  (def topic-metadata
    (memoize (fn []
               (fmap #(assoc % :key-serde ((resolver/serde-resolver) (:key-serde %))
                               :value-serde ((resolver/serde-resolver) (:value-serde %)))
                     +topic-metadata+))))

  (repl/list-topics)

  (repl/publish (get (topic-metadata) "input") "the-key" "the-value")

  ;(repl/get-keyvals (get (topic-metadata) "input"))

  (repl/consume (get (topic-metadata) "input") println)

  (reset! repl/consume-state-atom {}))

(comment

  (slurp "module/kotr/src/key-schema.json")
  (slurp "module/kotr/src/value-schema.json")

  (def +topic-metadata+
    {"input1"
     {:topic-name         "input1"
      :partition-count    1
      :replication-factor 1
      :key-serde          {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                           :schema        (slurp "module/kotr/src/key-schema.json")
                           ;:schema-filename "key-schema.json"
                           :key?          true}
      :value-serde        {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                           ;:schema-filename "value-schema.json"
                           :schema        (slurp "module/kotr/src/value-schema.json")
                           :key?          false}}})

  (def serde-resolver
    (partial resolver/serde-resolver :schema-registry-url "http://localhost:8081"))

  (def topic-metadata
    (memoize (fn []
               (fmap #(assoc % :key-serde ((serde-resolver) (:key-serde %))
                               :value-serde ((serde-resolver) (:value-serde %)))
                     +topic-metadata+))))

  (repl/publish (get (topic-metadata) "input1")
                {:the-key "the-key"}
                {:the-value "the-value"})

  (repl/consume (get (topic-metadata) "input1") println)

  (reset! repl/consume-state-atom {})

  ("input" topic-metadata)

  (slurp "value-schema.json"))

(comment

  (slurp "module/kotr/src/key-schema.json")
  (slurp "module/kotr/src/value-schema.json")

  (slurp "data/config_gen/schema/campaign_in.json")

  (def serde-resolver
    (partial resolver/serde-resolver :schema-registry-url "http://localhost:8081"))

  (def schema (-> (slurp "data/config_gen/schema/campaign_in.json")
                  (clojure.string/lower-case)
                  (json/parse-string true)
                  (select-keys [:name :fields])
                  (assoc :type "record")
                  (update :fields (fn [v]
                                    (mapv (fn [m]
                                            (-> (clojure.set/rename-keys m {:schema :type})
                                                ;       (update-in [:type :type ])
                                                )) v)))
                  (json/generate-string)))

  (def +topic-metadata+
    {:topic-name         "CAMPAIGN_IN"
     :partition-count    1
     :replication-factor 1
     :key-serde          (serdes/string-serde)  #_{:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                                   :schema        (slurp "module/kotr/src/key-schema.json")
                                                   ;:schema-filename "key-schema.json"
                                                   :key?          true}
     :value-serde #_(serdes/string-serde)
                         ((serde-resolver)
                           {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                            ;:schema-filename "value-schema.json"
                            :schema        schema
                            :key?          false})})

  (repl/publish +topic-metadata+
                {:the-key "the-key"}
                {:the-value "the-value"})

  (repl/consume +topic-metadata+ #_(get (topic-metadata) "input1") println)

  (reset! repl/consume-state-atom {})

  (slurp "value-schema.json"))


