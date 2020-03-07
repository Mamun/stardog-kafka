(ns kotr.client.stardog
  (:require [clojure.tools.logging :as log]
            [stardog.core :as stardog]
            [kotr.kafka.client :as client]
            [kotr.client.stardog.kafka-consumer :as kc]
            [kotr.client.stardog.turtle-builder :as tb]))


(defn push-to-stardog [db-spec turtle-coll]
  (with-open [c (stardog/connect db-spec)]
    (log/debug "inserting to stardog")
    (stardog/with-transaction [c]
                              (doseq [turtle turtle-coll]
                                ;   (log/info "--insert " turtle)
                                (stardog/insert! c turtle)))))


(defn add-ns [db-spec]
  (with-open [c (stardog/connect db-spec)]
    (stardog/add-ns! c "allianz" "http://allianz.com/core#")
    (stardog/add-ns! c "data" "http://data.allianz.com/core#")))


(defn run-process [context]
  (let [{:keys [meta-data-topic-name sink-connector-name db url user pass reasoning]} context
        db-spec (stardog/create-db-spec db url user pass reasoning)]
    (add-ns db-spec)
    (->> (kc/add-data-listener context sink-connector-name (fn [schema value]
                                                             (try
                                                               (log/debug "get new data from kafka............." {})
                                                               (->> (tb/avro-to-turtle context schema value)
                                                                    (push-to-stardog db-spec)
                                                                    ;(println "----------------" )
                                                                    )
                                                               (catch Exception e
                                                                 (do
                                                                   ; (println "Error processing data ")
                                                                   (log/error "Error " e))))))
         (kc/add-metadata-listener context meta-data-topic-name))))

(defn run-process-by-file-name [file-name]
  (log/info "process file name " file-name)
  (let [w (clojure.edn/read-string (slurp file-name))
        ;  kafka-broker (get w :kafka-broker)
        ; schema-url (get w :schema-url)
        ]
    (log/info "run process " w)
    ;  (alter-var-root #'repl/kafka-broker (constantly kafka-broker))
    ;  (alter-var-root #'repl/schema-url (constantly schema-url))
    (log/info "set kafka url is done......................... ")
    (run-process w)))

(defn -main [& args]
  (let [[file-name] args]
    (run-process-by-file-name file-name)))


(comment

  (swap! client/consume-state-atom (fn [m] {"_pipeline_metadata1_" true}))

  @client/consume-state-atom

  (reset! client/consume-state-atom {})

  (run-process-by-file-name "lconfig.edn")

  (-> (clojure.edn/read-string (slurp "lconfig.edn"))
      (client/print-topic "_pipeline_metadata" 2))


  (ksql/topic "ID3_PERSON_IN" 1 println)


  (kc/add-metadata-listener "_pipeline_metadata" (fn [k v]
                                                   (println "---value " v)))

  ;(boolean "true")






  (let [test-db-spec (stardog/create-db-spec "tnf_db" "http://localhost:5820/" "admin" "admin" true)]
    (with-open [c (stardog/connect test-db-spec)]
      (stardog/list-namespaces c))

    )



  (let [schema {:name   "person"
                :key    "id"
                :fields [{:name "id" :type "int"}
                         {:name "fnmae" :type "string"}]}
        v [{"id" 1 :fname "test" :lname "fname"}
           {"id" 2 :fname "test" :lname "fname"}]]
    (->> (avro-to-turtle schema v)
         (push-to-stardog test-db-spec))
    )


  (with-open [c (get-connection)]
    (star/add-ns! c "allianz" "http://ontologies.allianz.com/core#"))


  (with-open [c (get-connection)]
    (star/with-transaction [c]
                           (star/insert! c ["urn:a:product-1" "urn:a:type" "allianz:Product"])
                           ;(star/insert! c ["allianz:a:product-1" "rdf:a:type" "allianz:Product"])
                           ))

  (let [c (get-connection)]
    (star/query c "select ?n { .... }" {:converter str})
    )

  )
