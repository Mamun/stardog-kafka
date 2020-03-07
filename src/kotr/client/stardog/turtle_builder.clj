(ns kotr.client.stardog.turtle-builder
  (:require [stardog.values :as values]))

;(def key-identifier "_id")

(defn as-class-name [input-string]
  (let [words (clojure.string/split input-string #"[\s_-]+")]
    (clojure.string/join "" (cons (clojure.string/capitalize (first words)) (map clojure.string/capitalize (rest words))))))

(defn as-attr-name [input-string]
  (let [words (clojure.string/split input-string #"[\s_-]+")]
    (->> (clojure.string/join "" (cons (clojure.string/lower-case (first words)) (map clojure.string/capitalize (rest words)))))))

(defn as-value [field-schema field-name field-value]
  (let [t (get field-schema field-name)]
    (if t
      (try
        ; (values/convert field-value)
        field-value
        (catch Exception e
          field-value))
      field-value)))

(defn relation-turtle [key-identifier subject type-iden-name field-name field-value]
  (when (and
          (not= type-iden-name field-name)

          (clojure.string/ends-with? (clojure.string/lower-case field-name) key-identifier))
    (let [e-name (clojure.string/replace (clojure.string/lower-case field-name) (re-pattern key-identifier)  "")
          w (as-attr-name (str "has_" e-name))
          ;field-value ()
          e-name (as-attr-name e-name #_(str e-name "-id"))

          object (str "data" ":" "a" ":" e-name "-" field-value)
          object-as-uri (->> object
                             (values/as-uri))

          type-name (as-class-name e-name)
          class-object-as-uri (values/as-uri (str "allianz" ":" "a" ":" type-name))
          predict (str "rdf" ":" "a" ":" "type")]
      [[subject  (str "allianz" ":" "a" ":" w)   object-as-uri]
       [object  predict   class-object-as-uri]])))


(defn as-turtle-from-entity [key-identifier type-name type-iden-name fields-schema fields]
  (let [predict (str "rdf" ":" "a" ":" "type")
        type-name (as-class-name type-name)
        object (values/as-uri (str "allianz" ":" "a" ":" type-name))
        xf (comp (map
                   (fn [m]
                     ;  (println "------------------------" m)
                     (let [identifier  (get m type-iden-name)
                       ;    _ (println type-iden-name)
                           type-iden2 (clojure.string/replace type-iden-name (re-pattern key-identifier) "")
                           ;     _ (print "identifier " identifier)
                           identifier (clojure.string/join "_" (clojure.string/split identifier #" "))
                           subject (str "data" ":" "a" ":" (str (as-attr-name type-iden2) "-" identifier))
                           turtle [[subject predict object]]
                           xf (comp (remove (fn [[field-name field-value]]
                                              (or (nil? field-value)
                                                  (and (string? field-value)
                                                       (clojure.string/blank? field-value)))))
                                    (map (fn [[field-name field-value]]
                                           (let [field-name (if (keyword? field-name)
                                                              (name field-name)
                                                              field-name)
                                                 ;_ (println "---" field-name)
                                                 new-field-name (as-attr-name field-name)
                                                 field-value (as-value fields-schema new-field-name field-value)

                                                 out [[subject
                                                       (str "allianz" ":" "a" ":" new-field-name)
                                                       field-value]]
                                                 out (if-let [w (relation-turtle key-identifier subject type-iden-name field-name field-value)]
                                                       (into out w)
                                                       out)]
                                             out)))
                                    cat)]
                       (into turtle xf m))))
                 cat)]
    (into [] xf fields)))


(defn avro-to-turtle [context avro-schema v]
  (let [key-identifier (or (get context :key-identifier)
                           "_id")
        n (clojure.string/lower-case (get avro-schema :name))
        key (clojure.string/lower-case (get avro-schema :key))
        type-m (into {} (comp (map (fn [m]
                                     {(clojure.string/lower-case (get m :name))
                                      (second (get m :type))}
                                     ))) (get avro-schema :fields))]
    (as-turtle-from-entity key-identifier n key type-m v)))

(comment


  (avro-to-turtle {} {:name :hello :key "hello_id" :fields [{:name "fname" :type "string"}
                                                         {:name "hello_id" :type "string"}
                                                         {:name "other_id" :type "string"}
                                                         ]}
                  [{"fname" "fname" "hello_id" "1" "other_id" 3}]
                  )

  )