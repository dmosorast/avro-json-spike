(ns tap-postgres.custom-logical-type-v2
  (:require [cheshire.core :as cheshire]
            [clojure.tools.logging :as log]
            #_[taoensso.tufte :as tufte :refer (defnp p profiled profile)])
  (:import [custom_logical_type JsonConversion JsonLogicalType JsonLogicalTypeFactory JsonGenericDatumWriter
            JsonConversion$PersistentArrayMapConversion
            ]
           [org.apache.avro
            LogicalTypes
            LogicalTypes$LogicalTypeFactory
            Schema
            Schema$RecordSchema
            Schema$Parser
            Schema$Field
            Schema$Type]
           [org.apache.avro.generic
            GenericData
            GenericData$Array
            GenericData$Record
            GenericDatumWriter
            GenericDatumReader]
           [org.apache.avro.file DataFileWriter DataFileReader DataFileStream SeekableByteArrayInput]
           [java.io ByteArrayOutputStream ByteArrayInputStream]))

;; We need to register our logical type to get it into the framework
(LogicalTypes/register JsonLogicalType/JSON_LOGICAL_TYPE_NAME (JsonLogicalTypeFactory.))

(doseq [conversion JsonConversion/INSTANCES]
  (.addLogicalTypeConversion (GenericData/get) conversion))

;; Confirm that the types we expect are registered
(.getConversionByClass (GenericData/get) (type {}))
(.getConversionByClass (GenericData/get) (type []))

;; With an adjustment to make JsonConversion try to convert "Object", this should work
(def avro-schema (Schema/createRecord
                  "a_name"
                  "a_doc"
                  "a_namespace"
                  false
                  [(Schema$Field. "a_map" (.addToSchema (JsonLogicalType.)
                                                        (Schema/create Schema$Type/STRING)) ;; Type is irrelevant
                                  nil
                                  nil)
                   (Schema$Field. "a_nested_array" (.addToSchema (JsonLogicalType.)
                                                                 (Schema/create Schema$Type/STRING)) ;; Type is irrelevant
                                  nil
                                  nil)]))

;; To ensure that the final schema has it (this wasn't the case with the Parser)
(.getLogicalType (.schema (.getField avro-schema "a_map")))

;; Creating a Generic Record
(def avro-rec (doto
                  (GenericData$Record. avro-schema)
                (.put "a_map" {:a 123 "b" "foo"})
                (.put "a_nested_array" [1 [2 3] nil])))

;; Test Using This in a real avro serialize/deserialize situation
(defn serialize-deserialize [avro-rec]
  (let [avro-schema  (.getSchema avro-rec)
        ;; Writing
        baos         (ByteArrayOutputStream.)
        ;; Reading
        ;; Note: No custom reading required.
        datum-reader (GenericDatumReader. avro-schema)
        ]
    ;; BAOS post-serialize
    ;; NOTE: The JsonGenericDatumWriter
    (doto (DataFileWriter. (GenericDatumWriter. avro-schema (GenericData/get)))
      (.create avro-schema baos)
      (.append avro-rec)
      (.close))
    ;; BAOS post-deserialize
    (let [gdr (DataFileStream. (SeekableByteArrayInput. (.toByteArray baos)) datum-reader)]
      (loop [recs []]
        (if (.hasNext ^DataFileReader gdr)
          (recur (conj recs (.next gdr)))
          recs)))))

(defn avro->clojure [avro-rec]
  (into {} (map (fn [f] [(.name f) (.get avro-rec (.pos f))]) (.getFields (.getSchema avro-rec)))))

;; HACK: So, the only way I was able to get this to work is to subclass
;; the GenericDatumWriter and fix its type-matching to allow the
;; Conversion of type Json to use Object as its convertedType

;; This is really throwing a wrench in the whole situation, since it calls the function based on schema type AND casts the value.
;; https://github.com/apache/avro/blob/release-1.9.2/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumWriter.java#L79
(-> avro-rec
    serialize-deserialize
    first
    avro->clojure
    )
