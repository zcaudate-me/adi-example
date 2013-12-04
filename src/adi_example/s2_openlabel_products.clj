(ns adi-example.s2-openlabel-products
  (:require [adi.core :refer :all :as adi]
            [datomic.api :as d]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(def uri  "datomic:free://localhost:4334/products")
(def uri2 "datomic:mem://adi-example-s1")
(d/create-database uri)

(def schema
  {:product {:name          [{:required true
                              :fulltext true}]
             :manufacturer  [{:fulltext true}]
             :description   [{:fulltext true}]
             :upc_a         [{}]
             :ucc           [{}]
             :mpn           [{}]
             :upc           [{}]
             :ean           [{}]
             :pid           [{}]}})

(def env (adi/connect-env! uri schema true))

(def in-file (io/reader "data/upcdatabase.com.csv"))

(def in-csv (csv/read-csv in-file))

(first in-csv)
["Product name" "manufacturer" "description" "UPC-A" "EAN / UCC-13" "MPN" "UPC" "EAN" "Product id"]

(count in-csv)

(defn make-datom [v]
  (let [[name manu desc upc-a ucc mpn upc ean id] v]
    {:db/id (adi/iid)
     :product/name name
     :product/manufacturer manu
     :product/description desc
     :product/upc_a upc-a
     :product/ucc ucc
     :product/mpn mpn
     :product/upc upc
     :product/ean ean
     :product/pid id}))

@(d/transact (:conn env)
             [(make-datom (second in-csv))]
             )
(def all-data  (->> (map make-datom (next in-csv))
                    (partition-all 300)))

(first all-data)
(doseq [data all-data]
  @(d/transact (:conn env) data))

(time
 (count (select-ids env {:product/description '_})))
