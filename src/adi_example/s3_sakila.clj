(ns adi-example-s3-sakila
  (:require [lobos
             [connectivity :as conn]
             [core :as lb]
             [schema :as ls]]
;;           [korma.core :as k]
;;            [korma.db :as kdb]
            [hara.common :refer (error)]
            [adi.core :as adi]
            [clojure.java.jdbc :as j]
            [clojure.java.jdbc.sql :as sql]
            [datomic.api :as d]
            [hara.common :refer [hash-map? keyword-join merge-nested]]))

(comment
  (def kdb (kdb/mysql {:db "employees"
                       :user "root"
                       :password "root"}))

  (kdb/defdb mysql kdb)

  (k/defentity employees)

  (first (k/select employees)))

(def db {:classname "com.mysql.jdbc.Driver" ; must be in classpath
         :subprotocol "mysql"
         :subname "//localhost:3306/sakila"
         :user "root"
         :password "root"})

#_(conn/open-global db)



(use 'lobos.analyzer)
;;(use 'lobos.metadata)

(def jdsch (analyze-schema))

(def adi-type
  {:tinyint-unsigned :long
   :smallint-unsigned :long
   :mediumint-unsigned :long
   :smallint :long
   :tinyint :long
   :timestamp :instant
   :varchar :string
   :char :string
   :set :string
   :enum :string
   :clob :string
   :blob :bytes
   :date :instant
   :integer :long
   :boolean :boolean
   :datetime :instant
   :year :instant
   :decimal :bigdec})

;;(all-data-types jdsch)


(defn all-data-types [jdsch]
  (set
   (mapcat (fn [[tk tab]]
             (map (fn [[ck col]]
                    (-> col :data-type :name))
                  (:columns tab)))
           (:tables jdsch))))

;; Data
(defn process-data-type [col]
  [(let [dname (-> col :data-type :name)
         entry {:type (adi-type dname)}
         entry (if (:default col) entry entry)
         entry (if (= dname :clob) entry (assoc entry :fulltext true))]
     entry)])

(defn adi-schema [jdsch]
  (into {}
        (map (fn [[tk tab]]
               [tk (into {}
                         (map (fn [[ck col]]
                                [ck (process-data-type col)])
                              (:columns tab)))])
             (:tables jdsch))))


;; Refs
(defn process-ident-id-add-link [id kset]
  (let [nid (keyword (str (name id) "_link"))]
    (if (kset nid) (error "PROCESS_IDENT_ID_ADD_LINK: " nid " already in kset: " kset)
        nid)))

(defn process-ident-id [id kset]
  (let [nm (str (name id))
        nid (cond (.contains nm "_id")
                  (keyword (.replaceFirst nm "_id" ""))

                  (.contains nm "id_")
                  (keyword (.replaceFirst nm "id_" ""))

                  :else id)]
    (if (kset nid)
      (process-ident-id-add-link id kset)
      nid)))

(defn ref-col [tk constraint kset]
  (let [ident-id (-> constraint :columns first)]
     #_(println "Hello: " (-> constraint :columns))
     {:ns tk
     :ident_id ident-id
     :ident (process-ident-id ident-id kset)
     :link_ns (:parent-table constraint)
     :link_id (-> constraint :parent-columns first)}))

(defn find-ref-cols [jdsch]
  (let [tables (:tables jdsch)
        constraints (-> jdsch :tables :constraints)]
    (->> (mapcat (fn [[tk tab]]
                   (let [kset (set (keys (-> tab :columns)))]
                     (map (fn [[_ constraint]]
                            (let [res (if (instance? lobos.schema.ForeignKeyConstraint constraint)
                                        [tk (ref-col tk constraint kset)])]
                              res))
                          (:constraints tab))))
                 (:tables jdsch))
         (filter identity)
         vec)))

(defn ref-schema [jdsch]
  (let [recs
        (map (fn [[t mv]]
               [[t (:ident mv)]
                [{:type :ref
                  :ref {:ns (:link_ns mv)}}]])
             (find-ref-cols jdsch))]
    (loop [rf# {}
           recs# recs]
      (if (empty? recs#) rf#
          (let [[[ks v] & more] recs#]
            (recur (assoc-in rf# ks v) (next recs#)))))))

;; Unique
(defn find-unique-cols [jdsch]
  (->> (mapcat (fn [[tk tab]]
                 (map (fn [[_ constraint]]
                        (if (instance? lobos.schema.UniqueConstraint constraint)
                          [tk (-> constraint :columns first)]))
                      (:constraints tab)))
               (:tables jdsch))
       (filter identity)))

(defn add-unique-cols [jdsch sch]
  (let [uniques (find-unique-cols jdsch)]
    (loop [sch# sch
           uniques# uniques]
      (if-let [ks (first uniques#)]
        (recur (assoc-in sch# (concat ks '(0 :unique)) :value)
               (next uniques#))
        sch#))))

(defn entity-id-map [env attr]
  (into {}
        (d/q '[:find ?id ?x :in $ ?attr :where
               [?x ?attr ?id]]
             (d/db (:conn env))
             attr)))

(defn entity-id-rfmap [env attr]
  (into {}
        (d/q '[:find ?x ?id :in $ ?attr :where
               [?x ?attr ?id]]
             (d/db (:conn env))
             attr)))

(defn link-attr [env [ns attr]]
  (println attr)
  (let [ident (keyword-join [(:ns attr) (:ident attr)])
        link-lu (entity-id-map env (keyword-join [(:link_ns attr) (:link_id attr)]))
        ref-lu  (entity-id-rfmap env (keyword-join [(:ns attr) (:ident_id attr)]))]
    (println ident)
    (map (fn [[x id]]
           {:db/id x ident (get link-lu id)})
         ref-lu)))

#_(d/transact (:conn env) (link-attr env [:address {:ns :address, :ident_id :city_id, :ident :city, :link_ns :city, :link_id :city_id}]))

(defn link-refs [env jdsch]
  (let [attrs (find-ref-cols jdsch)]
    (doseq [attr attrs]
      (let [conns (link-attr env attr)]
        ;;(println "heoueou" (first conns))
        @(d/transact (:conn env) conns)
        ))))
(comment
  (link-refs env jdsch)
  ;;(-> env :schema :tree :address :city)
  (ref-schema jdsch)
  {:inventory {:store [{:type :ref, :ref {:ns :store}}]}, :film_actor {:film [{:type :ref, :ref {:ns :film}}]},
   :customer {:store [{:type :ref, :ref {:ns :store}}]},
   :store {:manager_staff [{:type :ref, :ref {:ns :staff}}]}, :staff {:store [{:type :ref, :ref {:ns :store}}]}, :film_category {:film [{:type :ref, :ref {:ns :film}}]}, :city {:country [{:type :ref, :ref {:ns :country}}]}, :payment {:staff [{:type :ref, :ref {:ns :staff}}]}, :film {:original_language [{:type :ref, :ref {:ns :language}}]}, :address {:city [{:type :ref, :ref {:ns :city}}]}, :rental {:staff [{:type :ref, :ref {:ns :staff}}]}}

  (-> env :schema :tree :customer :store)
  (find-ref-cols jdsch)
  [[:address {:ns :address, :ident_id :city_id, :ident :city, :link_ns :city, :link_id :city_id}]
   [:city {:ns :city, :ident_id :country_id, :ident :country, :link_ns :country, :link_id :country_id}]
   [:customer {:ns :customer, :ident_id :address_id, :ident :address, :link_ns :address, :link_id :address_id}]
   [:customer {:ns :customer, :ident_id :store_id, :ident :store, :link_ns :store, :link_id :store_id}]
   [:film {:ns :film, :ident_id :language_id, :ident :language, :link_ns :language, :link_id :language_id}]
   [:film {:ns :film, :ident_id :original_language_id, :ident :original_language, :link_ns :language, :link_id :language_id}]
   [:film_actor {:ns :film_actor, :ident_id :actor_id, :ident :actor, :link_ns :actor, :link_id :actor_id}]
   [:film_actor {:ns :film_actor, :ident_id :film_id, :ident :film, :link_ns :film, :link_id :film_id}]
   [:film_category {:ns :film_category, :ident_id :category_id, :ident :category, :link_ns :category, :link_id :category_id}]
   [:film_category {:ns :film_category, :ident_id :film_id, :ident :film, :link_ns :film, :link_id :film_id}]
   [:inventory {:ns :inventory, :ident_id :film_id, :ident :film, :link_ns :film, :link_id :film_id}]
   [:inventory {:ns :inventory, :ident_id :store_id, :ident :store, :link_ns :store, :link_id :store_id}] [:payment {:ns :payment, :ident_id :customer_id, :ident :customer, :link_ns :customer, :link_id :customer_id}] [:payment {:ns :payment, :ident_id :rental_id, :ident :rental, :link_ns :rental, :link_id :rental_id}] [:payment {:ns :payment, :ident_id :staff_id, :ident :staff, :link_ns :staff, :link_id :staff_id}] [:rental {:ns :rental, :ident_id :customer_id, :ident :customer, :link_ns :customer, :link_id :customer_id}] [:rental {:ns :rental, :ident_id :inventory_id, :ident :inventory, :link_ns :inventory, :link_id :inventory_id}] [:rental {:ns :rental, :ident_id :staff_id, :ident :staff, :link_ns :staff, :link_id :staff_id}] [:staff {:ns :staff, :ident_id :address_id, :ident :address, :link_ns :address, :link_id :address_id}] [:staff {:ns :staff, :ident_id :store_id, :ident :store, :link_ns :store, :link_id :store_id}] [:store {:ns :store, :ident_id :address_id, :ident :address, :link_ns :address, :link_id :address_id}] [:store {:ns :store, :ident_id :manager_staff_id, :ident :manager_staff, :link_ns :staff, :link_id :staff_id}]])
;;(adi/select env {:city/city '_})




(defn import-table [env db table]
  (let [recs (mapv (fn [x] {table x}) (j/query db (sql/select * table)))]
    (doseq [rec recs]
      ( (println rec))
      (adi/insert! env rec))))

;;(import-table env db :category)

(defn create-env-schema [uri jdsch]
  (let [sch (->> (adi-schema jdsch)
                 (merge-nested (ref-schema jdsch))
                 (add-unique-cols jdsch))
        env (adi/connect-env! "datomic:mem://adi-s3-mysql" sch true)]
    env))

(def jdsch (analyze-schema))
(def env (create-env-schema "datomic:mem://adi-s3-mysql" jdsch))


;;(-> env :schema :tree :category :category_id)
;;(import-table env db :category)

(defn import-env [env db jdsch]
  (let [table-names (keys (:tables jdsch))]
    (doseq [t table-names]
      (println "importing: " t)
      (let [recs (mapv (fn [x] {t x}) (j/query db (sql/select * t)))]
        (println "number recs: " (count recs))
        (adi/insert! env recs))
      (println "finished: " t))))

(import-env env db jdsch)

(adi/select env {:customer/address/city/name "Mannheim"})

#_(->> (adi/select env {:country/country '_}
                 :return {:country {:cities {:last_update :unchecked
                                             :country_id :unchecked
                                             :city_id :unchecked}
                                    :country_id :unchecked
                                    :last_update :unchecked}})
     (sort (fn [a b]
             (> (-> a :country :cities count)
                (-> b :country :cities count))))
     (take 5))


(count (adi/select env {:customer/address/city/country/name "China"}))


(-> env :schema :tree :store keys)

(link-refs env jdsch)

;; (def sch (add-unique-cols jdsch (adi-schema jdsch)))

;;(def env (adi/connect-env! "datomic:mem://adi-s3-mysql" sch true))

(comment (def a (j/query db (sql/select * :employees)))

         (def recs (mapv (fn [x] {:employees x}) a))

         (def table-names (keys (:tables jdsch)))

         (doseq [t table-names]
           (let [recs (mapv (fn [x] {t x}) (j/query db (sql/select * t)))]
             (adi/insert! env recs))))


(comment (adi/insert! env recs)

         (adi/select env {:employees/first_name "Maris"} :first)
         {:employees {:hire_date #inst "1990-04-30T14:00:00.000-00:00",
                      :emp_no 74567,
                      :last_name "Conti",
                      :first_name "Maris",
                      :gender "F",
                      :birth_date #inst "1963-06-21T14:00:00.000-00:00"}})







(comment
  :fk_film_language_original
  #lobos.schema.ForeignKeyConstraint{:cname :fk_film_language_original,
                                     :columns [:original_language_id],
                                     :parent-table :language,
                                     :parent-columns [:language_id], :match nil, :triggered-actions {}}

  (add-unique-cols jdsch (adi-schema jdsch))

  (-> assoc-in

      ())


  (keys sch) (:sname :tables :indexes :options)

  (:options sch)
  {:db-spec {:subprotocol "mysql", :classname "com.mysql.jdbc.Driver", :subname "//localhost:3306/sakila", :user "root", :password "root"}}

  (:sname sch) "sakila"

  (:indexes sch)

  (keys (:tables sch))
  => [:actor :address :category :city :country :customer :film :film_actor
      :film_category :film_text :inventory :language :payment :rental :staff :store]

  (-> sch :tables :actor)
  #lobos.schema.Table{:name :actor,
                      :columns
                      {:actor_id #lobos.schema.Column{:cname :actor_id, :data-type #lobos.schema.DataType{:name :smallint-unsigned, :args [], :options {}},
                                                      :default nil, :auto-inc true, :not-null true, :others []},
                       :first_name #lobos.schema.Column{:cname :first_name, :data-type #lobos.schema.DataType{:name :varchar, :args [45], :options {}},
                                                        :default nil, :auto-inc nil, :not-null true, :others []},
                       :last_name #lobos.schema.Column{:cname :last_name, :data-type #lobos.schema.DataType{:name :varchar, :args [45], :options {}},
                                                       :default nil, :auto-inc nil, :not-null true, :others []},
                       :last_update #lobos.schema.Column{:cname :last_update, :data-type #lobos.schema.DataType{:name :timestamp, :args [], :options {}},
                                                         :default #lobos.schema.Expression{:value "CURRENT_TIMESTAMP"},
                                                         :auto-inc nil, :not-null true, :others []}},
                      :constraints {:actor_primary_key_actor_id #lobos.schema.UniqueConstraint{:cname :actor_primary_key_actor_id,
                                                                                               :ctype :primary-key, :columns [:actor_id]}},
                      :indexes {:PRIMARY #lobos.schema.Index{:iname :PRIMARY, :tname :actor, :columns [:actor_id], :options (:unique)},
                                :idx_actor_last_name #lobos.schema.Index{:iname :idx_actor_last_name, :tname :actor, :columns [:last_name], :options nil}}}


  {:timestamp :instant
   :smallint-unsigned :long
   :mediumint-unsigned :long
   }
  (-> sch :tables :film)
  #lobos.schema.Table{:name :film,
                      :columns
                      {:last_update #lobos.schema.Column{:cname :last_update, :data-type #lobos.schema.DataType{:name :timestamp, :args [], :options {}},
                                                         :default #lobos.schema.Expression{:value "CURRENT_TIMESTAMP"},
                                                         :auto-inc nil, :not-null true, :others []},
                       :film_id #lobos.schema.Column{:cname :film_id, :data-type #lobos.schema.DataType{:name :smallint-unsigned, :args [], :options {}},
                                                     :default nil, :auto-inc true, :not-null true, :others []},
                       :language_id #lobos.schema.Column{:cname :language_id,
                                                         :data-type #lobos.schema.DataType{:name :tinyint-unsigned, :args [], :options {}},
                                                         :default nil, :auto-inc nil, :not-null true, :others []}
                       :length #lobos.schema.Column{:cname :length, :data-type #lobos.schema.DataType{:name :smallint-unsigned, :args [], :options {}},
                                                    :default nil, :auto-inc nil, :not-null nil, :others []},
                       :replacement_cost #lobos.schema.Column{:cname :replacement_cost, :data-type #lobos.schema.DataType{:name :decimal, :args [5 2], :options {}}, :default #lobos.schema.Expression{:value "19.99"}, :auto-inc nil, :not-null true, :others []},
                       :title #lobos.schema.Column{:cname :title, :data-type #lobos.schema.DataType{:name :varchar, :args [255], :options {}},
                                                   :default nil, :auto-inc nil, :not-null true, :others []},
                       :special_features #lobos.schema.Column{:cname :special_features,
                                                              :data-type #lobos.schema.DataType{:name :set, :args [], :options {}},
                                                              :default nil, :auto-inc nil, :not-null nil, :others []},
                       :rating #lobos.schema.Column{:cname :rating, :data-type #lobos.schema.DataType{:name :enum, :args [], :options {}},
                                                    :default #lobos.schema.Expression{:value "G"}, :auto-inc nil, :not-null nil, :others []},

                       :original_language_id #lobos.schema.Column{:cname :original_language_id,
                                                                  :data-type #lobos.schema.DataType{:name :tinyint-unsigned, :args [], :options {}},
                                                                  :default nil, :auto-inc nil, :not-null nil, :others []},
                       :release_year #lobos.schema.Column{:cname :release_year, :data-type #lobos.schema.DataType{:name :year, :args [], :options {}},
                                                          :default nil, :auto-inc nil, :not-null nil, :others []},
                       :rental_duration #lobos.schema.Column{:cname :rental_duration,
                                                             :data-type #lobos.schema.DataType{:name :tinyint-unsigned, :args [], :options {}},
                                                             :default #lobos.schema.Expression{:value 3}, :auto-inc nil, :not-null true, :others []},
                       :rental_rate #lobos.schema.Column{:cname :rental_rate, :data-type #lobos.schema.DataType{:name :decimal, :args [4 2], :options {}},
                                                         :default #lobos.schema.Expression{:value "4.99"}, :auto-inc nil, :not-null true, :others []},
                       :description #lobos.schema.Column{:cname :description, :data-type #lobos.schema.DataType{:name :clob, :args [65535], :options {}},
                                                         :default nil, :auto-inc nil, :not-null nil, :others []}},
                      :constraints {:film_primary_key_film_id #lobos.schema.UniqueConstraint{:cname :film_primary_key_film_id,
                                                                                             :ctype :primary-key, :columns [:film_id]},
                                    :fk_film_language #lobos.schema.ForeignKeyConstraint{:cname :fk_film_language, :columns [:language_id],
                                                                                         :parent-table :language, :parent-columns [:language_id],
                                                                                         :match nil, :triggered-actions {}},
                                    :fk_film_language_original #lobos.schema.ForeignKeyConstraint{:cname :fk_film_language_original,
                                                                                                  :columns [:original_language_id],
                                                                                                  :parent-table :language,
                                                                                                  :parent-columns [:language_id], :match nil, :triggered-actions {}}},

                      :indexes {:PRIMARY #lobos.schema.Index{:iname :PRIMARY, :tname :film, :columns [:film_id], :options (:unique)},
                                :idx_title #lobos.schema.Index{:iname :idx_title, :tname :film, :columns [:title], :options nil},
                                :idx_fk_language_id #lobos.schema.Index{:iname :idx_fk_language_id, :tname :film, :columns [:language_id], :options nil},
                                :idx_fk_original_language_id #lobos.schema.Index{:iname :idx_fk_original_language_id, :tname :film, :columns [:original_language_id], :options nil}}}

  (-> sch :tables :inventory)
  #lobos.schema.Table{:name :inventory,
                      :columns {:inventory_id #lobos.schema.Column{:cname :inventory_id,
                                                                   :data-type #lobos.schema.DataType{:name :mediumint-unsigned, :args [], :options {}},
                                                                   :default nil, :auto-inc true, :not-null true, :others []},
                                :film_id #lobos.schema.Column{:cname :film_id,
                                                              :data-type #lobos.schema.DataType{:name :smallint-unsigned, :args [], :options {}},
                                                              :default nil, :auto-inc nil, :not-null true, :others []},
                                :store_id #lobos.schema.Column{:cname :store_id,
                                                               :data-type #lobos.schema.DataType{:name :tinyint-unsigned, :args [], :options {}},
                                                               :default nil, :auto-inc nil, :not-null true, :others []},
                                :last_update #lobos.schema.Column{:cname :last_update,
                                                                  :data-type #lobos.schema.DataType{:name :timestamp, :args [], :options {}},
                                                                  :default #lobos.schema.Expression{:value "CURRENT_TIMESTAMP"}, :auto-inc nil, :not-null true, :others []}}

                      :constraints {:inventory_primary_key_inventory_id #lobos.schema.UniqueConstraint{:cname :inventory_primary_key_inventory_id, :ctype :primary-key, :columns [:inventory_id]},
                                    :fk_inventory_store #lobos.schema.ForeignKeyConstraint{:cname :fk_inventory_store, :columns [:store_id],
                                                                                           :parent-table :store, :parent-columns [:store_id], :match nil, :triggered-actions {}},
                                    :fk_inventory_film #lobos.schema.ForeignKeyConstraint{:cname :fk_inventory_film, :columns [:film_id],
                                                                                          :parent-table :film, :parent-columns [:film_id], :match nil, :triggered-actions {}}},

                      :indexes {:PRIMARY #lobos.schema.Index{:iname :PRIMARY, :tname :inventory, :columns [:inventory_id], :options (:unique)},
                                :idx_fk_film_id #lobos.schema.Index{:iname :idx_fk_film_id, :tname :inventory, :columns [:film_id], :options nil},
                                :idx_store_id_film_id #lobos.schema.Index{:iname :idx_store_id_film_id, :tname :inventory, :columns [:store_id :film_id], :options nil}}}
)
