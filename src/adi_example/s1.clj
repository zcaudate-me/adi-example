(ns adi-example.s1
  (:require [ring.util.response :refer [response]]
            [ring.middleware.session :refer [wrap-session]]
            [compojure.core :refer [defroutes GET]]
            [compojure.route :refer [resources]]
            [ribol.core :refer [manage on]]
            [cheshire.core :as json]
            [adi.core :refer :all :as adi]
            [org.httpkit.server :refer [run-server]]
            [vraja.http-kit :refer [with-clj-channel with-js-channel]]
            [clojure.core.async :refer [<! >! put! close! go]]))

(def DEFAULT_SCHEMA
  {:blog {:name [{}]
          :id    [{:type :long}]
          :owner [{:type :ref
                   :ref {:ns :user
                         :rval :owned_blogs}}]
          :authors [{:type :ref
                     :cardinality :many
                     :ref {:ns :user
                           :rval :author_of}}]}
   :post {:blog [{:type :ref
                  :ref {:ns :blog}}]
          :author [{:type :ref
                    :ref {:ns :user}}]
          :tags [{:cardinality :many}]
          :text [{}]
          :title [{}]}
   :comment {:post [{:type :ref
                     :ref {:ns :post}}]
             :text [{}]
             :user [{:type :ref
                     :ref {:ns :user}}]}
   :user {:name [{}]}})

(def DATA
  {:user {:name "zcaudate"
          :owned_blogs #{{:name "Something Same"
                       :id 3
                       :authors [{:+/db/id (adi/iid :Adam)
                                  :name "Adam"} {:name "Bill"}]
                       :posts [{:title "How To Flex Data" :tags #{"computing"}}
                               {:title "Cats and Dogs" :tags #{"pets"}
                                :comments {:text "This is a great article"
                                           :user (adi/iid :Adam)}}
                               {:title "How To Flex Data"}]}}}})



(def URI "datomic:mem://adi-example-s1")
(def ENV (connect-env! URI DEFAULT_SCHEMA true))
(insert! ENV DATA)

(select ENV {:blog/owner '_})
(def STATE
  {:uri URI
   :env (atom ENV)
   :schema (atom (-> ENV :schema :tree))
   :model (atom nil)
   :access (atom nil)
   :return (atom nil)})

(def METHODS
  {:insert! insert!
   :delete! delete!
   :update! update!
   :retract! retract!
   :select select
   :update-in! update-in!
   :delete-in! delete-in!
   :retract-in! retract-in!})

(comment
  {:op-type "setup"
   :resource #{"schema" "model" "access"}
   :op #{"set" "clear" "return"}
   }
  (reset! (:schema STATE) {:account {:user [{}]}})

  )

(defn parse-clj-arg [s]
  (try (read-string s)
       (catch Throwable t
         #_(.printStackTrace t))))

(defn parse-js-arg [s]
  (try (json/parse-string s)
       (catch Throwable t
         #_(.printStackTrace t))))

(defn parse-arg [msg s]
  (condp = (get msg "lang")
    "clj"  (parse-clj-arg s)
    "js"   (parse-js-arg s)))

(defn handle-setup-request [msg]
  (let [op (get msg "op")
        resource (get msg "resource")
        res (STATE (keyword resource))
        arg (parse-clj-arg (get msg "arg"))]
    (cond (and (= op "set") (= resource "schema"))
          (let [env (connect-env! URI arg true)]
            (reset! (:env STATE) env)
            (reset! (:schema STATE) (-> env :schema :tree))
            @(:schema STATE))

          (= op "set")
          (do (reset! res arg)
              @res)

          (= op "view")
          @res

          (= op "clear")
          (do (reset! res nil)
              @res))))

(defn handle-standard-request [msg]
  #_(println "STANDARD REQUEST:" (get msg "op") (get msg "args"))
  (let [op    (get msg "op")
        nargs (get msg "nargs")
        args  (->> (get msg "args")
                   (map #(parse-arg msg %))
                   (take nargs))

        f     (METHODS (keyword op))
        res (apply f @(:env STATE)
                   (concat args [:transact :full]
                           (if (get msg "ids")
                             [:ids])
                           (if-let [ret @(:return STATE)]
                             [:return ret])
                           (if-let [acc @(:access STATE)]
                             [:access acc])
                           (if-let [mod @(:model STATE)]
                             [:model mod])))]
    (println "STANDARD REQUEST:" res)
    (if (not= op "insert!")
      res
      {:message "DONE"})))

(defn ws-handler [req]
  (let [req (assoc req :ws-session (atom {}))]
    (with-js-channel req ws
      (go
       (loop []
         (when-let [msg (<! ws)]
           (let [op-type (get msg "op-type")
                 output
                 (try
                   (manage
                    (cond
                     (= op-type "test") {:stuff (get msg "return")}
                     (= op-type "setup") (handle-setup-request msg)
                     (= op-type "standard") (handle-standard-request msg))
                    (on :data-not-in-schema [nsv]
                        {:error (str nsv " is not in the schema")}))
                   (catch Throwable t
                     (.printStackTrace t)))]
             (if output
               (>! ws output)
               (>! ws {}))
             (recur))))))))

(defroutes app
  (GET "/ws" [] (wrap-session ws-handler))
  (resources "/"))

(serv)
(def serv (run-server app {:port 8088}))



(comment

  )
