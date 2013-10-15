(ns vraja.http-kit
  (:require [org.httpkit.server :as http]
            [clojure.core.async :refer [chan <! >! put! close! go]]
            [cheshire.core :as json]
            [clojure.tools.reader.edn :as edn]
            [clojure.core.async.impl.protocols :as p]))

(defn- make-read-ch [ws read-fn]
  (let [ch (chan)]
    (http/on-receive ws 
      #(put! ch (read-fn %)))
    ch))

(defn- make-write-ch [ws write-fn]
  (let [ch (chan)]
    (go
     (loop []
       (let [msg (<! ch)]
         (when msg
           (http/send! ws (write-fn msg))
           (recur)))))
    ch))

(defn- on-close [ws read-ch write-ch]
  (http/on-close ws
                 (fn [_]
                   (close! read-ch)
                   (close! write-ch))))

(defn- combine-chs [ws read-ch write-ch]
  (reify
    p/ReadPort
    (take! [_ handler]
      (p/take! read-ch handler))

    p/WritePort
    (put! [_ msg handler]
      (p/put! write-ch msg handler))

    p/Channel
    (close! [_]
      (p/close! read-ch)
      (p/close! write-ch)
      (http/close ws))))

(defn make-ch [httpkit-ch read-fn write-fn]
  (let [read-ch (make-read-ch httpkit-ch read-fn)
        write-ch (make-write-ch httpkit-ch write-fn)
        combined-ch (combine-chs httpkit-ch read-ch write-ch)]     
    (on-close httpkit-ch read-ch write-ch)
    combined-ch))

(defmacro with-js-channel [req ch-name & body]
  `(http/with-channel ~req httpkit-ch#
     (let [~ch-name (make-ch httpkit-ch# json/parse-string json/generate-string)]
       ~@body)))

(defmacro with-clj-channel [req ch-name & body]
 `(http/with-channel ~req httpkit-ch#
    (let [~ch-name (make-ch httpkit-ch# edn/read-string prn-str)]
      ~@body)))
