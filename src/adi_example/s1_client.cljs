(ns adi-example.s1-client
  (:require [purnam.cljs :refer [aset-in aget-in]]
            [vraja.cljs :refer [js-ch]]
            [cljs.core.async :refer [<! >! put! close! timeout]]
            [dommy.core :as d])
  (:require-macros [cljs.core.async.macros :refer [go]]
                   [purnam.js :refer [! obj arr def.n]]
                   [purnam.angular :refer [def.module def.config def.factory
                                           def.filter def.controller
                                           def.service def.directive]]))

(def.module adi [json.tree])

(def.n bind-msgs [ch scope]
  (go
   (loop []
     (when-let [msg (<! ch)]
       (js/console.log msg)
       (! scope.output.results msg)
       (scope.$apply)
       (js/console.log scope.output.results)
       (recur)))))
       
(def.controller adi.MainCtrl [$scope]
  (! $scope.input (obj :lang "clj"))
  (! $scope.output.results.temp "hello")

  (go
   (let [msgs (atom [])
         ws (<! (js-ch "ws://localhost:8088/ws"))]
     ;;(js/console.log ws)
     (bind-msgs ws $scope)
     (! $scope.setupOp 
       (fn [op resource]
         (put! ws (obj :op-type "setup" 
                       :op op
                       :resource resource
                       :arg $scope.input.setupArg))))
                       
     (! $scope.standardOp 
       (fn [op nargs]
         (js/console.log "HELLO" op nargs)
         (put! ws (obj :op-type "standard" 
                       :op op
                       :lang $scope.input.lang
                       :nargs nargs
                       :ids $scope.input.ids
                       :args [$scope.input.arg1 $scope.input.arg2 $scope.input.arg3]))))
     (>! ws (obj :op-type "setup" 
                 :op "return"
                 :resource "schema"
                 :arg $scope.input.setupArg)))))
