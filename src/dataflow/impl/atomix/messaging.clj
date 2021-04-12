(ns dataflow.impl.atomix.messaging
  (:require [dataflow.impl.atomix.utils :as utils :refer [->member-id *default-serializer*]])
  (:import [io.atomix.core Atomix]
           io.atomix.utils.serializer.Serializer
           java.util.concurrent.CompletableFuture
           java.util.concurrent.Executors))

(set! *warn-on-reflection* true)

(defn send!
  "Send either a message to cluster member(s) or as a broadcast.

   Target can be a single member id or a set of member ids

   If you want to broadcast to all members without needing to lookup all
   members, use [[broadcast!]] instead.
  "
  ^CompletableFuture [^Atomix agent subject message target]
  (let [s *default-serializer*]
    (if (sequential? target)
      (.multicast (.getCommunicationService agent) subject message
                  (utils/func #(.encode s %))
                  (set (mapv ->member-id target)))
      (.send (.getCommunicationService agent) subject message
             (utils/func #(.encode s %))
             (utils/func #(.decode s %))
             (->member-id target)))))

(defn broadcast!
  "Broadcasts a point-to-point message to all members"
  [^Atomix agent subject msg {:keys [include-self? serializer]
                              :or   {include-self? false
                                     serializer    *default-serializer*}}]
  (let [s ^Serializer serializer]
    (if include-self?
      (.broadcastIncludeSelf (.getCommunicationService agent)
                             (str subject)
                             msg
                             (utils/func #(.encode s %)))
      (.broadcast (.getCommunicationService agent)
                  (str subject)
                  msg
                  (utils/func #(.encode s %))))))

(def ^:private default-executor
  (memoize #(Executors/newSingleThreadExecutor)))

(defn subscribe-handler-completable-future!
  ^CompletableFuture
  [^Atomix agent subject handler]
  (let [subject (str subject)
        s       *default-serializer*]
    (.subscribe (.getCommunicationService agent)
                subject
                (utils/func #(.decode s %))
                (utils/func handler)
                (utils/func #(.encode s %)))))

(defn subscribe-handler!
  (^CompletableFuture [^Atomix agent subject handler] (subscribe-handler! agent subject handler (default-executor)))
  (^CompletableFuture [^Atomix agent subject handler ^java.util.concurrent.Executor executor]
  (let [s *default-serializer*]
    (.subscribe (.getCommunicationService agent)
                (str subject)
                (utils/func #(.decode s %))
                (utils/func handler)
                (utils/func #(.encode s %))
                executor))))

(defn subscribe-consumer!
  (^CompletableFuture [^Atomix agent subject handler] (subscribe-consumer! agent subject handler (default-executor)))
  (^CompletableFuture [^Atomix agent subject handler ^java.util.concurrent.Executor executor]
   (let [s *default-serializer*]
     (.subscribe (.getCommunicationService agent)
                 (str subject)
                 (utils/func #(.decode s %))
                 (utils/consumer handler)
                 executor))))

(defn unsubscribe! [^Atomix agent subject]
  (.unsubscribe (.getCommunicationService agent) subject))
