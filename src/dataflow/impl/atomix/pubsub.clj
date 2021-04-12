(ns dataflow.impl.atomix.pubsub
  (:require [dataflow.impl.atomix.utils :as utils :refer [->member-id *default-serializer*]])
  (:import [io.atomix.core Atomix]
           io.atomix.cluster.messaging.Subscription
           java.time.Duration
           java.util.concurrent.CompletableFuture
           java.util.concurrent.Executors))

(set! *warn-on-reflection* true)

(defn ^String subscription-topic [^Subscription s] (.topic s))
(defn ^CompletableFuture subscription-close [^Subscription s] (.close s))

(defn subscriptions [^Atomix agent topic]
  (.getSubscriptions (.getEventService agent) (str topic)))

(defn send!
  "Send message on topic for one subscribe to receive.

   If you want all subscribers to see this message, use [[broadcast!]] instead.
  "
  (^CompletableFuture [^Atomix agent subject message ^Duration timeout]
   (let [s *default-serializer*]
     (.send (.getEventService agent) subject message
            (utils/func #(.encode s %))
            (utils/func #(.decode s %))
            timeout)))
  (^CompletableFuture [^Atomix agent subject message]
   (let [s *default-serializer*]
     (.send (.getEventService agent) subject message
            (utils/func #(.encode s %))
            (utils/func #(.decode s %))))))

(defn broadcast!
  "Broadcasts a point-to-point message to all members"
  [^Atomix agent subject msg]
  (let [s *default-serializer*]
    (.broadcast (.getEventService agent)
                (str subject)
                msg
                (utils/func #(.encode s %)))))

(def ^:private default-executor
  (memoize #(Executors/newSingleThreadExecutor)))

(defn subscribe-handler-completable-future!
  ^CompletableFuture [^Atomix agent subject handler]
  (let [subject (str subject)
        s *default-serializer*]
    (.subscribe (.getEventService agent)
                subject
                (utils/func #(.decode s %))
                (utils/func handler)
                (utils/func #(.encode s %)))))

(defn subscribe-handler!
  (^CompletableFuture [^Atomix agent subject handler] (subscribe-handler! agent subject handler (default-executor)))
  (^CompletableFuture [^Atomix agent subject handler ^java.util.concurrent.Executor executor]
  (let [s *default-serializer*]
    (.subscribe (.getEventService agent)
                (str subject)
                (utils/func #(.decode s %))
                (utils/func handler)
                (utils/func #(.encode s %))
                executor))))

(defn subscribe-consumer!
  (^CompletableFuture [^Atomix agent subject handler] (subscribe-consumer! agent subject handler (default-executor)))
  (^CompletableFuture [^Atomix agent subject handler ^java.util.concurrent.Executor executor]
   (let [s *default-serializer*]
     (.subscribe (.getEventService agent)
                 (str subject)
                 (utils/func #(.decode s %))
                 (utils/consumer handler)
                 executor))))

(defn unsubscribe! ^CompletableFuture [^Atomix agent subject]
  (let [subject (str subject)]
    (CompletableFuture/allOf
     (into-array CompletableFuture
                 (for [sub (subscriptions agent subject)]
                   (subscription-close sub))))))