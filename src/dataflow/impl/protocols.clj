(ns dataflow.impl.protocols
  (:require [clojure.core.async :as async]))

(defprotocol IWorker
  (index [worker] "Returns the worker's index")
  (input-ch [worker])
  (notify-worker [worker partition-key id msg])
  (subscribe [worker id to])
  (register-cleanup [worker f])
  (close [worker]))

(defprotocol IInput
  (send-batch! [input values])
  (advance! [input])
  (finish [input]))

(defprotocol IEdge
  (get-worker* [e]))

(defn send! [input value]
  (send-batch! input [value]))

(defn send-and-advance! [input value]
  (send-batch! input [value])
  (advance! input))

(defn get-worker [edge-or-worker]
  (if (satisfies? IWorker edge-or-worker)
    edge-or-worker
    (get-worker* edge-or-worker)))

(defn consume
  "Spawns a go-loop to consume all items from a channel (dropping them by default). Consume is presumable for side effects"
  ([ch f]
   (when ch
     (async/go-loop []
       (when-let [item (async/<! ch)]
         (f item)
         (recur)))))
  ([ch]
   (when ch
     (async/go-loop []
       (when (async/<! ch)
         (recur))))))
