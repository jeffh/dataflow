(ns dataflow.api
  (:require [dataflow.impl.core :as impl]
            [clojure.core.async :as async]))

(defn run
  "Starts execution of a dataflow computation graph with a given set of options for a worker.
   
   Parameters:
    - type = :threads | :sockets (defaults to :threads)
      Workers are expected to all run in the same process in different threads. Uses a core.async thread pool.
    - n = Integer | :cpus (defaults to 1)
      The number of expected concurrent workers to run. If :cpus is given, defaults to the number of available CPUs.
    - make-dataflow = (fn [worker input-ch] ...) :- nil | async channel
      The function sets up a dataflow computation grpah using the given
      input-ch as a channel that receives input.

      Use helper functions to help attach computation to the input-ch.
      Optionally can return the async channel that run will ensure is
      consumed.

   For building dataflow computation, see:
     - [[pipeline!]]
     - [[pipeline-progress!]]
     - [[segmented-pipeline!]]
     - [[exchange!]]
     - [[join!]]
   "
  [{:as   opts
    :keys [type n]}
   make-dataflow]
  (impl/run opts make-dataflow))

(def pipeline! impl/pipeline!)
(def pipeline-progress! impl/pipeline-progress!)
(def segmented-pipeline! impl/segmented-pipeline!)
(def exchange! impl/exchange!)
(def probe! impl/probe!)
(def join! impl/join!)

(def put! async/put!)
(def close! async/close!)

#_
(def graph
  (-> (->Graph nil [:input] [:output])
      (add-node (->InputAtom :input input (atom starting-time) :mapper))
      (add-node (->Map :mapper inc :accum))
      (add-node (->reduce :reducer + 0 :inspector))
      (add-node (->Accumulater :accum identity :inspector (atom nil)))
      (add-node (->Trace :tracer println :output))
      (add-node (->Inspect :inspector println :output))
      (add-node (->OutputAtom :output output))))

(def worker (basic-controller graph))
#_
(comment
  (edit/diff [1 2 3] [2 3 4])
  (start graph worker)
  (stop graph worker)
  (do (swap! input inc) @output)
  (do (swap! (-> graph :nodes :input :ref) inc) @output)
  (advance-time (-> graph :nodes :a) worker)
  (reset! input @input))
