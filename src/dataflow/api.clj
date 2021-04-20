(ns dataflow.api
  (:require [dataflow.impl.core :as impl]
            [dataflow.impl.protocols :as protocols]))

(def input-variable impl/input-variable)

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
     - [[reduce!]]
     - [[count!]]
   "
  [{:as   opts
    :keys [type
           n
           nodes index]}
   make-dataflow]
  (case type
    :threads (impl/run-threads opts make-dataflow)
    :cluster (impl/run-cluster (merge opts {:nodes nodes :index index})
                               make-dataflow)))

(def ^{:doc      "pipeline! processes the input channel using the transducer xf.
                  Results of xf are produced in the returned channel as values. xf receives the
                  input value only.

                  See [[pipeline-progress!]] to access internal time values.
                  See [[segmented-pipeline!]] to have two-phased processing of input channel"
       :arglists '([input xf])}
  pipeline! impl/pipeline!)
(def ^{:doc      "pipeline-progress! processes the input channel using the
                  transducer xf. xf is expected to produce [t value] events for the input
                  channel."
       :arglists '([input xf])}
  pipeline-progress! impl/pipeline-progress!)
(def segmented-pipeline! impl/segmented-pipeline!)
(def ^{:doc "exchange! repartitions messages in the input channel to allow workers to process messages.
             Messages with the same key will be processed by the same worker.

             step-id represents a unique identifier for this step for workers
             to coordinate this step in the dataflow process.

             key-fn is a custom function that receives the message value to
             partition. Identical results of these functions will move the
             message to the same worker. Default is [[identity]]."
       :arglists '([input worker step-id]
                   [input worker step-id key-fn])}
  exchange! impl/exchange!)
(def ^{:doc "probe! taps the input channel and produces times on the given output-t channel.
             Returns the a channel with all the same values as the input channel

             See [[wait-for]]."
       :arglists '([input output-t])}
  probe! impl/probe!)
(def join! impl/join!)
(def ^{:doc "concat! merges into an input stream"}
  concat! impl/concat!)
(def count! impl/count!)
(def reduce! impl/reduce!)
(def loop-variable impl/loop-variable)
(def loop-when! impl/loop-when!)

(def wait-for impl/wait-for)

(def advance! protocols/send!)
(def send! protocols/send!)
(def send-and-advance! protocols/send-and-advance!)
(def send-batch! protocols/send-batch!)