(ns user
  (:require [clojure.tools.namespace.repl :refer [refresh refresh-all]]
            [dataflow.api :as api]
            [dataflow.impl.core :as impl]
            [clojure.core.async :as async]
            [clojure.string :as string]))

(comment
  (do ;; threaded cluster - technically more thread than workers since core-async is backed by a threadpool
    (def sig (async/chan (async/sliding-buffer 1)))
    (def printer (impl/print-ch (fn [& args] (locking *out* (apply prn args)))))
    (def workers
      (api/run
       {:n    2
        :type :threads}
       (fn [w input]
         (-> input
             #_(pipeline! (trace (partial async/put! printer)))
             (api/pipeline! (mapcat #(string/split % #" ")))
             (api/exchange! :words)
             (api/count! :count-words)
             (api/reduce! :count-merge (partial merge-with (fnil + 0 0)) {})
             #_(pipeline! (trace (partial async/put! printer)))
             (api/pipeline-progress! (impl/trace (partial async/put! printer)))
             (api/probe! sig)
             #_(pipeline-progress! (trace (using-lock *out* prn)))
             #_(segmented-pipeline! (fn [item] (locking *out* (println "item" item)) item)
                                    (fn [coll] (locking *out* (println "coll" coll)) coll))))))
    (def input (api/input-variable :input (first (:workers workers)))))

  (api/send-and-advance! input "foo bar")

  (do ;; using atomix to cluster by processes
    (def sig (async/chan (async/sliding-buffer 1)))
    (def printer (api/print-ch (using-lock *out* prn)))
    (def workers
      (run-cluster
       {:nodes [["node1" nil 4678]
                ["node2" nil 4679]]
        ;; normally just a single index pointing to one of the nodes
        ;; but it's easier to test with a vector of indicies
        :index [0 1]}
       (fn [w input]
         (-> input
             #_(pipeline! (trace (partial async/put! printer)))
             (pipeline! (mapcat #(string/split % #" ")))
             #_(pipeline-progress! (trace (partial async/put! printer)))
             #_(exchange! :words)
             (count! :count-words)
             (reduce! :count-merge (partial merge-with (fnil + 0 0)) {})
             (pipeline! (trace (partial async/put! printer)))
             #_(pipeline-progress! (trace (partial async/put! printer)))
             (probe! sig)
             #_(pipeline-progress! (trace (using-lock *out* prn)))
             #_(segmented-pipeline! (fn [item] (locking *out* (println "item" item)) item)
                                    (fn [coll] (locking *out* (println "coll" coll)) coll))))))
    (def input (input-variable :input (first (:workers workers)))))

  (atomix/members (:agent (first (:workers workers))))
  (.start (:agent (first (:workers workers))))


  (def p (wait-for sig 1))
  (protocols/send! input "hello world foo bar the quick brown fox jumped over the lazy dog")
  (protocols/advance! input)
  (deref p 1000 :default)
  (do
    (async/close! input)
    (async/close! printer)
    ((:close workers)))

  (do
    (def workers
      (run
       {:n 2}
       (fn [w input]
         (let [v (loop-variable w)]
           (-> input
               (concat! v)
               (pipeline! (comp (map #(if (even? %) (/ % 2) (+ (* 3 %) 1)))
                                (remove #{1})))
               (loop-when! (fn [t _] (< t 10)) v)
               (pipeline-progress! (trace (using-lock *out* (partial prn "DONE")))))))))
    (def input (input-variable (first (:workers workers)))))

  (protocols/send-batch! input (range 10))
  (protocols/advance! input)
  (do
    (async/close! input)
    ((:close workers)))
  )