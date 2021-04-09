(ns dataflow.impl.core
  (:require [clojure.string :as string]
            [clojure.core.async :as async]))

(defprotocol IWorker
  (index [worker] "Returns the worker's index")
  (input-ch [worker])
  (notify-worker [worker partition-key id value t])
  (recv-ch [worker id to])
  (close [worker]))

(defrecord InprocWorker [idx input state]
  IWorker
  (index [_] idx)
  (input-ch [_] input)
  (notify-worker [_ partition-key id value t]
    (let [i   (mod (hash partition-key) (:count state))
          msg {:from  idx
               :to    i
               :topic [i id]
               :value value
               :time  t}]
      ;; (println "send" (pr-str msg))
      (async/put! (:comm state) msg)))
  (recv-ch [_ id to]
    (let [tmp (async/chan 1 (map (juxt :time :value)))]
      (async/pipe tmp to)
    ;; (println "recv" (pr-str [idx id]))
      (-> state :pub (async/sub [idx id] tmp))))
  (close [worker]
    (-> input async/close!)
    (-> state :comm async/close!)
    worker))

(defn timestamp-ch
  "Wraps in channel with a new channel that automatically annotates epoch time to inputs."
  ([]
   (async/chan 1 (map-indexed (fn [i value] [[i] value]))))
  ([in]
   (let [out (async/chan 1 (map-indexed (fn [i value] [[i] value])))]
     (async/pipe in out)
     out)))

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

(defn- inproc-workers [n]
  (let [comm (async/chan 32)
        state {:comm  comm
               :pub   (async/pub comm :topic)
               :count n}]
    (mapv
     #(->InprocWorker % (async/chan) state)
     (range n))))

(defn- tap-ch [ch f]
  (let [out (async/chan)]
    (async/go-loop []
      (when-let [item (async/<! ch)]
        (f item)
        (async/>! out item)
        (recur)))
    out))

(defn run [{:keys [type n]
            :or   {type :threads
                   n    1}}
           make-dataflow]
  (let [n       (if (= n :cpus)
                  (.availableProcessors (Runtime/getRuntime))
                  n)
        workers (vec
                 (for [worker (inproc-workers n)]
                   (let [w-input (input-ch worker)]
                     (consume (make-dataflow worker w-input))
                     worker)))]
    {:close (fn closer []
              (doseq [w workers]
                (close w)))
     :workers workers}))

(defn input-variable 
  ([worker]
   (let [ch (timestamp-ch)]
     (async/pipe ch (input-ch worker))
     ch))
  ([worker name w-index]
   (let [ch (timestamp-ch)]
     (if (= w-index (index worker))
       (async/pipe ch (input-ch worker))
       (async/go-loop []
                      (when-let [[t value] (async/<! ch)]
                        (notify-worker worker value name value t))))
     ch)))

(defn trace [f]
  (map (fn [item] (f item) item)))

(defn using-lock [obj f]
  (fn [& args]
    (locking obj
      (apply f args))))

(defn capture [join]
  (let [prev (volatile! nil)]
    [(fn capture [rf]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result item]
          (vreset! prev item)
          (rf result item))))
     (fn restore [rf]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result item]
          (rf result (join @prev item)))))]))

(defn pipeline-progress!
  [input xf]
  (let [out (async/chan 1 xf)]
    (async/pipe input out)
    out))

(defn pipeline!
  [input xf]
  (let [[start end] (capture (fn [[t _] new] [t new]))
        out (async/chan 1 (comp start (map second) xf end))]
    (async/pipe input out)
    out))

(defn segmented-pipeline!
  [input item-f collection-f]
  (let [out (async/chan)]
    (async/go
      (let [v (volatile! nil)
            a (java.util.ArrayList.)]
        (loop []
          (let [[t item :as msg] (async/<! input)]
            (if (or (nil? msg) (not= t @v))
              (let [old-t @v]
                (when-not (.isEmpty a)
                  (async/into (mapv (fn [v] [old-t v]) (collection-f (vec a))) out)
                  (.clear a))
                (vreset! v t)
                (when msg
                  (.add a (item-f item))
                  (recur)))
              (do (.add a (item-f item))
                  (recur))))))
    out)))

(defn exchange!
  ([input worker step-id] (exchange! input worker step-id identity))
  ([input worker step-id key-fn]
   (let [out (async/chan)]
     (async/go-loop []
       (when-let [[t msg] (async/<! input)]
         (notify-worker worker (key-fn msg) step-id msg t)
         (recur)))
     (recv-ch worker step-id out)
     out)))

(defn iterate! [input predicate step]
  (let [iterator (async/chan)
        out      (async/chan)]
    (async/go-loop []
      (if-let [[t msg] (async/<! input)]
        (do
          (async/>! iterator [(conj t 0) msg])
          (recur))
        (async/close! iterator)))
    (async/go-loop []
      (if-let [[t value :as msg] (async/<! iterator)]
        (if (predicate msg)
          (async/>! out [(subvec t (dec (count t))) value])
          (do
            (async/pipe (step ))
            (async/>! iterate [(update t (dec (count t)) inc) (step msg)])
            (recur)))
        (async/close! out)))
    out))

(defn concat! [input loop-var]
  (let [input-w-loop-t (async/chan 1 (map (fn [[t v]] [(conj t 0) v])))]
    (async/pipe input input-w-loop-t)
    (async/merge [input-w-loop-t loop-var])))

(defn loop-variable
  ([] (loop-variable 1))
  ([amt]
  ;; TODO: we never close loop-variables... what's the best way to do this?
   (async/chan 1 (map (fn [[t v]] [(update t (dec (count t)) + amt) v])))))

(defn loop-when! [input predicate loop-stream]
  (let [out (async/chan)]
    (async/go-loop []
      (if-let [item (async/<! input)]
        (let [[t value] item]
          (do
            (if (predicate (t (dec (count t))) value)
              (async/put! loop-stream item)
              (async/>! out item))
            (recur)))
        (async/close! out)))
    out))

(defn probe!
  [input output]
  (let [m (async/mult input)
        out (async/chan)]
    (async/tap m output)
    (async/tap m out)
    out))

(defn wait-for [ch expected-t]
  (let [p (promise)]
    (async/go-loop []
      (if-let [[t _ :as item] (async/<! ch)]
        (if (= t expected-t)
          (deliver p item)
          (recur))
        (deliver p nil)))
    p))

#_(into [] (exchange identity 4) (range 1))

(comment

  (do
    (def sig (async/chan (async/sliding-buffer 1)))
    (def workers
      (run
       {:n :cpus}
       (fn [w input]
         (-> input
             (pipeline! (trace (using-lock *out* prn)))
             (pipeline! (mapcat #(string/split % #" ")))
             #_(exchange! w :words)
             (pipeline! (trace (using-lock *out* prn)))
             (probe! sig)
             #_(pipeline-progress! (trace (using-lock *out* prn)))
             #_(segmented-pipeline! (fn [item] (locking *out* (println "item" item)) item)
                                    (fn [coll] (locking *out* (println "coll" coll)) coll))))))
    (def input (input-variable (first (:workers workers)))))


  (async/put! input "hello world foo bar quick brown fox jumped over the lazy dog")
  (def p (wait-for sig 1))
  (deref p 10000 :default)
  (async/close! input)
  ((:close workers))

  (do
    (def workers
      (run
       {:n 1}
       (fn [w input]
         (let [v (loop-variable)]
           (-> input
               (concat! v)
               (pipeline! (comp (map #(if (even? %) (/ % 2) (+ (* 3 %) 1)))
                                (remove #{1})))
               (loop-when! (fn [t _] (< t 10)) v)
               (pipeline-progress! (trace (using-lock *out* (partial prn "DONE")))))))))
    (def input (input-variable (first (:workers workers)))))

  (async/onto-chan input (range 10))
  (do
    (async/close! input)
    ((:close workers)))

  ;; TODO:
  ;;   - How do we support the time-lattice with this interface?
  ;;   - Can we make this distributed?
  ;;   - How do we compute the minimum amout of changes needed to propagate?
  ;;   - How do we join two dispirate pieces of data?
  )