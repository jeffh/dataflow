(ns dataflow.impl.core
  (:require [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.core.async.impl.channels :as channels]
            [clojure.core.async.impl.protocols :as impl]
            [dataflow.impl.protocols :as protocols :refer [input-ch]]
            [dataflow.impl.atomix :as atomix]
            [dataflow.impl.atomix.pubsub :as pubsub]
            [dataflow.impl.atomix.messaging :as messaging]))

(deftype InputVariable [ch id worker time]
  protocols/IInput
  (send-batch! [_ values]
    (doseq [v values]
      (async/put! ch [@time v])))
  (advance! [_]
    (protocols/notify-worker worker ::all id [(swap! time inc)]))
  (finish [self] (protocols/advance! self) (async/close! ch))
  protocols/IEdge
  (get-worker* [_] worker)
  channels/MMC
  (cleanup [_] (channels/cleanup ch))
  (abort [_] (channels/abort ch))
  impl/WritePort
  (put! [_ val handler] (impl/put! ch val handler))
  impl/ReadPort
  (take! [_ handler] (impl/take! ch handler))
  impl/Channel
  (closed? [_] (impl/closed? ch))
  (close! [_] (impl/close! ch)))

(deftype Edge [ch worker]
  protocols/IEdge
  (get-worker* [_] worker)
  channels/MMC
  (cleanup [_] (channels/cleanup ch))
  (abort [_] (channels/abort ch))
  impl/WritePort
  (put! [_ val handler] (impl/put! ch val handler))
  impl/ReadPort
  (take! [_ handler] (impl/take! ch handler))
  impl/Channel
  (closed? [_] (impl/closed? ch))
  (close! [_] (impl/close! ch)))

;; Using a custom type to "tag" the channel
(deftype LoopVariable [ch]
  protocols/IEdge
  (get-worker* [_] (protocols/get-worker ch))
  channels/MMC
  (cleanup [_] (channels/cleanup ch))
  (abort [_] (channels/abort ch))
  impl/WritePort
  (put! [_ val handler] (impl/put! ch val handler))
  impl/ReadPort
  (take! [_ handler] (impl/take! ch handler))
  impl/Channel
  (closed? [_] (impl/closed? ch))
  (close! [_] (impl/close! ch)))

(def conj-vec (fnil conj []))

(defn- subscribe* [worker-state idx id to]
  (let [tmp (async/chan 1 (comp (map
                                 :value)
                                (map (fn [v]
                                       #_(locking *out* (println "recv" (pr-str [idx id])))
                                       v))))]
    (async/pipe tmp to)
    (-> worker-state :pub (async/sub [idx id] tmp))))

(defn- close* [cleaners chs]
  (doseq [f @cleaners] (f))
  (doseq [ch chs] (async/close! ch)))

(defrecord InprocWorker [idx input state cleaners]
  protocols/IWorker
  (index [_] idx)
  (input-ch [self] (->Edge input self))
  (notify-worker [_ partition-key id msg]
    (condp = partition-key
      ::all
      (dotimes [i (:count state)]
        (let [msg {:from  idx
                   :topic [i id]
                   :value msg
                   :time  (first msg)}]
          #_(locking *out* (println "send-all" (pr-str msg)))
          (async/put! (:comm state) msg)))
      ::carry
      (let [i   idx
            msg {:from  idx
                 :to    i
                 :topic [i id]
                 :value msg
                 :time  (first msg)}]
        #_(locking *out* (println "send-all" (pr-str msg)))
        (async/put! (:comm state) msg))

      (let [i   (mod (hash partition-key) (:count state))
            msg {:from  idx
                 :to    i
                 :topic [i id]
                 :value msg
                 :time  (first msg)}]
        #_(locking *out* (println "send" (pr-str msg)))
        (async/put! (:comm state) msg))))
  (subscribe [_ id to]
    (subscribe* state idx id to))
  (register-cleanup [_ f] (swap! cleaners conj-vec f))
  (close [worker]
    (close* cleaners [input (:comm state)])
    worker))

(defn make-edge
  ([input xf]
   (let [w (protocols/get-worker input)
         e (->Edge (async/chan 1 xf) w)]
     (async/pipe input e)
     e))
  ([worker]
   (let [w (protocols/get-worker worker)]
     (->Edge (async/chan) w))))

(defn- inproc-workers [n]
  (let [comm (async/chan 32)
        state {:comm  comm
               :pub   (async/pub comm :topic)
               :count n}]
    (mapv
     #(->InprocWorker % (async/merge [(async/chan) (let [notifies (async/chan)]
                                                     (subscribe* state % :input notifies)
                                                     notifies)])
                      state (atom nil))
     (range n))))

(defn- atomix-subscribe* [state index id to]
  (let [tmp (async/chan 1 (map :value))]
    (async/pipe tmp to)
    (-> state :pub (async/sub id tmp))))

(defrecord AtomixWorker [agent index nodes input state cleaners]
  protocols/IWorker
  (index [_] index)
  (input-ch [self] (->Edge input self))
  (notify-worker [_ partition-key id msg]
    (condp = partition-key
      ::all
      (let [msg {:from  index
                 :topic id
                 :value msg
                 :time  (first msg)}]
        (pubsub/broadcast! agent "workers" msg))
      ::carry
      (let [i   index
            msg {:from  index
                 :to    i
                 :topic id
                 :value msg
                 :time  (first msg)}]
        (messaging/send! agent (str "worker-" i) msg (first (nodes i))))
      (let [i   (mod (hash partition-key) (count nodes))
            msg {:from  index
                 :to    i
                 :topic id
                 :value msg
                 :time  (first msg)}]
        #_(locking *out* (println "send" (pr-str msg)))
        (messaging/send! agent (str "worker-" i) msg (first (nodes i))))))
  (subscribe [_ id to]
    (atomix-subscribe* state index id to))
  (register-cleanup [_ f] (swap! cleaners conj-vec f))
  (close [worker]
    (.stop agent)
    (close* cleaners [input (:out state)])
    worker))

(defn- atomix-worker [idx nodes cfg]
  (let [nodes (vec nodes)
        agent (atomix/replica (first (nodes idx)) nodes cfg)
        out   (async/chan 32)
        state {:out   out
               :pub   (async/pub out :topic)
               :count (count nodes)}]
    (messaging/subscribe-consumer! agent (str "worker-" idx) (fn [msg] #_(locking *out* (prn "RECV" msg)) (async/put! out msg)))
    (pubsub/subscribe-consumer! agent "workers" (fn [msg] #_(locking *out* (prn "recv-broadcast" msg)) (async/put! out msg)))
    (.start agent)
    (->AtomixWorker agent idx nodes (async/merge [(async/chan) (let [notifies (async/chan)]
                                                                 (atomix-subscribe* state idx :input notifies)
                                                                 notifies)])
                    state
                    (atom []))))

(defn- tap-ch [ch f]
  (let [out (make-edge (protocols/get-worker ch))]
    (async/go-loop []
      (when-let [item (async/<! ch)]
        (f item)
        (async/>! out item)
        (recur)))
    out))

(defn run-threads [{:keys [n]
                    :or   {n 1}}
           make-dataflow]
  (assert (or (= n :cpus) (integer? n)))
  (let [n       (if (= n :cpus)
                  (.availableProcessors (Runtime/getRuntime))
                  n)
        workers (vec
                 (for [worker (inproc-workers n)]
                   (let [w-input (input-ch worker)]
                     (protocols/consume (make-dataflow worker w-input))
                     worker)))]
    {:close   (fn closer []
                (doseq [w workers]
                  (protocols/close w)))
     :workers workers}))

(defn run-cluster [{:keys [nodes index] :as options}
                   make-dataflow]
  ;; TODO(jeff): Fix issues
  ;; - atomix messaging does not guarantee ordering (unclear if pubsub has that guarantee either)
  (let [indexes (if (sequential? index)
                  index
                  [index])
        workers (vec
                 (for [idx index]
                   (let [worker  (atomix-worker idx nodes options)
                         w-input (input-ch worker)]
                     (protocols/consume (make-dataflow worker w-input))
                     worker)))]
    {:close   (fn closer []
                (doseq [w workers]
                  (protocols/close w)))
     :workers workers}))


(defn input-variable
  [id worker]
  (->InputVariable (input-ch worker) id worker (atom 0)))

(defn trace [f]
  (map (fn [item] (f item) item)))

(defn using-lock [obj f]
  (fn [& args]
    (locking obj
      (apply f args))))

(defn capture
  ([] (capture (fn [[t _] new] [t new])))
  ([join]
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
           (rf result (join @prev item)))))])))

(defn for-each [pred xf]
  (fn [rf]
    (let [xrf (xf rf)]
      (fn
        ([] (xrf))
        ([result] (xrf result))
        ([result item]
         (if (pred item)
           (xrf result item)
           (rf result item)))))))

(defn- is-notification? [item]
  (< (count item) 2))
(defn- is-value? [item]
  (>= (count item) 2))

(defn for-notify [f]
  (fn [rf]
    (let [v (volatile! nil)]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result item] (rf result (do
                                    (let [t (item 0)]
                                      (when (not= @v t)
                                        (vreset! v t)
                                        (f t)))
                                    item)))))))

(defn pipeline-progress!
  [input xf]
  (make-edge input xf))

#_
(let [x [[1 "hello"]
         [1 "world"]
         [2]]
      [start end] (capture)]
  (into []
        (for-each is-value? (comp start (map second) end))
        x))

(defn pipeline!
  [input xf]
  (let [[start end] (capture)]
    (pipeline-progress! input (for-each is-value? (comp start (map second) xf end)))))

(defn segmented-pipeline!
  [input item-f notify-f]
  (let [[start end] (capture)]
    (pipeline-progress! input (comp (for-notify notify-f) (for-each is-value? (comp start (map second) (map item-f) end)))))
  #_
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
  ([input step-id] (exchange! input step-id identity))
  ([input step-id key-fn]
   (let [out    (make-edge (protocols/get-worker input))
         worker (protocols/get-worker input)]
     (async/go-loop []
       (if-let [msg (async/<! input)]
         (do
           (if (is-value? msg)
             (protocols/notify-worker worker (key-fn msg) step-id msg)
             (protocols/notify-worker worker ::carry step-id msg))
           (recur))
         #_(protocols/notify-worker ::close step-id nil)))
     (protocols/subscribe worker step-id out)
     out)))

(defn concat! [input other-input]
  (if (instance? LoopVariable other-input)
    (let [input-w-loop-t (pipeline-progress! input (map (fn [[t v]] [(conj t 0) v])))]
      (async/merge [input-w-loop-t other-input]))
    (async/merge [input other-input])))

(defn loop-variable
  ([worker] (loop-variable worker 1))
  ([worker amt]
  ;; TODO: we never close loop-variables... what's the best way to do this?
   (let [v (->LoopVariable (async/chan 1 (map (fn [[t v]] [(update t (dec (count t)) + amt) v]))))]
     (protocols/register-cleanup worker #(async/close! v))
     v)))

(defn loop-when! [input predicate loop-stream]
  (let [out (make-edge (protocols/get-worker input))]
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
        out (make-edge (protocols/get-worker input))]
    (async/tap m output)
    (async/tap m out)
    out))

(defn wait-for [ch expected-t]
  (let [p          (promise)
        expected-t (if (integer? expected-t)
                     [expected-t]
                     expected-t)]
    (async/go-loop []
      (if-let [msg (async/<! ch)]
        (let [t (first msg)]
          (if (not (neg? (compare t expected-t)))
            (deliver p t)
            (recur)))
        (deliver p nil)))
    p))

(defn join! [left-input right-input left-key-fn right-key-fn merge-fn]
  (throw (ex-info "Not implemented"))
  (let [out (make-edge (protocols/get-worker left-input))]
    (async/go-loop []
      #_(async/alt!
          left-input
          right-input))
    out))

(defn vertex*
  ([input value-handler] (vertex* input nil value-handler (fn [t state] [state])))
  ([input init-state value-handler] (vertex* input init-state value-handler (fn [t state] [state])))
  ([input init-state value-handler notify-handler]
   (let [out (make-edge (protocols/get-worker input))]
     (async/go-loop [state init-state
                     ct nil]
       (if-let [[t value :as msg] (async/<! input)]
         (do
           #_(locking *out* (println state "<!" msg))
           (if (is-value? msg)
             (recur (value-handler state t value) t)
             (when (and (not (nil? ct)) (not= t ct))
               (doseq [value (notify-handler ct state)]
                 (async/>! out [ct value]))
               (async/>! out [t]))))
         (do
           (when ct
             (doseq [value (notify-handler state ct)]
               (async/>! out [ct value])))
           (async/close! out))))
     out)))

(defn count!
  ([input step-id] (count! input step-id identity))
  ([input step-id partition-key]
   (let [input (exchange! input step-id partition-key)]
     (vertex* input {} (fn [state t value] (update state value (fnil inc 0)))))))

(defn reduce!
  ([input step-id f] (reduce! input step-id f nil))
  ([input step-id f init-value]
   (let [input (exchange! input step-id (constantly 0))]
     (vertex* input init-value (fn [state t value] (f state value))))))

(defn print-ch [f]
  (let [printer (async/chan 64)]
    (async/go-loop []
      (when-let [msg (async/<! printer)]
        (f msg)
        (recur)))
    printer))

(comment

  (do ;; threaded cluster - technically more thread than workers since core-async is backed by a threadpool
    (def sig (async/chan (async/sliding-buffer 1)))
    (def printer (print-ch (using-lock *out* prn)))
    (def workers
      (run-threads
       {:n 2}
       (fn [w input]
         (-> input
             #_(pipeline! (trace (partial async/put! printer)))
             (pipeline! (mapcat #(string/split % #" ")))
             (exchange! :words)
             (count! :count-words)
             (reduce! :count-merge (partial merge-with (fnil + 0 0)) {})
             #_(pipeline! (trace (partial async/put! printer)))
             (pipeline-progress! (trace (partial async/put! printer)))
             (probe! sig)
             #_(pipeline-progress! (trace (using-lock *out* prn)))
             #_(segmented-pipeline! (fn [item] (locking *out* (println "item" item)) item)
                                    (fn [coll] (locking *out* (println "coll" coll)) coll))))))
    (def input (input-variable :input (first (:workers workers)))))

  (do ;; using atomix to cluster by processes
    (def sig (async/chan (async/sliding-buffer 1)))
    (def printer (print-ch (using-lock *out* prn)))
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
    ((:close workers))))

;; TODO:
;; - Verify: do we compute the minimum amout of changes needed to propagate changes?
;; - How do we join two despirate streams?
;; - Is dropping time-lattice a "good-enough" solution? What are we loosing?
;; - If a live/OLTP system, how do we handle deploys?

