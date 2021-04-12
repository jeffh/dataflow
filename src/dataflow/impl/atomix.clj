(ns dataflow.impl.atomix
  (:require [dataflow.impl.atomix.messaging :as msg]
            [dataflow.impl.atomix.pubsub :as pubsub]
            [dataflow.impl.protocols :as protocols])
  (:import [io.atomix.core Atomix]
           [io.atomix.cluster Node MemberId Member]
           [io.atomix.core.profile Profile]
           [io.atomix.cluster.discovery BootstrapDiscoveryProvider]
           [io.atomix.protocols.raft.partition RaftPartitionGroup]
           [io.atomix.storage StorageLevel]
           [java.util.concurrent Executors]
           [java.util.function Function Consumer]))

(set! *warn-on-reflection* true)

(defn ^MemberId ->member-id [id]
  (cond
    (instance? MemberId id) id
    (instance? Node id) (.id ^Node id)
    :else (MemberId/from (str id))))

(defn ^Node ->node [m-or-v]
  (cond
    (instance? Node m-or-v) m-or-v
    (map? m-or-v)
    (let [{:keys [id host port]} m-or-v]
      (cond-> (Node/builder)
        id (.withId (str id))
        (and host port) (.withAddress (str host) (int port))
        (and (not host) port) (.withAddress (int port))
        (and host (not port)) (.withAddress (str host))
        :always (.build)))
    :else
    (let [[id host port] m-or-v]
      (cond-> (Node/builder)
        id (.withId (str id))
        (and host port) (.withAddress (str host) (int port))
        (and (not host) port) (.withAddress (int port))
        (and host (not port)) (.withAddress (str host))
        :always (.build)))))

(defn ^BootstrapDiscoveryProvider bootstrap-discover [nodes]
  (let [b (BootstrapDiscoveryProvider/builder)]
    (.build (.withNodes b ^java.util.Collection (mapv ->node nodes)))))

(defn ^RaftPartitionGroup raft [name members {:keys [data-dir num-partitions partition-size segment-size flush-on-commit? max-entry-size
                                                     storage-level]}]
  (-> (RaftPartitionGroup/builder (str name))
      (.withMembers ^java.util.Collection (mapv ->member-id members))
      (cond->
       data-dir (.withDataDirectory data-dir)
       num-partitions (.withNumPartitions (int num-partitions))
       partition-size (.withPartitionSize (int partition-size))
       segment-size (.withSegmentSize (long segment-size))
       max-entry-size (.withMaxEntrySize (int max-entry-size))
       storage-level (.withStorageLevel (if (instance? StorageLevel storage-level)
                                          storage-level
                                          (case storage-level
                                            :mapped StorageLevel/MAPPED
                                            :disk StorageLevel/DISK)))
       flush-on-commit? (.withFlushOnCommit flush-on-commit?))))

(defn ^Profile profile-consensus [& member-ids] (Profile/consensus ^java.util.Collection member-ids))
(defn ^Profile profile-grid
  ([] (Profile/dataGrid))
  ([num-partitions] (Profile/dataGrid (int num-partitions))))
(defn ^Profile profile-client [] (Profile/client))

(defn ^Atomix cluster [{:keys [cluster-id member-id membership-provider host port
                               multicast? management-group partition-groups profiles properties zone rack]}]
  (.build
   (cond-> (Atomix/builder)
     cluster-id (.withClusterId (str cluster-id))
     member-id (.withMemberId (->member-id member-id))
     membership-provider (.withMembershipProvider ^io.atomix.cluster.discovery.NodeDiscoveryProvider membership-provider)
     management-group (.withManagementGroup management-group)
     partition-groups (.withPartitionGroups ^java.util.Collection partition-groups)
     profiles (.withProfiles ^java.util.Collection profiles)
     properties (.withProperties ^java.util.Properties properties)
     (and host port) (.withAddress (str host) (int port))
     (and (not host) port) (.withAddress (int port))
     (and host (not port)) (.withAddress (str host))
     rack (.withRack (str rack))
     zone (.withZone (str zone))
     multicast? (.withMulticastEnabled))))

(defn ^Atomix replica
  ([local-node-id nodes] (replica local-node-id nodes nil))
  ([local-node-id nodes config]
  (let [nodes (mapv ->node nodes)
        ^Node self  (first (filter (comp #{local-node-id} #(str (.id ^Node %))) nodes))]
    (cluster (merge {:membership-provider (bootstrap-discover nodes)
                     :member-id           local-node-id
                     :host                (.host (.address self))
                     :port                (.port (.address self))}
                    config)))))

(defn member->map [^Member m]
  {:id         (str (.id m))
   :properties (into {} (.properties m))
   :active?    (.isActive m)
   :rack       (.rack m)
   :version    (str (.version m))
   :zone       (.zone m)
   :config     (let [cfg (.config m)]
                 {:address    (str (.getAddress cfg))
                  :host       (.getHost cfg)
                  :id         (str (.getId cfg))
                  :rack       (.getRack cfg)
                  :zone       (.getZone cfg)
                  :properties (into {} (.getProperties cfg))})})
(defn local-member [^Atomix agent] (member->map (.getLocalMember (.getMembershipService agent))))
(defn members [^Atomix agent] (set (map member->map (.getMembers (.getMembershipService agent)))))
(defn reachable-members [^Atomix agent] (set (map member->map (.getReachableMembers (.getMembershipService agent)))))

(comment
  (do
    (def nodes [["node1" nil 5677]
                ["node2" nil 5678]
                ["node3" nil 5679]])
    (def r1 (replica (first (nodes 0)) nodes))
    (def r2 (replica (first (nodes 1)) nodes))
    (def r3 (replica (first (nodes 2)) nodes)))

  (doseq [r [r1 r2 r3]]
    (.start r))

  (doseq [r [r1 r2 r3]]
    (.stop r))

  (members r2)
  (local-member r2)

  (msg/subscribe-handler! r1 "test" (fn [x] (println "RECV" x) x))
  (msg/unsubscribe! r1 "test")
  (def res (msg/send! r2 "test" "hello world" "node1"))
  (.getNow res :missing)
  (msg/send! r1 "node1" "hello world" "node2")


  (pubsub/subscribe-handler! r1 "test" (fn [x] (println "RECV" x) (str x "world")))
  (pubsub/unsubscribe! r1 "test")
  (pubsub/subscribe-handler! r2 "test" (fn [x] (println "RECV2" x) x))
  (pubsub/unsubscribe! r2 "test")
  (def res (pubsub/send! r2 "test" "hello world"))
  (.getNow res :missing)
  (pubsub/broadcast! r2 "test" "hello world")
  )
