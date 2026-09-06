(ns netz.examples.snapshot-archive
  "Bridge a complete bounded snapshot catalogue to durable blob replication.
   One archive is one replicated object: W receipts mean W complete closures,
   not W copies of each object scattered across unrelated peer sets."
  (:require [netz.content-wire :as content]
            [netz.examples.snapshot-store :as snapshot-store]
            [netz.identity :as id]
            [netz.replication.blob :as blob]
            [netz.replication.materialization :as materialization]
            [netz.replication.storage :as storage]
            [netz.snapshot-ref :as ref]
            [netz.wire :as wire]))

(def codec "netz/demo/snapshot-archive-v1")

(defn- validate-blocks! [statement blocks]
  (when-not (and (map? blocks) (<= 1 (count blocks) 1025)
                 (every? (fn [[cid bytes]]
                           (and (string? cid) (re-matches #"[0-9a-f]{72}" cid)
                                (content/verify-block? (id/hex->bytes cid) bytes))) blocks))
    (throw (ex-info "Invalid snapshot archive blocks" {})))
  (let [root (id/bytes->hex (:netz/snapshot statement))
        catalogue (some-> (get blocks root) wire/decode)
        entries (get catalogue "entries")]
    (when-not (and (vector? entries)
                   (= (set (keys blocks))
                      (conj (set (map #(id/bytes->hex (get % "cid")) entries)) root)))
      (throw (ex-info "Snapshot archive does not contain the exact catalogue closure" {}))))
  blocks)

(defn prepare
  "Return bytes, chunked blob split and existing application-object binding.
   Feed the split to the existing durable-blob transfer/controller APIs, with
   signed layout and finite retention. The binding must be authenticated by the
   application. This example is bounded by Netz's 5 MiB CBOR frame ceiling."
  [{:keys [statement blocks]}]
  (validate-blocks! statement blocks)
  (let [bytes (wire/encode-canonical
               {"v" 1 "type" codec "reference" (ref/encode statement)
                "blocks" (into (sorted-map) blocks)})
        split (blob/split bytes)]
    {:bytes bytes :split split
     :binding (materialization/application-binding
               {:store-id "netz/demo/snapshots" :application-key (:netz/snapshot statement)
                :codec codec :object-id (:object-id split) :manifest-id (:manifest-id split)})}))

(defn restore
  "Decode verified materialized blob bytes bound to the expected announcement.
   The normal snapshot-store/open! boundary still authenticates the publisher
   and validates the catalogue before any application values are decoded."
  [bytes expected-statement]
  (let [archive (wire/decode bytes)]
    (when-not (and (= #{"v" "type" "reference" "blocks"} (set (keys archive)))
                   (= 1 (get archive "v")) (= codec (get archive "type"))
                   (id/bufs= (ref/encode expected-statement) (get archive "reference")))
      (throw (ex-info "Snapshot archive binding mismatch" {})))
    {:statement expected-statement
     :blocks (validate-blocks! expected-statement (get archive "blocks"))}))

(defn stage-local!
  "Durably stage the complete archive in the controller's local blob repository
   before begin!. This local install is NOT a remote replication receipt."
  [repository {:keys [split] :as archive}]
  (doseq [{:keys [index id data]} (:pieces split)]
    (when-not (= :ok (snapshot-store/await!
                      (storage/-stage-piece! repository nil (:manifest-id split) (:manifest split) index id data)))
      (throw (ex-info "Snapshot archive staging failed" {:index index}))))
  (let [result (snapshot-store/await!
                (storage/-install-object! repository nil (:manifest-id split) (:manifest split)
                                          {} "netz/store/fsync/1"))]
    (when-not (and (= :ok (:result result)) (= "netz/store/fsync/1" (:persistence result)))
      (throw (ex-info "Snapshot archive did not reach requested persistence" {:result result}))))
  archive)
