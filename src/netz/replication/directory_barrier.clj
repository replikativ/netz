(ns netz.replication.directory-barrier
  "Optional Konserve directory barriers for exclusively owned JVM file stores.
  Loading this namespace does not load Konserve or mutate the filesystem."
  (:import (java.nio.file Path Paths)))

(defn- absolute-path ^Path [value]
  (let [^Path p (cond
                  (instance? Path value) value
                  (and (string? value) (seq value))
                  (Paths/get value (make-array String 0))
                  :else (throw (ex-info "An explicit durable ancestor is required"
                                        {:type ::invalid-ancestor})))]
    (.normalize (.toAbsolutePath p))))

(defn open
  "Resolve required helpers without performing IO. The caller asserts that the
  ancestor's name is already durable and excludes concurrent filesystem mutation.
  Use provision! before returning a writable store or accepting retained writes."
  [ancestor]
  (let [ancestor (absolute-path ancestor)
        resolve! (fn [symbol]
                   (try
                     (or (requiring-resolve symbol)
                         (throw (ex-info "Required Konserve helper is missing" {})))
                     (catch Exception cause
                       (throw (ex-info "Fsync storage requires Konserve directory provisioning support"
                                       {:type ::unsupported-konserve :symbol symbol}
                                       cause)))))
        provision (resolve! 'konserve.directory-sync/provision-directory!)
        sync (resolve! 'konserve.directory-sync/sync-directory!)]
    {:ancestor ancestor :provision provision :sync sync}))

(defn provision!
  "Repeat all managed parent/name barriers, including existing components."
  [{:keys [ancestor provision]} target]
  (provision ancestor target))

(defn sync!
  "Persist a directory after rename. Never suppress a failed barrier."
  [{:keys [sync]} directory]
  (sync directory))
