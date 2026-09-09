(ns netz.replication-directory-barrier-test
  (:require [clojure.test :refer [deftest is]]
            [netz.replication.directory-barrier :as barrier])
  (:import (java.nio.file Paths AccessDeniedException)))

(deftest invalid-ancestor-fails-before-helper-loading
  (let [loads (atom [])]
    (with-redefs [clojure.core/requiring-resolve #(swap! loads conj %)]
      (doseq [value [nil "" :bad]]
        (is (thrown? clojure.lang.ExceptionInfo (barrier/open value))))
      (is (empty? @loads)))))

(deftest absent-helper-is-actionable-and-does-no-io
  (doseq [missing ['konserve.directory-sync/provision-directory!
                   'konserve.directory-sync/sync-directory!]]
    (let [calls (atom [])]
      (with-redefs [clojure.core/requiring-resolve
                    (fn [symbol] (when-not (= symbol missing)
                                   (fn [& args] (swap! calls conj args))))]
        (try (barrier/open ".") (is false "Missing helper must fail")
             (catch clojure.lang.ExceptionInfo error
               (is (= ::barrier/unsupported-konserve (:type (ex-data error))))
               (is (= missing (:symbol (ex-data error))))))
        (is (empty? @calls))))))

(deftest opening-is-nonmutating-and-barrier-errors-propagate
  (let [calls (atom [])
        denied (AccessDeniedException. "injected")
        ancestor (.normalize (.toAbsolutePath (Paths/get "." (make-array String 0))))]
    (with-redefs [clojure.core/requiring-resolve
                  (fn [symbol]
                    (case symbol
                      konserve.directory-sync/provision-directory!
                      (fn [root target] (swap! calls conj [root target]) target)
                      konserve.directory-sync/sync-directory!
                      (fn [_] (throw denied))))]
      (let [handle (barrier/open ".")]
        (is (empty? @calls))
        (is (= ancestor (:ancestor handle)))
        (dotimes [_ 2] (is (= ancestor (barrier/provision! handle ancestor))))
        (is (= [[ancestor ancestor] [ancestor ancestor]] @calls))
        (try (barrier/sync! handle ancestor) (is false "Failure must propagate")
             (catch AccessDeniedException error (is (identical? denied error))))))))
