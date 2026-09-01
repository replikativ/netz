(ns netz.replication-inventory-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.inventory :as inventory]))

(def addresses
  [(identity/hex->bytes (apply str (repeat 32 "cc")))
   (identity/hex->bytes (apply str (repeat 32 "11")))
   (identity/hex->bytes (apply str (repeat 32 "aa")))
   (identity/hex->bytes (apply str (repeat 32 "11")))])

(defn hexes [values]
  (mapv identity/bytes->hex values))

(deftest inventory-pages-are-sorted-distinct-and-exclusive
  (let [first-page (inventory/page-addresses addresses nil 2)
        second-page (inventory/page-addresses
                     addresses (:next-cursor first-page) 2)]
    (is (= [(apply str (repeat 32 "11"))
            (apply str (repeat 32 "aa"))]
           (hexes (:manifests first-page))))
    (is (= (apply str (repeat 32 "aa"))
           (:next-cursor first-page)))
    (is (= [(apply str (repeat 32 "cc"))]
           (hexes (:manifests second-page))))
    (is (nil? (:next-cursor second-page)))))

(deftest inventory-page-input-is-bounded-and-canonical
  (testing "invalid cursors and page sizes fail before backend work"
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (inventory/page-addresses addresses "AA" 1)))
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (inventory/page-addresses addresses nil 0)))
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (inventory/page-addresses
                  addresses nil (inc inventory/max-page-size))))))

(deftest uppercase-address-values-normalize-but-cursors-do-not
  (let [upper (apply str (repeat 32 "AB"))
        page (inventory/page-addresses [upper] nil 1)]
    (is (= [(str/lower-case upper)]
           (hexes (:manifests page))))))
