(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'io.replikativ/netz)
(def version "0.1.0-alpha.1")
(def class-dir "target/classes")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(defn jar-file [] (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src"]
                :scm {:url "https://github.com/replikativ/netz"
                      :connection "scm:git:git://github.com/replikativ/netz.git"
                      :developerConnection "scm:git:ssh://git@github.com/replikativ/netz.git"}
                :pom-data [[:description "Authenticated P2P dissemination and state synchronization for Kabel."]
                           [:url "https://github.com/replikativ/netz"]
                           [:licenses
                            [:license
                             [:name "Apache License, Version 2.0"]
                             [:url "https://www.apache.org/licenses/LICENSE-2.0.txt"]
                             [:distribution "repo"]]]]})
  (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
  (b/copy-file {:src "LICENSE" :target (str class-dir "/META-INF/LICENSE")})
  (b/jar {:class-dir class-dir :jar-file (jar-file)})
  {:lib lib :version version :jar-file (jar-file)})

(defn install [_]
  (jar nil)
  (b/install {:basis @basis :lib lib :version version
              :jar-file (jar-file) :class-dir class-dir}))

(defn deploy [_]
  (jar nil)
  (dd/deploy {:installer :remote
              :artifact (jar-file)
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))
