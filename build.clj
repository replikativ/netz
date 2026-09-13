(ns build
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'org.replikativ/netz)
(def version-prefix (str/trim (slurp "VERSION_PREFIX")))
(def version (format "%s.%s" version-prefix (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(defn jar-file [] (format "target/%s-%s.jar" (name lib) version))

(defn- require-text! [path expected]
  (let [contents (slurp path)]
    (when-not (str/includes? contents expected)
      (throw (ex-info "Release metadata is out of sync"
                      {:path path :expected expected})))))

(defn- require-entry! [^java.util.jar.JarFile jar entry]
  (when-not (.getJarEntry jar entry)
    (throw (ex-info "Required release artifact entry is missing"
                    {:jar (jar-file) :entry entry}))))

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
  (b/copy-file {:src (jar-file) :target "target/netz.jar"})
  {:lib lib :version version :jar-file (jar-file)})

(defn verify [_]
  (jar nil)
  (let [py-version version]
    (require-text! "pyproject.toml" "dynamic = [\"version\"]")
    (require-text! "setup.py" "from build_version import build_version")
    (require-text! "README.md" "0.1.<commit-count>")
    (require-text! "docs/getting-started.md" "0.1.<commit-count>")
    (require-text! "CHANGELOG.md" "## 0.1 series — alpha")
    (with-open [artifact (java.util.jar.JarFile. (io/file (jar-file)))]
      (doseq [entry ["META-INF/LICENSE"
                     "META-INF/maven/org.replikativ/netz/pom.xml"
                     "netz/core.cljc"
                     "netz/wire.cljc"]]
        (require-entry! artifact entry))
      (let [pom-entry (.getJarEntry artifact
                                    "META-INF/maven/org.replikativ/netz/pom.xml")
            pom (slurp (.getInputStream artifact pom-entry))]
        (when-not (and (str/includes? pom (str "<version>" version "</version>"))
                       (str/includes? pom "Apache License, Version 2.0"))
          (throw (ex-info "Generated POM has incorrect release metadata"
                          {:version version})))))
    (println "Verified JVM" version "and shared Python release metadata")
    {:lib lib :version version :python-version py-version
     :jar-file (jar-file)}))

(defn install [_]
  (jar nil)
  (b/install {:basis @basis :lib lib :version version
              :jar-file (jar-file) :class-dir class-dir}))

(defn deploy [_]
  (verify nil)
  (dd/deploy {:installer :remote
              :artifact (jar-file)
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))
