(defproject org.clojars.mihaelkonjevic/konserve-pg "0.1.3-SNAPSHOT"
  :description "A PostgreSQL backend for konserve with HugSQL."
  :url "https://github.com/retro/konserve-pg"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.2.6"]
                 [com.layerware/hugsql "0.4.9"]
                 [to-jdbc-uri "0.1.0"]
                 [hugsql-adapter-case "0.1.0"]
                 [io.replikativ/konserve "0.5.0"]])

