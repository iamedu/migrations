(defproject migrations "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [fogus/bacwn "0.3.0"]
                 [com.oracle/ojdbc "7"]
                 [org.clojure/java.jdbc "0.3.0-alpha4"]
                 [korma "0.3.0-RC5"]
                 [org.clojure/data.csv "0.1.2"]]
  :main migrations.core)
