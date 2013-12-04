(defproject adi-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.async "0.1.242.0-44b1e3-alpha"]
                 [org.clojure/clojurescript "0.0-1934"]
                 [org.clojure/data.csv "0.1.2"]
                 [im.chit/adi "0.3.1"]
                 [purnam "0.1.0"]
                 [http-kit "2.1.12"]
                 [compojure "1.1.5"]
                 [hiccup "1.0.4"]
                 [prismatic/dommy "0.1.2"]
                 [lobos "1.0.0-beta1"]
                 [org.clojure/java.jdbc "0.3.0-alpha5"]
                 ;;[korma "0.3.0-RC6"]
                 ;;[org.clojure/java.jdbc "0.2.3"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [cheshire "5.2.0"]]

   :cljsbuild {:builds [{:source-paths ["src"]
                         :compiler {:output-to "resources/public/js/adi-example.js"
                                    :optimizations :whitespace
                                    :pretty-print true}}]})
