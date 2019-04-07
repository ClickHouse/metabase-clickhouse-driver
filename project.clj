(defproject metabase/clickhouse-driver "1.0.0-SNAPSHOT-0.1.52"
  :min-lein-version "2.5.0"

  :aliases
  {"bikeshed"                  ["with-profile" "+bikeshed" "bikeshed" "--max-line-length" "205"]
   "check-namespace-decls"     ["with-profile" "+check-namespace-decls" "check-namespace-decls"]
   "check-reflection-warnings" ["with-profile" "+reflection-warnings" "check"]
   "docstring-checker"         ["with-profile" "+docstring-checker" "docstring-checker"]
   "eastwood"                  ["with-profile" "+eastwood" "eastwood"]
   "test"                      ["with-profile" "+expectations" "expectations"]}
  
  :dependencies
  [[ru.yandex.clickhouse/clickhouse-jdbc "0.1.52"
    :exclusions [com.fasterxml.jackson.core/jackson-core
                 org.slf4j/slf4j-api]]]

  :profiles

  {:provided
   {:dependencies [[metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "clickhouse.metabase-driver.jar"}

   :bikeshed
   {:plugins [[lein-bikeshed "0.4.1"]]}
   
   :eastwood
   {:plugins
    [[jonase/eastwood "0.3.1" :exclusions [org.clojure/clojure]]]
    
    :eastwood
    {:exclude-namespaces [:test-paths]
     :config-files       ["../metabase/test_resources/eastwood-config.clj"]
     :add-linters        [:unused-private-vars
                          :unused-namespaces
                          ;; These linters are pretty useful but give a few false positives and can't be selectively
                          ;; disabled (yet)
                          ;;
                          ;; For example see https://github.com/jonase/eastwood/issues/193
                                        ;
                          ;; It's still useful to re-enable them and run them every once in a while because they catch
                          ;; a lot of actual errors too. Keep an eye on the issue above and re-enable them if we can
                          ;; get them to work
                          #_:unused-fn-args
                          #_:unused-locals]
     ;; Turn this off temporarily until we finish removing self-deprecated functions & macros
     :exclude-linters    [:deprecations]}}

   :reflection-warnings
   {:global-vars {*warn-on-reflection* true}}

   :docstring-checker
   {:plugins
    [[docstring-checker "1.0.3"]]
    
    :docstring-checker
    {:include [#"^metabase"]
     :exclude [#"test"
               #"^metabase\.http-client$"]}}})
