(ns konserve-pg.core-test
  (:require [clojure.test :refer :all]
            [konserve.core :as k]
            [konserve-pg.core :refer [new-pg-store]]
            [clojure.core.async :refer [<!!]]))

(deftest pg-store-test
  (testing "Test the pg store functionality."
    (let [store (<!! (new-pg-store "postgres://postgres:postgres@localhost:5432/konserve"))]
       (is (= (<!! (k/exists? store :foo))
             false))
      (<!! (k/assoc-in store [:foo] nil))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/assoc-in store [:foo] :bar))
      (is (= (<!! (k/exists? store :foo))
             true))
      (is (= (<!! (k/get-in store [:foo]))
             :bar))
      (<!! (k/dissoc store :foo))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/bassoc store :binbar (byte-array (range 10))))
      (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                   (is (= (map byte (slurp input-stream))
                                          (range 10)))))))))


(comment

  (def store (<!! (new-pg-store "postgres://postgres:postgres@localhost:5432/konserve")))

  (<!! (k/assoc-in store [:foo] nil))

  (<!! (k/get-in store [:foo]))

  (<!! (k/assoc-in store [:foo] :bar))

  (run-tests))
