(ns konserve-pg.core-test
  (:require [clojure.test :refer :all]
            [konserve.core :as k]
            [konserve-pg.core :refer [new-pg-store delete-store]]
            [clojure.core.async :refer [<!!]]))

(deftest pg-store-test
  (testing "Test the pg store functionality."
    (let [uri "postgres://postgres:postgres@localhost:5432/konserve"
          store (<!! (new-pg-store uri))]
      (is (= (<!! (k/exists? store :foo))
             false))
      (<!! (k/assoc-in store [:foo] nil))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      
      (<!! (k/assoc-in store [:num] 1))
      (is (= (<!! (k/get-in store [:num])) 1))

      (<!! (k/update-in store [:num] inc))
      (is (= (<!! (k/get-in store [:num])) 2))

      (<!! (k/update-in store [:num] + 2 3))
      (is (= (<!! (k/get-in store [:num])) 7))

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
                                          (range 10))))))
      (delete-store uri))))


(comment

  (def store (<!! (new-pg-store "postgres://postgres:postgres@localhost:5432/konserve")))

  (<!! (k/assoc-in store [:foo] nil))

  (<!! (k/get-in store [:foo]))

  (<!! (k/assoc-in store [:foo] :bar))

  (run-tests))
