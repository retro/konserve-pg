# konserve-pg

A PostgreSQL implementation of the [konserve kv-protocol](https://github.com/replikativ/konserve) on top of [HugSQL](https://www.hugsql.org/).

## Usage

Add to your leiningen dependencies:
[![Clojars Project](http://clojars.org/org.clojars.mihaelkonjevic/konserve-pg/latest-version.svg)](http://clojars.org/org.clojars.mihaelkonjevic/konserve-pg)

The whole purpose of konserve is to have a unified associative key-value interface for
edn datastructures. Just use the standard interface functions of konserve.

You can also provide a DB connection object to the `new-pg-store` constructor
as an argument. We do not require additional settings beyond the konserve
serialization protocol for the store, so you can still access the store through
PostgreSQL directly wherever you need.

~~~clojure
  (require '[konserve-pg.core :refer :all]
           '[konserve.core :as k)
  (def pg-store (<!! (new-pg-store "postgres://postgres:postgres@localhost:5432/konserve")))

  (<!! (k/exists? pg-store  "john"))
  (<!! (k/get-in pg-store ["john"]))
  (<!! (k/assoc-in pg-store ["john"] 42))
  (<!! (k/update-in pg-store ["john"] inc))
  (<!! (k/get-in pg-store ["john"]))

  (defrecord Test [a])
  (<!! (k/assoc-in pg-store ["peter"] (Test. 5)))
  (<!! (k/get-in pg-store ["peter"]))
~~~


## Changes

### 0.1.0

- binary support
- use konserve 0.5.0
- arbitrary key length (hashing)
- use new reduced konserve interface and serializers

## License

Copyright © 2014-2019 Christian Weilbach and Mihael Konjevic

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
