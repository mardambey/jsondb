jsondb
======

JsonDB connects to MySQL on one side and listens HTTP on the other accepting
MySQL queries over HTTP, running them, JSON(-p) serializes the results,
and returns them to the caller. Useful with tools that need json(p) data
into the browser, like afterquery.

JsonDB can store queries (with aliases) in a QueryStore. These queries can
be loaded on startup (or after, on demand) and can have a refresh interval
associated with them so they are always fresh in the cache.

Example usage
-------------

    http://server/query?sql=select * from users
    http://server/query?alias=users-by-country

Under the hood
--------------
* [MapDB](http://www.mapdb.org/): Persistence, in-memory LRU cache, data comperession.
* [Akka](http://akka.io): Concurrency, configuration, scheduler
* [Netty](http://netty.io): Http web server

--
Hisham Mardam-Bey <hisham.mardambey@gmail.com>

