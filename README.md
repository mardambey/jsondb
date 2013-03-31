jsondb
======

JsonDB connects to MySQL on one side and listens HTTP on the other accepting
MySQL queries over HTTP, running them, JSON-p serialized the results,
and returns them to the caller. Useful with tools that need jsonp data
into the browser, like afterquery.

