module github.com/rstutsman/cs6450-labs

go 1.25

replace github.com/rstutsman/cs6450-labs/kvs => ./

require github.com/orcaman/concurrent-map/v2 v2.0.1 // indirect
