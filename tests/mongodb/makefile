rebuild:
	rm -rf dbdata;
	make dbdata

dbdata:
	mkdir dbdata
	mongod -f mongod.conf --noauth --fork --pidfilepath `pwd`/mongo.pid && sleep 2 && mongo localhost:7777/admin < setup.mongo.js

start:
	mongod -f mongod.conf --fork

stop:
	mongod -f mongod.conf --shutdown

foreground:
	mongod -f mongod.conf

