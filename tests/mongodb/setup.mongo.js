use admin
db.createUser(
  {
    user: "root",
    pwd: "framerd",
      roles: [ { role: "userAdminAnyDatabase", db: "admin" },
	       { role: "dbAdminAnyDatabase", db: "admin" },
	       { role: "readWriteAnyDatabase", db: "admin" },
	       { role: "userAdmin", db: "admin" },
	       { role: "dbAdmin", db: "admin" },
	       { role: "readWrite", db: "admin" },
	       { role: "dbOwner", db: "admin" },
	       { role: "root", db: "admin" } ]
  }
)
use knotest;
db.createUser(
  {
    user: "root",
    pwd: "framerd",
      roles: [ { role: "userAdmin", db: "knotest" },
	       { role: "dbAdmin", db: "knotest" },
	       { role: "readWrite", db: "knotest" },
	       { role: "dbOwner", db: "knotest" } ]
  }
)
db.createUser(
  {
    user: "writer",
    pwd: "none",
      roles: [ { role: "readWrite", db: "knotest" } ]
  }
)
db.createUser(
  {
    user: "reader",
    pwd: "none",
      roles: [ { role: "read", db: "knotest" } ]
  }
)
use admin;
db.shutdownServer();


