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
use fdtest;
db.createUser(
  {
    user: "root",
    pwd: "framerd",
      roles: [ { role: "userAdmin", db: "fdtest" },
	       { role: "dbAdmin", db: "fdtest" },
	       { role: "readWrite", db: "fdtest" },
	       { role: "dbOwner", db: "fdtest" } ]
  }
)
db.createUser(
  {
    user: "writer",
    pwd: "none",
      roles: [ { role: "readWrite", db: "fdtest" } ]
  }
)
db.createUser(
  {
    user: "reader",
    pwd: "none",
      roles: [ { role: "read", db: "fdtest" } ]
  }
)
use admin;
db.shutdownServer();


