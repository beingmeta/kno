;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{sqldb sqloids db})

(define dbmodule (get-module (config 'DBMODULE 'sqlite)))
(use-module dbmodule)

(define dbopen (get dbmodule (config 'DBOPEN 'sqlite/open)))

(define dbspec (config 'dbspec (get-component "temp.sqlite")))

(define (build-testdb db)
  ;;; If already created?
  (ignore-errors
   (sqldb/exec db "create table arithops ( stringval VARCHAR(128), add1 INT, add2 INT);"))
  (let ((addentry (sqldb/proc db "INSERT INTO arithops (stringval,add1,add2) values (?,?,?)"))
	(getentry (sqldb/proc db "SELECT stringval FROM arithops where add1=? and add2=?"))
	(irange (config 'irange 42))
	(jrange (config 'jrange 5)))
    (applytest #t procedure? addentry)
    (applytest #t applicable? addentry)
    (applytest #f compound-procedure? addentry)
    (applytest #t non-determinstic? addentry)
    (applytest #f procedure-name addentry)
    ;; (applytest 3 procedure-arity addentry)
    (applytest 3 procedure-min-arity addentry)
    (dotimes (i irange) 
      (dotimes (j jrange)
	(addentry (glom (* i j)) i j)))
    (applytest {} getentry (1+ irange) (1+ jrange))))

(define (test-db db)
  (let ((getentry (sqldb/proc db "SELECT stringval FROM arithops where add1=? and add2=?")))
    (applytest #t procedure? getentry)
    (applytest #t applicable? getentry)
    (applytest #f compound-procedure? getentry)
    ;; (applytest 2 procedure-arity getentry)
    (applytest 2 procedure-min-arity getentry)
    (dotimes (repeat 50)
      (let ((i (random (config 'irange 42))) (j (random (config 'jrange 5))))
	(applytest (glom (* i j)) get (getentry i j) 'stringval)))))


(define (init-sqlitedb)
  (let ((db (dbopen dbspec #[] #[sqlexec #t create #t])))
    (build-testdb db)
    db))

(define (main)
  (let ((db (if (and (eq? (config 'dbmodule 'sqlite) 'sqlite)
		     (not (file-exists? dbspec)))
		(init-sqlitedb)
		(dbopen dbspec #[] #[sqlexec #t create #t]))))
    (test-db db)))
