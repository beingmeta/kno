;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/config)

(use-module '{ezrecords stringfmts logger varconfig fifo texttools})
(use-module '{knodb})

(define %loglevel %warn%)

(module-export! 'knodb/configfn)

(define (knodb/configfn setupfn (opts #f) (db #f))
  (slambda (var (val))
    (if (not (bound? val)) db
	(try
	 (tryif (and db (getopt opts 'oneshot)) db)
	 (tryif (and db (equal? (pool-source db) val)) db)
	 (tryif (and db (string? val) 
		     (or (equal? (pool-source db) val)
			 (and (position #\/ (pool-source db))
			      (file-exists? (pool-source db))
			      (equal? (realpath val) (realpath (pool-source db))))))
	   db)
	 (let* ((source (if (and (table? val) (testopt val 'source))
			    (getopt val 'source #f)
			    val))
		(open-opts (if (and (table? val) (testopt val 'source))
			       (cons val opts)
			       opts))
		(dbref (resolve-dbref val open-opts)))
	   (debug%watch "knodb/config" var db val dbref)
	   (cond ((not dbref) 
		  (if (getopt opts 'err)
		      (irritant val |InvalidDBRef| knodb/configfn)
		      db))
		 ((not db)
		  (set! db (setupfn dbref open-opts))
		  (when db (lognotice |KnoDB/CONFIG| var " = " db))
		  db)
		 ((equal? db dbref)
		  (logdebug |KnoDB/CONFIG| "Redundant " var " init")
		  db)
		 ((getopt opts 'mutable)
		  (logwarn |DBChange| var " = " dbref " replacing " db)
		  (when (getopt opts 'dropfn)
		    ((getopt opts 'dropfn) db))
		  (set! db (setupfn dbref open-opts))
		  db)
		 (else
		  (logwarn |DBConflict| 
		    var " = " db ", configured late as " val " => " dbref)
		  (when (getopt opts 'err) 
		    (irritant dbref
			|DBConflict| knodb/configfn
			var " is already " db " not " val " => " dbref))
		  db)))))))

(define (resolve-dbref val opts (db #f))
  (cond ((ambiguous? val)
	 (do-choices (v val)
	   (unless db
	     (let ((probe (knodb/ref v opts)))
	       (when (and (exists? probe) probe)
		 (set! db probe))))))
	((or (pair? val) (vector? val))
	 (doseq (v val)
	   (unless db
	     (let ((probe (knodb/ref v opts)))
	       (when (and (exists? probe) probe)
		 (set! db probe))))))
	((not (string? val))
	 (set! db (knodb/ref val opts)))
	((position #\: val)
	 (set! db (knodb/ref val opts)))
	((position #\: val)
	 (set! db (knodb/ref val opts)))
	((and (file-directory? val)
	      (file-exists? (mkpath val (getopt opts 'basename "db.pool"))))
	 (set! db (knodb/ref (mkpath val (getopt opts 'basename "db.pool")) opts)))
	((and (file-exists? val) (not (file-directory? val)))
	 (set! db (knodb/ref val opts)))
	((file-exists? (glom val ".pool"))
	 (set! db (knodb/ref (glom val ".pool") opts)))
	(else))
  (and (exists? db) db))

