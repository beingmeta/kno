;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/config)

(use-module '{ezrecords text/stringfmts logger varconfig fifo texttools})
(use-module '{knodb knodb/filenames})

(define %loglevel %warn%)

(define-init db-configs (make-hashtable))

(module-export! '{knodb/configfn knodb/config-dbref})

(define (normsource dbspec)
  (realpath dbspec))

(define (knodb/configfn setupfn (opts #f) (unsetupfn #f))
  "Returns a config handler for setting up a database given a setup function and "
  "a table of configuration options.\n"
  "`knodb/configfn` also takes an *unsetupfn* but that isn't handled yet."
  (local db #f)
  (local setup #f)
  (local aliases {})
  (slambda (var (val))
    (local disabled (getopt opts 'disabled))
    (local dbname (getopt opts 'dbname "db"))
    (cond ((not (bound? val)) db)
	  ;; The most common case where we actually do something
	  ((and val (or (not db) (not setup))
		(not disabled))
	   (let ((usedb (if (or (pool? val) (index? val)) val
			    (knodb/config-dbref val opts))))
	     (unless usedb
	       (if (getopt opts 'err)
		   (irritant val |BadDBRef| "for " dbname " given " opts)
		   (logerr |BadDBRef| val " for " dbname " given " opts)))
	     ;; It's simple with no current DB
	     (set! db usedb)
	     (setupfn usedb opts)
	     (set! setup #t)))
	  ((and (not db) (not val)) db)
	  ((and (equal? db val) setup) db)
	  ((and (overlaps? val aliases) setup) db)
	  ((equal? db val)
	   (setupfn db opts)
	   (set! setup #t))
	  ((and disabled (or (not setup) (not db)))
	   ;; Okay to change it because it hasn't been setup
	   (set! db val)
	   val)
	  (disabled
	   (logwarn |Locked|
	     "The " dbname " is currently set up as " db " and disabled")
	   db)
	  ((or (not db) (not val))
	   (logwarn |CodeError| "You should never see this")
	   db)
	  (else
	   ;;; At this point, neither db nor val nor setup are false and we're not disabled
	   ;;; We also know that db and val are not the same (equal)
	   (let* ((source (if (and (table? val) (testopt val 'source))
			      (getopt val 'source #f)
			      val))
		  (open-opts (if (table? val)
				 (cons val opts)
				 opts)))
	     (cond ((and (string? source) (equal? source (dbctl db 'source))) db)
		   ((overlaps? source aliases) db)
		   ((and (string? source) (knodb/same-path? db source))
		    (loginfo |RedundantConfig| "The " dbname " is already configured as " db)
		    ;; This will only generate the log message once
		    (set+! aliases source)
		    db)
		   (unsetupfn db)
		   ((getopt opts 'err)
		    (irritant val |DBConflict| "The " dbname " has already been configured with " db))
		   (else
		    (logwarn |DBConflict|
		      "Can't configure db " dbname " as " val ", the " dbname
		      " has already been configured to "
		      db))))))))

(define (knodb/config-dbref val opts)
  (local db #f)
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
	((has-suffix val {".pool" ".index" ".flexpool" ".flexindex"})
	 (set! db (knodb/ref val opts)))
	((and (file-directory? val)
	      (file-exists? (mkpath val (getopt opts 'basename "db.pool"))))
	 (set! db (knodb/ref (mkpath val (getopt opts 'basename "db.pool")) opts)))
	((and (file-directory? val)
	      (file-exists? (mkpath val (glom (basename (strip-suffix val "/")) ".pool"))))
	 (set! db (knodb/ref (mkpath val (glom (basename (strip-suffix val "/")) ".pool"))
			     opts)))
	((and (file-directory? val)
	      (file-exists? (mkpath val (glom (basename (strip-suffix val "/")) ".flexpool"))))
	 (set! db (knodb/ref (mkpath val (glom (basename (strip-suffix val "/")) ".flexpool"))
			     opts)))
	((and (file-exists? val) (not (file-directory? val)))
	 (set! db (knodb/ref val opts)))
	((file-exists? (glom val ".pool"))
	 (set! db (knodb/ref (glom val ".pool") opts)))
	(else))
  (and (exists? db) db))

