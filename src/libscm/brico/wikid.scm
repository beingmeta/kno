;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/wikid)

(define %nosubst '{wikidsource terms.pool names.pool wikid.index})

(use-module '{texttools reflection logger varconfig storage/flex})
(use-module 'usedb)

(define-init %loglevel %notify%)
;;(set! %loglevel %debug%)

(define wikidsource #f)
(define wikid.db #f)

(define terms.base (oid-plus @1/0 (* 256 1024 1024)))
(define terms.pool {})

(define names.base (oid-plus @1/0 (* 512 1024 1024)))
(define names.pool {})

(define wikid.index {})

(module-export!
 '{terms.pool terms.base names.pool names.base
   wikid.index})

(define wikid-readonly #t)
(varconfig! wikid:readonly wikid-readonly)

(define wikid-background #t)
(varconfig! wikid:background wikid-background)

(defambda (setup-wikid source (success #f) (setup #f) 
		       (use-indexes #f))
  (logdebug |SetupWikiD| source)
  (cond ((ambiguous? source)
	 (do-choices (elt source)
	   (unless success
	     (when (setup-wikid elt) (set! success #t)))))
	((or (pair? source) (vector? source))
	 (doseq (elt source)
	   (unless success
	     (when (setup-wikid elt) (set! success #t)))))
	((not (string? source)))
	((textsearch #{"," ";" ":" "|"} source)
	 (let* ((first-sep (textsearch #{"," ";" ":" "|"} source))
		(sepchar (slice source first-sep (1+ first-sep))))
	   (set! success
	     (setup-wikid (remove "" (segment source sepchar))))))
	((and (has-suffix source {".pool" ".index"})
	      (file-exists? source))
	 (set! success (setup-wikid (dirname (abspath source)))))
	((and (file-directory? source) 
	      (file-exists? (mkpath source "wikid.db")))
	 (set! success (setup-brico (mkpath source "wikid.db"))))
	((file-directory? source)
	 (let ((pools {}) (indexes {}) (failed #f) 
	       (sources (getfiles source)))
	   (do-choices (file sources)
	     (onerror
		 (cond ;; Use other pools in the directory
		       ((has-suffix file ".pool")
			(set+! pools (pool/ref file [readonly wikid-readonly])))
		       ((has-suffix file ".index")
			(set+! indexes
			  (open-index file
			    [readonly wikid-readonly
			     background wikid-background]))))
		 (lambda (ex) 
		   (logwarn |DBFailed| "Couldn't use " file)
		   (set! failed #t)
		   (break))))
	   (when (or (exists? pools) (exists? indexes))
	     (lognotice |WikiD|
	       "Loaded " (choice-size pools) " pools "
	       "and " (choice-size indexes) " indexes for WikiD "
	       "from " source)
	     (logdebug |WikiD|
	       "Loaded " (choice-size pools) " pools "
	       "and " (choice-size indexes) " indexes:"
	       (do-choices (pool pools) (printout "\n\t" pool))
	       (do-choices (index indexes) (printout "\n\t" index))))
	   (when  (and (not failed) (exists? pools) (exists? indexes)
		       (name->pool "wikid.framerd.org"))
	     (set! brico.db `#[%pools ,pools %indexes ,indexes])
	     (set! use-indexes indexes)
	     (set! success #t)
	     (set! setup #t))
	   success))
	((file-exists? source)
	 (onerror
	     (begin
	       (set! brico.db 
		 (usedb source [readonly wikid-readonly
				background wikid-background]))
	       (set! success #t)
	       (set! setup #t))
	     (lambda (ex)
	       (logwarn |BadWikiD| 
		 "Couldn't use WIKIDSOURCE " source ": "
		 ex)
	       #f)))
	((file-exists? (glom source ".db"))
	 (set! success (setup-wikid (glom source ".db"))))
	((or (textmatch `#((isalnum+) "@" ,host-name) source)
	     (textmatch `#(,host-name ":" (isdigit+)) source))
	 (onerror
	     (begin
	       (use-pool source [readonly wikid-readonly])
	       (open-index source [readonly wikid-readonly
				   background wikid-background])
	       (set! success #t)
	       (set! setup #t))
	     (lambda (ex)
	       (logwarn |BadWikiD| 
		 "Couldn't use WIKIDSOURCE " source ": " ex)
	       #f))))
  (unless success
    (logwarn |WikiDSource| "Setup failed: " source))
  (when (and success setup)
    (set! wikidsource source)
    (if wikid.db
	(set! wikid.index (get brico.db '%indexes))
	(if use-indexes (set! wikid.index use-indexes)))
    (set! terms.pool (name->pool "wikidterms.brico.framerd.org"))
    (set! names.pool (name->pool "wikidnames.brico.framerd.org"))
    (set! wikid-dir 
      (and (not (textsearch #/:@/ (pool-source terms.pool)))
	   (dirname (pool-source terms.pool)))))
  success)

(define wikidsource-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) wikidsource)
	  ((equal? val wikidsource)
	   wikidsource)
	  ((and (exists? terms.pool) terms.pool
		(equal? val (pool-source terms.pool))))
	  ((and (exists? terms.pool) terms.pool)
	   (logwarn |Brico| "Redundant WIKID configuration "
		    "from " val " \&mdash; "
		    "WIKID is already provided from "
		    (pool-source terms.pool))
	   #f)
	  (else (setup-wikid val)))))
(config-def! 'wikidsource wikidsource-config)

(define (init-wikid dir)
  (flexdb/make (mkpath dir "terms.pool")
	       [type 'bigpool base (oid-plus @1/0 (* 128 1024 1024))
		capacity (* 1024 1024)
		adjuncts #[%words #[type bigpool pool "terms_words"]
			   %norms #[type bigpool pool "terms_norms"]
			   %glosses #[type bigpool pool "terms_glosses"]
			   %aliases #[type bigpool pool "terms_aliases"]
			   %indicators #[type bigpool pool "terms_indicators"]]])
  (flexdb/make (mkpath dir "names.pool")
	       [type 'bigpool base (oid-plus @1/0 (* 256 1024 1024))
		capacity (* 1024 1024)
		adjuncts #[%words #[type bigpool pool "names_words"]
			   %norms #[type bigpool pool "names_norms"]
			   %glosses #[type bigpool pool "names_glosses"]
			   %aliases #[type bigpool pool "names_aliases"]
			   %indicators #[type bigpool pool "names_indicators"]]]))

