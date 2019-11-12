;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'brico/wikid)

(use-module '{texttools reflection logger varconfig stringfmts knodb})

(define-init %loglevel %notify%)
;;(set! %loglevel %debug%)

(module-export! '{wikid.pool wikid.index wikid.source})
(define %nosubst '{wikid.source wikid.pool wikid.index wikid.background})

(define wikid.source #f)
(define wikid.pool #f)
(define wikid.index #f)
(define wikid.indexes #f)
(define wikid.background #f)
(define wikid.opts #f)

(define (setup-wikid source (opts wikid.opts) (err))
  (default! err (getopt opts 'err #t))
  (cond ((and wikid.source
	      (or (equal? source wikid.source)
		  (equal? (realpath source) wikid.source)))
	 (unless (config 'quiet)
	   (loginfo |RedundantWikiDInit|
	     "Wikid is already consistently provided from wikid.source"))
	 wikid-source)
	((and wikid.source err)
	 (irritant wikid.source |WikidSourceConflict|))
	(wikid.source wikid.source)
	((or (position #\@ source) (position #\: source))
	 (set! wikid.pool (kb/ref source (opt+ opts 'pool source)))
	 (set! wikid.index
	   (kb/ref source (opt+ opts 'index source 
				    'background wikid.background)))
	 (unless (config 'quiet)
	   (lognotice |WIKID| "being accessed from " wikid.source))
	 (set! wikid.indexes wikid.index)
	 (set! wikid.source source))
	((not (file-directory? source))
	 (irritant source |WikidSourceNotADirectory|))
	((not (file-exists? (mkpath source "wikid.pool")))
	 (irritant source |NoWikidPool|))
	(else
	 (set! wikid.pool (flex/ref (mkpath source "wikid.pool") wikid.opts))
	 (let* ((indexfiles (pick (getfiles source) has-suffix ".index"))
		(use-opts (opt+ opts 'register (getopt opts 'register #t)
				'background wikid.background))
		(indexes (flex/ref indexfiles use-opts)))
	   (set! wikid.indexes indexes)
	   (set! wikid.index (make-aggregate-index indexes [register #t]))
	   (unless (config 'quiet)
	     (lognotice |WIKID| "Using one pool, "
			($size (getvalues (dbctl wikid.pool 'adjuncts)) "adjunct") ", and "
			($size indexes "index" "indexes")
			" for WIKID from "  source)))
	 (set! wikid.source source))))

(config-def! 'wikidsource
  (lambda (var (val))
    (if (unbound? val) wikid.source
	(setup-wikid val wikid.opts #t))))

(config-def! 'wikid:background
  (lambda (var (val))
    (cond ((unbound? val) wikid.background)
	  ((and (not val) wikid.background)
	   (irritant wikid.background '|CantRemoveFromBackground|))
	  ((and val wikid.background) wikid.index)
	  (else (use-index wikid.indexes)
		(set! wikid.background #t)
		wikid.index))))

