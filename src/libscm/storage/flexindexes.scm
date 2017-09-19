;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flexindexes)

(use-module '{ezrecords stringfmts logger texttools})
(use-module '{storage/adjuncts storage/filenames})
(use-module '{storage/flex})

(module-export! '{flex/index})

(module-export! '{flexindex-suffix})

(define-init %loglevel %notice%)

(define flexindex-suffix
  #("." (opt #((isxdigit) (isxdigit) (isxdigit) ".")) "index" (eos)))

(define (flex/index spec (opts #f))
  (when (and (table? spec) (not (index? spec)) (not opts))
    (set! opts spec)
    (set! spec (getopt opts 'index (getopt opts 'source #f))))
  (cond ((index? spec) spec)
	((not (string? spec)) (irritant spec |InvalidIndexSpec|))
	((or (position #\@ spec) (position  #\: spec))
	 (if (getopt opts 'background)
	     (use-index spec opts)
	     (open-index spec opts)))
	(else
	 (let ((baseindex (ref-index spec opts)))
	   (let* ((source (index-source baseindex))
		  (next (glom (textsubst source flexindex-suffix "")
			  ".001.index"))
		  (indexes {})
		  (count 1))
	     (while (file-exists? next)
	       (set+! indexes (ref-index next opts))
	       (set! count (1+ count))
	       (set! next (glom (textsubst source flexindex-suffix "")
			    "." (padnum count 3 16) ".index")))
	     (lognotice |FlexIndex| "Found " count " indexes based at " baseindex)
	     (indexctl baseindex 'props 'seealso indexes)
	     (indexctl indexes 'props 'base baseindex))
	   baseindex))))

(define (ref-index path opts)
  (if (file-exists? path)
      (if (getopt opts 'background)
	  (use-index path opts)
	  (open-index path opts))
      (if (getopt opts 'background)
	  (let ((ix (make-index path opts)))
	    (use-index ix))
	  (make-index path opts))))
