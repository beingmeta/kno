;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikipedia)
;;; This uses tables to get Wikipedia mappings for BRICO concepts

(define %volatile 'brico/wikiref)

(use-module '{brico webtools xhtml texttools})

(define wikisource #f)
(define brico2enwiki {})
(define brico2wiki {})

(define (default-wikiref concept (language default-language))
  (try (tryif (eq? language english) (get brico2enwiki concept))
       (get brico2wiki (cons language concept))))
(define brico/wikiref default-wikiref)

(define (brico/wikiurl concept (language default-language))
  (string-append
   "http://"  (get language 'iso639/1) ".wikipedia.org/wiki/"
   (string-subst (brico/wikiref concept language) " " "_")))

(module-export! '{brico2wiki brico2enwiki brico/wikiref brico/wikiurl})

(define (config-wikisource var (val))
  (cond ((not (bound? val)) wikisource)
	((not (string? val))
	 (error "Invalid WIKISOURCE" val))
	((equal? val wikisource) #f)
	((position #\@ val)
	 (set! brico/wikiref (dtproc val 'getwikiref))
	 (set! wikisource val)
	 #t)
	(else
	 (when (file-exists? (mkpath val "brico2enwiki.table"))
	   (message "Using enwiki mapping from "
		    (mkpath val "brico2enwiki.table"))
	   (set! brico2enwiki
		 (file->dtype (mkpath val "brico2enwiki.table"))))
	 (when (file-exists? (mkpath val "brico2wiki.index"))
	   (message "Using wiki mapping from "
		    (mkpath val "brico2wiki.index"))
	   (set! brico2wiki
		 (open-index (mkpath val "brico2wiki.index"))))
	 (set! wikisource val))))
(config-def! 'wikisource config-wikisource)



