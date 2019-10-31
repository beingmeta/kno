;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidref)

(use-module '{logger webtools varconfig libarchive texttools
	      brico stringfmts})
(use-module '{flexdb flexdb/branches flexdb/typeindex 
	      flexdb/flexindex})

(module-export! '{wikid->brico})

(define %loglevel %notice%)

(define wikid-brico-map #f)

(define (wikid->brico wikid (wikidstring))
  (default! wikidstring (if (string? wikid) wikid (get wikid 'id)))
  (unless (oid? wikid) (set! wikid (or (wikidata/ref wikidstring) wikid)))
  (try (tryif wikid-brico-map (get wikid-brico-map wikidstring))
       (?? 'wikidref wikidstring)
       wikidstring))

(define (brico->wikid frame)
  (try (tryif wikid-brico-map (get wikid-brico-map frame))
       (wikidata/ref (get frame 'wikidref))
       #f))
