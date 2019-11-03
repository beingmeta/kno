;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata)

(use-module '{logger webtools varconfig libarchive texttools
	      brico stringfmts})
(use-module '{flexdb flexdb/branches flexdb/typeindex 
	      flexdb/flexindex})

(module-export! '{wikidata.dir
		  wikidata.pool wikidata.index  wikids.index
		  words.index norms.index has.index props.index
		  buildmap.table
		  wikidata.props
		  wikidata/ref wikid/ref
		  wikidata/find wikid/find})

(module-export! '{wikidata/save!})

(define %loglevel %notice%)

(define wikidata.dir #f)

(define wikidata.pool #f)
(define wikidata.index #f)
(define wikids.index #f)

(define words.index #f)
(define norms.index #f)
(define has.index #f)
(define props.index #f)

(define buildmap.table #f)
(define wikidata.props #f)

(define wikidata-build #f)

(define (setup-wikidata dir)
  (if core.index
      (let ((props (?? 'type '{wikidprop wikidata}))
	    (table (make-hashtable)))
	(prefetch-oids! props)
	(do-choices (prop props)
	  (add! table (get prop 'wikid) prop)
	  (add! table (downcase (get prop 'wikid)) prop)
	  (add! table (string->symbol (downcase (get prop 'wikid))) prop))
	(set! wikidata.props table))
      (error |NoBrico|))

  (set! wikidata.dir (realpath dir))

  (set! wikidata.pool
    (flexdb/make (mkpath dir "wikidata.flexpool")
		 [create #t type 'flexpool
		  base @31c1/0 capacity (* 128 1024 1024)
		  partsize (* 1024 1024) pooltypek 'bigpool
		  prefix "pools/"
		  adjuncts #[labels #[pool "labels"]
			     aliases #[pool "aliases"]
			     claims #[pool "claims"]
			     sitelinks #[pool "sitelinks"]]
		  reserve 1]))

  (when wikidata-build
    (set! buildmap.table
      (flexdb/make (mkpath dir "wikids.table") [indextype 'memindex create #t])))

  (set! wikids.index
    (flex/open-index (mkpath dir "wikids.flexindex")
		     [indextype 'hashindex size (* 8 1024 1024) create #t
		      keyslot 'id register #t
		      maxkeys (* 4 1024 1024)]))

  (set! words.index
    (flex/open-index (mkpath dir "words.flexindex")
		     [indextype 'hashindex size (* 4 1024 1024) create #t
		      keyslot 'words register #t
		      maxkeys (* 2 1024 1024)]))
  (set! norms.index
    (flex/open-index (mkpath dir "norms.flexindex")
		     [indextype 'hashindex size (* 4 1024 1024) create #t
		      keyslot 'norms register #t
		      maxkeys (* 2 1024 1024)]))
  (set! has.index
    (flexdb/make (mkpath dir "hasprops.index")
		 [indextype 'hashindex create #t keyslot 'has register #t]))
  (set! props.index
    (flex/open-index (mkpath dir "props.flexindex")
		     [indextype 'hashindex size (* 4 1024 1024) create #t
		      register #t maxkeys (* 2 1024 1024)]))
  (set! wikidata.index
    (make-aggregate-index {words.index norms.index has.index props.index})))

(config-def! 'wikidata
  (lambda (var (val #f))
    (cond ((not val) wikidata.dir)
	  ((not (string? val)) (irritant val |NotADirectoryPath|))
	  ((and wikidata.dir
		(equal? (realpath wikidata.dir) (realpath val)))
	   wikidata.dir)
	  (wikidata.dir (irritant wikidata.dir |WikidataAlreadyConfigured|))
	  ((not (file-directory? val))
	   (irritant val |NotADirectoryPath|))
	  (else (setup-wikidata val)))))

(config-def! 'wikidata:build
  (lambda (var (val))
    (cond ((not (bound? val)) wikidata-build)
	  ((not val)
	   (set! wikidata-build #f)
	   (set! buildmap.table #f))
	  ((not wikidata.dir) (error |NoWikidataConfigured|))
	  (wikidata-build wikidata-build)
	  (else (set! buildmap.table
		  (flexdb/make (mkpath wikidata.dir "wikids.table")
			       [indextype 'memindex create #t]))
		(set! wikidata-build #t)))))

(define (wikidata/save!)
  (flexdb/commit! {wikidata.pool wikidata.index 
		   wikids.index buildmap.table
		   has.index}))

(define (wikidata/ref arg . ignored)
  (cond ((not wikidata.pool) (error |WikdataNotConfigured|))
	((oid? arg)
	 (if (or (in-pool? arg wikidata.pool) (test arg 'wikitype))
	     arg
	     (and (test arg 'wikidref)
		  (try (pickoids (get arg 'wikidref))
		       (tryif buildmap.table 
			 (get buildmap.table (get arg 'wikidref)))
		       (get wikids.index (get arg 'wikidref))
		       #f))))
	((string? arg)
	 (try (tryif buildmap.table (get buildmap.table (upcase arg)))
	      (get wikids.index (upcase arg))
	      #f))
	((symbol? arg)
	 (try (tryif buildmap.table (upcase (symbol->string arg)))
	      (get wikids.index (upcase (symbol->string arg)))
	      #f))))

(define wikid/ref wikidata/ref)

(defambda (wikidata/find . specs)
  (apply find-frames wikidata.index specs))

(define wikid/find wikidata/find)
