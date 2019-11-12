#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(config! 'bricosource (get-component "brico"))

(use-module '{logger logctl webtools varconfig libarchive texttools stringfmts optimize})
(use-module '{knobase knobase/branches knobase/typeindex})
;; (use-module 'brico)

(begin (logctl! 'knobase %debug%)
  (logctl! 'knobase/flexpool %debug%)
  (logctl! 'knobase/adjuncts %debug%))

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'dbloglevel %warn%)

(define inbufsize (* 1024 1024 3))
(varconfig! inbufsize inbufsize)

(define %loglevel %notice%)

(define wikidata-root (get-component "wikidata"))
(unless (file-directory? wikidata-root) (mkdir wikidata-root))
(define wikidata-pools-root (mkpath wikidata-root "pools"))
(unless (file-directory? wikidata-pools-root) (mkdir wikidata-pools-root))
(define (reset)
  (remove-file (getfiles {wikidata-root (mkpath wikidata-root "pools")})))

(define wikidata.pool
  (knobase/make "wikidata/wikidata.flexpool"
	       [create #t type 'flexpool
		base @31c1/0 capacity (* 128 1024 1024)
		partsize (* 1024 1024) partition-type 'bigpool
		prefix "pools/"
		adjuncts #[labels #[pool "labels"]
			   aliases #[pool "aliases"]
			   claims #[pool "claims"]
			   sitelinks #[pool "sitelinks"]]]))

#|
(define wikidata.pool
  (knobase/make "wikidata/wikidata.pool"
	       [create #t type 'bigpool
		base @31c1/0 capacity (* 1024 1024)
		adjuncts #[labels #[pool "labels"]
			   aliases #[pool "aliases"]
			   claims #[pool "claims"]
			   sitelinks #[pool "sitelinks"]]]))
|#


