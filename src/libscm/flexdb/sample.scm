#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(config! 'bricosource (get-component "brico"))

(use-module '{logger webtools varconfig libarchive texttools stringfmts optimize})
(use-module '{flexdb flexdb/branches flexdb/typeindex})
;; (use-module 'brico)

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'dbloglevel %warn%)

(define inbufsize (* 1024 1024 3))
(varconfig! inbufsize inbufsize)

(define %loglevel %notice%)

(define wikidata.pool
  (flexdb/make "wikidata/wikidata.flexpool"
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
  (flexdb/make "wikidata/wikidata.00.pool"
	       [create #t type 'bigpool
		base @31c1/0 capacity (* 1024 1024)
		adjuncts #[labels #[pool "labels"]
			   aliases #[pool "aliases"]
			   claims #[pool "claims"]
			   sitelinks #[pool "sitelinks"]]]))
|#
