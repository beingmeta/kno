;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/initwikid)

(use-module '{texttools reflection logger varconfig stringfmts knobase})

(module-export! '{wikid/init!})

(define-init %loglevel %notify%)
;;(set! %loglevel %debug%)

(define (get-index-templates)
  {[filename "wikid_core.index"
    indextype 'hashindex size (* 16 1024 1024)]
   [filename "wikid_isa.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot @?isa]
   [filename "wikid_genls.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot @?genls]
   [filename "wikid_partof.index"
    indextype 'hashindex size (* 16 1024 1024)]
   [filename "wikid_relations.index"
    indextype 'hashindex size (* 16 1024 1024)]
   [filename "wikid_termlogic.index"
    indextype 'hashindex size (* 16 1024 1024)]

   [filename "wikid_english.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (?? 'type 'language 'key 'en)]
   [filename "wikid_en_norms.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (?? 'type 'norm 'key 'en)]
   [filename "wikid_en_aliases.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (?? 'type 'aliases 'key 'en)]
   [filename "wikid_en_indicators.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (?? 'type 'indicators 'key 'en)]
   [filename "wikid_en_frags.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (?? 'type 'fragments 'key 'en)]

   [filename "wikid_words.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (difference (?? 'type 'language) (?? 'key 'en))]
   [filename "wikid_norms.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (difference (?? 'type 'norm) (?? 'key 'en))]
   [filename "wikid_aliases.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (difference (?? 'type 'aliases) (?? 'key 'en))]
   [filename "wikid_indicators.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (difference (?? 'type 'indicators) (?? 'key 'en))]
   [filename "wikid_frags.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot (difference (?? 'type 'fragments) (?? 'key 'en))]

   [filename "pending.index"
    indextype 'hashindex size (* 16 1024 1024)
    keyslot 'pending]})

(define (open-wikid-index spec dir)
  (kb/make (mkpath dir (get spec 'filename))
	       [indextype (getopt spec 'type 'hashindex)
		size (getopt spec 'size 'hashindex)
		size (getopt spec 'maxkeys (* 4 1024 1024))
		keyslot (getopt spec 'keyslot #f)
		create #t register #t]))

(define (wikid/init source)
  (set! wikid.pool
    (kb/make (mkpath source "wikid.pool")
		 [create #t type 'bigpool
		  base @1/8000000 capacity (* 4 1024 1024)
		  adjuncts #[%words #[pool "wikid_words"]
			     %norms #[pool "wikid_norms"]
			     %glosses #[pool "wikid_glosses"]
			     %aliases #[pool "wikid_aliases"]
			     %indicators #[pool "wikid_indicators"]]
		  reserve 1]))
  (let ((indexes (open-wikid-index (get-index-templates) source)))
    (set! wikid.index (make-aggregate-index indexes))))
