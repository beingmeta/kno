;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/indexes)

(use-module '{mongodb logger})

(module-export! '{mongodb/index})

(define (collection-index-fetchfn key.value collection)
  (get (mongodb/find collection `#[,(car key.value) ,(cdr key.value)]
	 `#[return #[_id #t]])
       '_id))

(define (mongodb/index collection (opts #f))
  (cons-extindex 
   (glom "index-" (collection/name collection) "@" (mongodb/spec collection))
   collection-index-fetchfn #f collection #t (opts+ opts 'register #t)))

