;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

;;; For compatability when storage/flexpools got moved to storage/flexpool

(in-module 'storage/flexpools)

(use-module 'storage/flexpool)

(module-export! '{FLEXPOOL/PARTITIONS
		  FLEXPOOL/RECORD
		  FLEXPOOL?
		  FLEXPOOL/REF
		  FLEXPOOL/OPEN
		  FLEXPOOL/MAKE
		  FLEXPOOL/ZERO
		  FLEXPOOL/FRONT
		  FLEXPOOL/LAST
		  FLEXPOOL/INFO
		  FLEXPOOL/PARTCOUNT
		  FLEXPOOL/DELETE!
		  FLEXPOOL/ADJUNCT!
		  FLEXPOOL/PADLEN
		  FLEX/ZERO
		  FLEX/FRONT
		  FLEX/LAST
		  FLEX/INFO
		  FLEXPOOL/SPLIT})

(define-import FLEXPOOL/PARTITIONS 'storage/flexpool)
(define-import FLEXPOOL/RECORD 'storage/flexpool)
(define-import FLEXPOOL? 'storage/flexpool)
(define-import FLEXPOOL/REF 'storage/flexpool)
(define-import FLEXPOOL/OPEN 'storage/flexpool)
(define-import FLEXPOOL/MAKE 'storage/flexpool)
(define-import FLEXPOOL/ZERO 'storage/flexpool)
(define-import FLEXPOOL/FRONT 'storage/flexpool)
(define-import FLEXPOOL/LAST 'storage/flexpool)
(define-import FLEXPOOL/INFO 'storage/flexpool)
(define-import FLEXPOOL/PARTCOUNT 'storage/flexpool)
(define-import FLEXPOOL/DELETE! 'storage/flexpool)
(define-import FLEXPOOL/ADJUNCT! 'storage/flexpool)
(define-import FLEXPOOL/PADLEN 'storage/flexpool)
(define-import FLEX/ZERO 'storage/flexpool)
(define-import FLEX/FRONT 'storage/flexpool)
(define-import FLEX/LAST 'storage/flexpool)
(define-import FLEX/INFO 'storage/flexpool)
(define-import FLEXPOOL/SPLIT 'storage/flexpool)

