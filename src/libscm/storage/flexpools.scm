;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

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

(define FLEXPOOL/PARTITIONS (within-module 'storage/flexpool FLEXPOOL/PARTITIONS))
(define FLEXPOOL/RECORD (within-module 'storage/flexpool FLEXPOOL/RECORD))
(define FLEXPOOL? (within-module 'storage/flexpool FLEXPOOL?))
(define FLEXPOOL/REF (within-module 'storage/flexpool FLEXPOOL/REF))
(define FLEXPOOL/OPEN (within-module 'storage/flexpool FLEXPOOL/OPEN))
(define FLEXPOOL/MAKE (within-module 'storage/flexpool FLEXPOOL/MAKE))
(define FLEXPOOL/ZERO (within-module 'storage/flexpool FLEXPOOL/ZERO))
(define FLEXPOOL/FRONT (within-module 'storage/flexpool FLEXPOOL/FRONT))
(define FLEXPOOL/LAST (within-module 'storage/flexpool FLEXPOOL/LAST))
(define FLEXPOOL/INFO (within-module 'storage/flexpool FLEXPOOL/INFO))
(define FLEXPOOL/PARTCOUNT (within-module 'storage/flexpool FLEXPOOL/PARTCOUNT))
(define FLEXPOOL/DELETE! (within-module 'storage/flexpool FLEXPOOL/DELETE!))
(define FLEXPOOL/ADJUNCT! (within-module 'storage/flexpool FLEXPOOL/ADJUNCT!))
(define FLEXPOOL/PADLEN (within-module 'storage/flexpool FLEXPOOL/PADLEN))
(define FLEX/ZERO (within-module 'storage/flexpool FLEX/ZERO))
(define FLEX/FRONT (within-module 'storage/flexpool FLEX/FRONT))
(define FLEX/LAST (within-module 'storage/flexpool FLEX/LAST))
(define FLEX/INFO (within-module 'storage/flexpool FLEX/INFO))
(define FLEXPOOL/SPLIT (within-module 'storage/flexpool FLEXPOOL/SPLIT))

