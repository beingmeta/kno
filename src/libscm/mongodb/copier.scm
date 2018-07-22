;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/copier)

(use-module '{mongodb mongodb/pools})

(module-export! '{mongodb/copy-pool})

(use-module '{mongodb ezrecords varconfig engine logger})

;;; Copying pools

(define (mongodb/copy-pool input collection (slotinfo {}))
  (let ((base (pool-base input))
	(capacity (pool-capacity input))
	(load (pool-load input))
	(metadata (poolctl input 'metadata))
	(curinfo (mongodb/get collection "_pool"))
	(curmd (mongodb/get collection "_metadata")))
    (unless (exists? curinfo)
      (mongodb/insert! collection
	`#[_id "_pool" base ,base capacity ,capacity load ,load]))
    (store! metadata '_id "_metadata")
    (unless (exists? curmd)
      (mongodb/modify! collection #[_id "_metadata"] `#[$set ,metadata]))
    (engine/run
	(lambda (oid)
	  (let ((v (frame-create #f '_id oid)))
	    (do-choices (slotid (getkeys oid))
	      (store! v slotid (get oid slotid)))
	    (mongodb/modify! collection
		`#[_id ,oid] `#[$set ,v]
		`#[upsert #t new #t])))
	(pool-elts input))))

