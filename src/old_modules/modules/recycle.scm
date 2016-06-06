;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'recycle)

;;; Provides for the reuse of OIDs which are indexed as status: deleted.

(module-export!
 '{recycle-oids! get-recycled-oid update-status-index! newframe})

(define pool-queues (make-hashtable))

(define (recycle-oids! pool)
  (when (pool? pool)
    (let ((deleted (pick (?? 'status 'deleted) pool)))
      (store! pool-queues pool (rsorted deleted oid-lo)))))

(define oids-now-in-use {})

(define get-recycled-oid
  (slambda (pool)
    (let ((queue (get pool-queues pool)))
      (if (or (fail? queue) (eq? queue '())) (fail)
	  (let ((top (car queue)))
	    (set+! oids-now-in-use top)
	    (set! queue (cdr queue))
	    (lock-oids! top) (prefetch-oids! top)
	    (while (not (empty? (getkeys top)))
	      (if (null? queue)
		  (set! top (fail))
		  (begin (set! top (car queue))
			 (lock-oids! top)
			 (prefetch-oids! top)
			 (set! queue (cdr queue))
			 (set+! oids-now-in-use top))))
	    (store! pool-queues pool queue)
	    top)))))

(define (update-status-index! index)
  (drop! index (cons 'status 'deleted)
	 oids-now-in-use)
  (commit index))

(define (initframe oid slotvals)
  (unless (null? slotvals)
    (store! oid (car slotvals) (cadr slotvals))
    (initframe oid (cddr slotvals))))

(define newframe
  (ambda (pool . args)
    (if (or (not pool) (not (test pool-queues pool)))
	(apply frame-create pool args)
	(let ((top (get-recycled-oid pool)))
	  (if (exists? top)
	      (begin (apply initframe top args) top)
	      (apply frame-create pool args))))))

