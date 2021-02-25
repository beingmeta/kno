;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'knodb/dbserv)

(use-module '{text/stringfmts logger texttools varconfig})

(module-export! '{fetchoid fetchoids fetchkey fetchkeys fetchsize
		  getload pool-data})

(define-init server-config #[])

(define-init strict-mode #t)

(define (fetchoid oid (dbarg #f) (config (srv/getconfig)) (pool))
  (unless (oid? oid) (irritant oid |NotAnOID|))
  (default! pool (getpool oid))
  (cond ((or (fail? pool) (not pool))
	 (irritant oid |UncoveredOID|))
	((not dbarg)
	 (if strict-mode
	     (if (or (test config 'pools pool) (not (test config 'pools)))
		 (oid-value oid)
		 (irritant oid |UncoveredOID|))
	     (oid-value oid)))
	((testopt config dbarg) (get (getopt config dbarg) oid))
	(else (let ((adj (get-adjunct pool dbarg)))
		(tryif adj (get adj oid))))))

(define (fetchoids oidvec (db #f) (config (srv/getconfig)))
  (doseq (oid oidvec) (unless (oid? oid) (irritant oid '|NotAnOID|)))
  (cond ((and db (testopt config db))
	 (let ((db (get config db)))
	   (forseq (oid oidvec) (get db oid))))
	(db
	 (let ((poolmap (frame-create #f)))
	   (doseq (oid oidvec)
	     (add! poolmap (or (get-adjunct oid db) {}) oid))
	   (do-choices (pool (getkeys poolmap))
	     (pool-prefetch! (get poolmap pool) pool)))
	 (forseq (oid oidvec) (adjunct-value oid db)))
	(else (prefetch-oids! oidvec)
	      (forseq (oid oidvec) (oid-value oid)))))

(define (pool-data (clientid #f) (config (srv/getconfig)))
  (try (getopt config 'pooldata {})
       (get-pool-data (getopt config 'pool {}))
       (get-pool-data (getopt config 'pools {}))))

(define (getload (oid #f) (config (srv/getconfig)))
  (if (and oid (oid? oid))
      (pool-load (getpool oid))
      (try (get (get config 'pool-data) 'load)
	   (pool-load (get config 'pool))
	   (pool-load (smallest (get config 'pools) pool-base)))))

(define (get-pool-data pool)
  (let ((adjuncts (dbctl pool 'adjuncts)))
    (frame-create #f
      'base (pool-base pool)
      'capacity (pool-capacity pool)
      'label (pool-label pool)
      'metadata #[]
      'adjuncts (getkeys (dbctl pool 'adjuncts))
      'readonly #t)))

(define (fetchkey key (dbarg #f) (config (srv/getconfig)) (index))
  (default! index (if dbarg (getopt config dbarg) (getopt config 'index)))
  (cond ((fail? index) (irritant dbarg |NoIndex|))
	((not (or (index? index) (table? index)))
	 (irritant dbarg |NotAnIndex|))
	((hashtable? index) (get index key))
	((pair? key) (find-frames index (car key) (cdr key)))
	(else (get index key))))

(define (fetchkeys keyvec (db #f) (config (srv/getconfig)) (index))
  (default! index (if dbarg (getopt config dbarg) (getopt config 'index)))
  (cond ((fail? index) (irritant dbarg |NoIndex|))
	((hashtable? index))
	((index? index) (index-prefetch! index keys))
	(else (irritant dbarg |NotAnIndex|)))
  (forseq (key keyvec)
    (if (pair? key)
	(find-frames index (car key) (cdr key))
	(get index key))))

(define (fetchcount key (dbarg #f) (config (srv/getconfig)) (index))
  (default! index (if dbarg (get config dbarg) (get config 'index)))
  (cond ((fail? index) (irritant dbarg |NoIndex|))
	((not (or (index? index) (table? index)))
	 (irritant dbarg |NotAnIndex|))
	((hashtable? index) (|| (get index key)))
	((index? index) (index-size index key))
	(else (|| (get index key)))))

(define (fetchsizes (dbarg #f) (keyvec #f) (config (srv/getconfig)) (index))
  (default! index (if dbarg (get config dbarg) (get config 'index)))
  (cond ((fail? index) (irritant dbarg |NoIndex|))
	((not keyvec)
	 (cond ((index? index) (index-sizes index))
	       ((hashtable? index)
		(let* ((keys (getkeys index))
		       (sizes (make-hashtable (|| keys))))
		  (do-choices (key keys)
		    (store! sizes key (|| (get index key))))
		  sizes))
	       (else (irritant dbarg |NotAnIndex|))))
	((hashtable? index)
	 (let ((sizes (make-hashtable (length keyvec))))
	   (doseq (key keys)
	     (store! sizes key (|| (get index key))))
	   sizes))
	((index? index) (index-sizes index keys))
	(else (irritant dbarg |NotAnIndex|))))

(define (listkeys (db #f) (config (srv/getconfig)) (index))
  (default! index (if dbarg (getopt config dbarg) (getopt config 'index)))
  (cond ((fail? index) (irritant dbarg |NoIndex|))
	((hashtable? index) (getkeys index))
	((pool? index) (pool-elts index))
	((index? index) (getkeys index))
	(else (irritant dbarg |NotAnIndex|))))

