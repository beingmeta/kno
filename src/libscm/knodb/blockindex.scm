;;; -*- mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/blockindex)

;;; A blockindex is a hashtable which is stored in a file or other location (it uses GPATH).  When
;;; opened, the file is read (as an xtype). It contains a simple table with 'metadata' and 'table'
;;; slots, that are then used to populate the blockindex structure which lies behind the procindex
;;; implementation.

(use-module '{ezrecords stringfmts logger varconfig gpath texttools})
(use-module '{knodb knodb/countrefs})

(define-init %loglevel %warn%)

(module-export! '{blockindex/open blockindex/create blockindex/data})
(module-export! '{blockindex/path blockindex/table blockindex/metadata})

(define-init blockindexes (make-hashtable))
(define-init blockindex-data (make-hashtable))

(define *xrefs-thresh* 2)
(varconfig! blockindex:xthresh *xrefs-thresh* config:positive)

;;; BLockindex record structure

(define (blockindex->string ix)
  (stringout "#<BLOCKINDEX '" (blockindex-path ix)  "' "
    (if (blockindex-table ix)
	(printout ($count (table-size (blockindex-table ix)) "key"))
	"unloaded")
    ">"))

(defrecord (blockindex mutable opaque `(stringfn . blockindex->string))
  path table metadata)

(define (write-dump dump path (thresh *xrefs-thresh*))
  (let ((freqs (make-hashtable)))
    (countrefs dump freqs)
    (let* ((xrefs (rsorted (table-skim freqs thresh) freqs))
	   (xtype (encode-xtype dump [xrefs xrefs])))
      (gp/save! path xtype "application/kno+blockindex"))))

(define (read-blockindex path)
  (let* ((fetched (gp/fetch path))
	 (dump (decode-xtype fetched))
	 (table (get dump 'table))
	 (metadata (or (try (get dump 'metadata) #[]) #[])))
    (cons-blockindex path table metadata)))

;;; Opening blockindexes

(defslambda (open-blockindex path opts create)
  (let* ((blockindex (if create
			 (cons-blockindex path (make-hashtable) (or (getopt opts 'metadata #[]) #[]))
			 (read-blockindex path)))
	 (index (make-procindex path (cons #[type blockindex] opts)
				blockindex path "BLOCKINDEX")))
    (store! blockindex-data index blockindex)
    (store! blockindexes path index)
    (store! blockindexes (gpath->string path) index)
    index))

(define (blockindex/open path (opts #f))
  (try (get blockindexes path)
       (get blockindexes (gpath->string path))
       (if (gp/exists? path)
	   (open-blockindex path opts #f)
	   (if (testopt opts 'create)
	       (open-blockindex path opts #t)
	       (irritant path |MissingBlockIndexPath|)))))

(define (blockindex/create path (opts #f))
  (try (get blockindexes path)
       (blockindex/open path (cons [create #t] opts))))

(define (blockindex/data ix) (try (get blockindex-data ix) #f))
(define (blockindex/path arg)
  (cond ((blockindex? arg) (blockindex-path arg))
	((and (index? arg) (test blockindex-data arg))
	 (blockindex-path (get blockindex-data arg)))
	(else (irritant arg |NotBlockIndex|))))
(define (blockindex/table arg)
  (cond ((blockindex? arg) (blockindex-table arg))
	((and (index? arg) (test blockindex-data arg))
	 (blockindex-table (get blockindex-data arg)))
	(else (irritant arg |NotBlockIndex|))))
(define (blockindex/metadata arg)
  (cond ((blockindex? arg) (blockindex-metadata arg))
	((and (index? arg) (test blockindex-data arg))
	 (blockindex-metadata (get blockindex-data arg)))
	(else (irritant arg |NotBlockIndex|))))

;;; Handlers

(define (blockindex-fetch index blockindex key)
  (get (blockindex-table blockindex) key))
(define (blockindex-fetchn index blockindex keyvec (table))
  (set! table (blockindex-table blockindex))
  (forseq (key keyvec) (get table key)))

(define (blockindex-fetchsize index blockindex key)
  (|| (get (blockindex-table blockindex) key)))

(define (blockindex-fetchkeys index blockindex)
  (getkeys (blockindex-table blockindex)))

(define (blockindex-commit index blockindex phase adds drops stores (metadata #f))
  (if (overlaps? phase '{save write})
      (let ((table (blockindex-table blockindex)))
	(when (and stores (> (table-size stores)  0))
	  (do-choices (key (getkeys stores)) (store! table key (get stores key))))
	(when (and drops (> (table-size drops) 0))
	  (do-choices (key (getkeys drops)) (drop! table key (get drops key))))
	(when (and adds (> (table-size adds) 0))
	  (if (and drops (> (table-size drops) 0))
	      (let ((addkeys (getkeys adds)) 
		    (dropkeys (getkeys drops)))
		(do-choices (key (difference adds drops))
		  (add! table key (get adds key)))
		(do-choices (key (intersection adds drops))
		  (add! table key (difference (get adds key) (get drops key)))))
	      (do-choices (key (getkeys adds)) (add! table key (get adds key)))))
	(write-dump [table table
		     metadata metadata]
		    (blockindex-path blockindex)
		    (try (get (blockindex-metadata blockindex) 'xthresh)
			 *xrefs-thresh*))
	(+ (if adds (table-size adds) 0)
	   (if drops (table-size drops) 0)
	   (if stores (table-size stores) 0)))
      #f))

(defindextype 'blockindex
  `#[open ,blockindex/open
     create ,blockindex/create
     fetch ,blockindex-fetch
     fetchn ,blockindex-fetchn
     fetchkeys ,blockindex-fetchkeys
     commit ,blockindex-commit])
