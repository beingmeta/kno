;;; -*- mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/memindex)

;;; A memindex is a hashtable which is stored in a file or other location (it uses GPATH).  When
;;; opened, the file is read (as an xtype). It contains a simple table with 'metadata' and 'table'
;;; slots, that are then used to populate the memindex structure which lies behind the procindex
;;; implementation.

(use-module '{ezrecords text/stringfmts logger varconfig gpath texttools})
(use-module '{knodb knodb/countrefs})

(define-init %loglevel %warn%)

(module-export! '{memindex/open memindex/create memindex/data})
(module-export! '{memindex/path memindex/table memindex/metadata})

(define-init memindexes (make-hashtable))
(define-init memindex-data (make-hashtable))

(define *xrefs-thresh* 2)
(varconfig! memindex:xthresh *xrefs-thresh* config:positive)

;;; Memindex record structure

(define (memindex->string ix)
  (stringout "#<MEMINDEX '" (memindex-path ix)  "' "
    (if (memindex-table ix)
	(printout ($count (table-size (memindex-table ix)) "key"))
	"unloaded")
    ">"))

(defrecord (memindex mutable opaque `(stringfn . memindex->string))
  path table metadata)

(define (write-dump dump path (thresh *xrefs-thresh*))
  (let ((freqs (make-hashtable)))
    (countrefs dump freqs)
    (let* ((xrefs (rsorted (table-skim freqs thresh) freqs))
	   (xtype (encode-xtype dump [xrefs xrefs])))
      (gp/save! path xtype "application/kno+memindex"))))

(define (read-memindex path)
  (let* ((fetched (gp/fetch path))
	 (dump (decode-xtype fetched))
	 (table (get dump 'table))
	 (metadata (or (try (get dump 'metadata) #[]) #[])))
    (cons-memindex path table metadata)))

;;; Opening memindexes

(defslambda (open-memindex path opts create)
  (let* ((memindex (if create
			 (cons-memindex path (make-hashtable) (or (getopt opts 'metadata #[]) #[]))
			 (read-memindex path)))
	 (index (make-procindex path (cons #[type memindex] opts)
				memindex path "MEMINDEX")))
    (store! memindex-data index memindex)
    (store! memindexes path index)
    (store! memindexes (gpath->string path) index)
    index))

(define (memindex/open path (opts #f))
  (try (get memindexes path)
       (get memindexes (gpath->string path))
       (if (gp/exists? path)
	   (open-memindex path opts #f)
	   (if (testopt opts 'create)
	       (open-memindex path opts #t)
	       (irritant path |MissingMemindexPath|)))))

(define (memindex/create path (opts #f))
  (try (get memindexes path)
       (memindex/open path (cons [create #t] opts))))

(define (memindex/data ix) (try (get memindex-data ix) #f))
(define (memindex/path arg)
  (cond ((memindex? arg) (memindex-path arg))
	((and (index? arg) (test memindex-data arg))
	 (memindex-path (get memindex-data arg)))
	(else (irritant arg |NotMemindex|))))
(define (memindex/table arg)
  (cond ((memindex? arg) (memindex-table arg))
	((and (index? arg) (test memindex-data arg))
	 (memindex-table (get memindex-data arg)))
	(else (irritant arg |NotMemindex|))))
(define (memindex/metadata arg)
  (cond ((memindex? arg) (memindex-metadata arg))
	((and (index? arg) (test memindex-data arg))
	 (memindex-metadata (get memindex-data arg)))
	(else (irritant arg |NotMemindex|))))

;;; Handlers

(define (memindex-fetch index memindex key)
  (get (memindex-table memindex) key))
(define (memindex-fetchn index memindex keyvec (table))
  (set! table (memindex-table memindex))
  (forseq (key keyvec) (get table key)))

(define (memindex-fetchsize index memindex key)
  (|| (get (memindex-table memindex) key)))

(define (memindex-fetchkeys index memindex)
  (getkeys (memindex-table memindex)))

(define (memindex-commit index memindex phase adds drops stores (metadata #f))
  (if (overlaps? phase '{save write})
      (let ((table (memindex-table memindex)))
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
		    (memindex-path memindex)
		    (try (get (memindex-metadata memindex) 'xthresh)
			 *xrefs-thresh*))
	(+ (if adds (table-size adds) 0)
	   (if drops (table-size drops) 0)
	   (if stores (table-size stores) 0)))
      #f))

(defindextype 'memindex
  `#[open ,memindex/open
     create ,memindex/create
     fetch ,memindex-fetch
     fetchn ,memindex-fetchn
     fetchkeys ,memindex-fetchkeys
     commit ,memindex-commit])
