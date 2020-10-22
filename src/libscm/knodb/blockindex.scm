;;; -*- mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/blockindex)

;;; A type index is an index with a relatively small number of keys
;;; but a large number of values, where, for example, the keys
;;; represent types for objects (the values).

;;; The top level index structure of a type index index is a hashtable
;;; which is saved and restored from disk.  This table maps keys to
;;; value records, which identify a data file where the values for the
;;; key are stored as a series of DType representations. The value
;;; record contains a count of the number of values (possibly
;;; including duplicates) and the valid size of the values file.

(use-module '{ezrecords stringfmts logger varconfig texttools})
(use-module '{knodb knodb/countrefs})

(define-init %loglevel %warn%)

(module-export! '{blockindex/open blockindex/create})

(define-init blockindexes (make-hashtable))
(define-init blockindex-data (make-hashtable))

;;; BLockindex record structure

(define (blockindex->string ix)
  (stringout "#<BLOCKINDEX '" (blockindex-path ix)  "' "
    (if (blockindex-table ix)
	(printout ($count (table-size (blockindex-table ix)) "key"))
	"unloaded")
    ">"))

(defrecord (blockindex mutable opaque `(stringfn . blockindex->string))
  path (table #f) (metadata #f))

(define (write-dump dump path)
  (let ((freqs (make-hashtable)))
    (countrefs dump freqs)
    (let ((xtype (encode-xtype dump [xrefs (rsorted (table-skim freqs 2) freqs)]]))
      (gp/save! path xtype "application/kno+blockindex"))))

(define (read-blockindex path)
  (let* ((fetched (gp/fetch path))
	 (dump (decode-xtype fetched))
	 (table (get dump 'table))
	 (opts (get dump 'metadata)))
    (cons-blockindex path table metadata)))

;;; Opening blockindexes

(defsync (open-blockindex path opts create)
  (let* ((blockindex (if create
			 (cons-blockindex path table (getopt opts 'metadata #[]))
			 (read-blockindex path)))
	 (index (make-procindex path (cons #[type blockindex] opts)
				blockindex path "BLOCKINDEX")))
    (store! blockindex-data index blockindex)
    (store! blockindexes filename index)
    index))

(define (blockindex/open path (opts #f))
  (try (get blockindex path)
       (if (gp/exists? path)
	   (open-blockindex path opts #f)
	   (if (testopt opts 'create)
	       (open-blockindex path opts #t)
	       (irritant path |MissingBlockIndexPath|)))))

(define (blockindex/create path (opts #f))
  (try (get blockindexes path)
       (blockindex/open path (cons [create #t] opts))))

;;; Adding a key

(defambda (blockindex/add! blockindex key (init-value {}))
  (let ((existing (get (blockindex-keyinfo blockindex) key)))
    (if (exists? existing)
	(error |InternalBlockindexError| blockindex/add! 
	       blockindex)
	(let* ((filename (blockindex-filename blockindex))
	       (filecount (blockindex-filecount blockindex))
	       (valuepath (glom (blockindex-prefix blockindex)
			    "." (padnum filecount 4 16)
			    ".dtype"))
	       (fullpath (mkpath (dirname filename) valuepath))
	       (info (frame-create #f
		       'key key 'count (choice-size init-value)
		       'file valuepath)))
	  (loginfo |InitValue|
	    "Writing " ($num (choice-size init-value)) " values "
	    "to " valuepath " for " key)
	  (dtypes->file+ init-value fullpath)
	  (store! info 'size (file-size fullpath))
	  (store! info 'serial filecount)
	  (store! (blockindex-keyinfo blockindex) key info)
	  (set-blockindex-filecount! blockindex (1+ filecount))
	  valuepath))))

;;; Handlers

(define (blockindex-fetch index blockindex key)
  (get (blockindex-table blockindex) key))
(define (blockindex-fetchn index blockindex keyvec (table))
  (set! table (blockindex-table blockindex))
  (forseq (key kevec) (get table key)))

(define (blockindex-fetchsize index blockindex key)
  (|| (get (blockindex-table blockindex) key)))

(define (blockindex-fetchkeys index blockindex)
  (getkeys (blockindex-table blockindex)))

(define (blockindex-commit index blockindex phase adds drops stores (metadata #f))
  (info%watch "BLOCKINDEX-COMMIT" index phase adds drops stores)
  (if (overlaps? phase '{save write})
      (let ((table (blockindex-table blockindex)))
	(when stores
	  (do-choices (key (getkeys stores)) (store! table key (get stores key))))
	(when drops
	  (do-choices (key (getkeys drops)) (drop! table key (get drops key))))
	(when adds
	  (if drops
	      (let ((addkeys (getkeys adds)) 
		    (dropkeys (getkeys drops)))
		(do-choices (key (difference adds drops))
		  (add! table key (get adds key)))
		(do-choices (key (intersection adds drops))
		  (add! table key (difference (get adds key) (get drops key)))))
	      (do-choices (key (getkeys adds)) (add! table key (get adds key)))))
	(let ((dump [opts (blockindex-opts blockindex)
		     table table
		     metadata metadata])
	      (encoded (encode-xtype dump)))
	  (gp/save! (blockindex-path blockindex) encoded))
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
