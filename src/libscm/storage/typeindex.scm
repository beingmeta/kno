;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/typeindex)

;;; A type index is an index with a relatively small number of keys
;;; but a large number of values, where, for example, the keys
;;; represent types for objects (the values).

;;; The top level index structure of a type index index is a hashtable
;;; which is saved and restored from disk.  This table maps keys to
;;; value records, which identify a data file where the values for the
;;; key are stored as a series of DType representations. The value
;;; record contains a count of the number of values (possibly
;;; including duplicates) and the valid size of the values file.

(use-module '{ezrecords stringfmts logger varconfig fifo texttools})
(use-module '{storage/adjuncts storage/filenames})
(use-module '{storage/flex})

(define-init %loglevel %notify%)

(module-export! '{typeindex/open})

(message "Loading from dev")

;;; Flexpool records

(define (typeindex->string ix)
  (stringout "#<TYPEINDEX " (typeindex-filename ix) 
    " " (write (mkpath (typeindex-prefix ix) "*")) " "
    " " (table-size (typeindex-keyinfo ix)) " keys>"))

(defrecord (typeindex mutable opaque `(stringfn . typeindex->string))
  filename prefix (opts) (keyinfo) (filecount 0))

(define-init typeindex-data (make-hashtable))
(define-init typeindexes (make-hashtable))

;;; Opening typeindexes

(define (typeindex/open filename (opts #f))
  (try (get typeindexes filename)
       (let ((keyinfo (if (file-exists? filename)
			  (file->dtype filename)
			  (init-typeindex filename)))
	     (prefix (textsubst (basename filename) #("." (not> "/") (eos)) ""))
	     (filecount 0))
	 (do-choices (key (getkeys keyinfo))
	   (let ((v (get keyinfo key)))
	     (if (and (test v 'serial) (number? (get v 'serial)))
		 (if (> (get v'serial) filecount) 
		     (set! filecount (get v 'serial)))
		 (irritant v |InvalidKeyInfo|))))
	 (let* ((typeindex (cons-typeindex filename prefix opts keyinfo filecount))
		(index (make-procindex (realpath filename)
				       (make-typeindex-opts opts)
				       typeindex
				       filename
				       "TYPEINDEX")))
	   (store! typeindex-data index typeindex)
	   (store! typeindexes filename index)
	   index))))

(define (init-typeindex filename)
  (let ((keyinfo (make-hashtable)))
    (dtype->file keyinfo filename)
    keyinfo))

(define (make-typeindex-opts opts)
  (cons `#[fetch ,typeindex-fetch
	   fetchkeys ,typeindex-fetchkeys
	   save ,typeindex-save] 
	opts))

;;; Adding a key

(defambda (typeindex/add! typeindex key (init-value {}))
  (let ((existing (get (typeindex-keyinfo typeindex) key)))
    (if (exists? existing)
	(error |InternalTypeindexError| typeindex/add! 
	       typeindex)
	(let* ((filename (typeindex-filename typeindex))
	       (filecount (typeindex-filecount typeindex))
	       (valuepath (glom (typeindex-prefix typeindex)
			    "." (padnum filecount 4 16)
			    ".dtype"))
	       (fullpath (mkpath (dirname filename) valuepath))
	       (info (frame-create #f
		       'key key 'count (choice-size init-value)
		       'file valuepath)))
	  (logwarn |InitValue|
	    "Writing " ($num (choice-size init-value)) " values "
	    "to " valuepath " for " key)
	  (dtypes->file+ init-value fullpath)
	  (store! info 'size (file-size fullpath))
	  (store! info 'serial filecount)
	  (store! (typeindex-keyinfo typeindex) key info)
	  (set-typeindex-filecount! typeindex (1+ filecount))
	  valuepath))))

;;; Reading a value

(define (read-value info key typeindex (filename))
  (set! pathname (mkpath (dirname (typeindex-filename typeindex))
			 (get info 'file)))
  (if (file-exists? pathname)
      (let ((disk-size (file-size pathname))
	    (expected-size (get info 'size)))
	;; This should truncate eventually
	(file->dtypes pathname))
      (begin (logwarn |MissingValue|
	       "The value file " pathname " for " key " in " typeindex " "
	       "does not exist")
	(fail))))

;;; Handlers

(define (typeindex-fetch index typeindex key (info) (file))
  (set! info (get (typeindex-keyinfo typeindex) key))
  (tryif (exists? info)
    (let ((v (file->dtypes 
	      (mkpath (dirname (typeindex-filename typeindex))
		      (get info 'file)))))
      (store! info 'count (choice-size v))
      v)))

(define (typeindex-fetchsize index typeindex key (info))
  (set! info (get (typeindex-keyinfo typeindex) key))
  (if (fail? info) 0
      (if (test info 'count)
	  (get info 'count)
	  (let ((v (file->dtypes (mkpath (dirname (typeindex-filename typeindex))
					 (get info 'file)))))
	    (store! info 'count (choice-size v))
	    (choice-size v)))))

(define (typeindex-fetchkeys index typeindex)
  (getkeys (typeindex-keyinfo typeindex)))

(define (typeindex-save index typeindex adds drops stores (metadata #f))
  (let ((keyinfo (typeindex-keyinfo typeindex))
	(savedir (dirname (typeindex-filename typeindex))))
    (when stores
      (do-choices (key (getkeys stores))
	(if (test keyinfo key)
	    (let ((filename (mkpath savedir (get keyinfo 'file))))
	      (ftruncate filename 0)
	      (dtype->file+ (get stores key) filename))
	    (typeindex/add! typeindex key (get stores key)))))
    (when drops
      (do-choices (key (getkeys drops))
	(when (test keyinfo key)
	  (let* ((info (get keyinfo key))
		 (file (get info 'file))
		 (fullpath (mkpath savedir file))
		 (current (tryif (file-exists? fullpath)
			    (file->dtypes fullpath))))
	    (move-file fullpath (glom fullpath ".bak"))
	    (dtypes->file+ (difference (choice current (get adds key))
				       (get drop(mkpath savedir file)s key))
			   fullpath)))))
    (when adds
      (do-choices (key (difference (getkeys adds) (tryif drops (getkeys drops))))
	(if (test keyinfo key)
	    (let* ((info (get keyinfo key))
		   (file (get info 'file))
		   (dir (dirname (typeindex-filename typeindex)))
		   (fullpath (mkpath dir file))
		   (values (get adds key)))
	      (dtype->file+ values fullpath)
	      (store! info 'count (+ (get info 'count) (choice-size values)))
	      (store! info 'size (file-size fullpath)))
	    (typeindex/add! typeindex key (get adds key)))))
    (when metadata
      (dtype->file metadata 
		   (glom (textsubst (typeindex-filename typeindex) #("." (rest)) "")
		     ".metadata.dtype")))
    (dtype->file keyinfo (typeindex-filename typeindex))
    (+ (if adds (table-size adds) 0)
       (if drops (table-size drops) 0)
       (if stores (table-size stores) 0))))










