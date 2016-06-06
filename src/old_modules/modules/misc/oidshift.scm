;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'misc/oidshift)

;;; Moving OIDs around for big DB updates

(use-module '{optimize mttools varconfig logger})

(module-export! '{os/convert os/convert-oid os/convert-pool os/convert-file})

;;;; Constants

(define M1 (* 1024 1024))
(define M32 (* 32 1024 1024))
(define M16 (* 16 1024 1024))

(define conversion-base @2/0)
(define conversion-range (* 2 M32))
(define conversion-target (oid-plus @1/0 M16))

(varconfig! os/base conversion-base)
(varconfig! os/range conversion-range)
(varconfig! os/target conversion-target)

;;;; Conversion

(define (os/convert value)
  (if (oid? value)
      (os/convert-oid value)
      (if (pair? value)
	  (cons (qc (os/convert (car value)))
		(qc (os/convert (cdr value))))
	  (if (vector? value)
	      (map os/convert value)
	      (if (haskeys? value)
		  (if (hashtable? value)
		      (convert-hashtable value)
		      (if (hashset? value)
			  (choice->hashset (os/convert (hashset-elts value)))
			  (let ((newtable (frame-create #f)))
			    (do-choices (key (getkeys value))
			      (store! newtable
				      (os/convert key)
				      (os/convert (get value key))))
			    newtable)))
		  value)))))

(define (os/convert-oid oid)
  (let ((offset (oid-offset oid conversion-base)))
    (if (and offset (> offset 0) (< offset conversion-range))
	(oid-plus conversion-target offset)
	oid)))

(define (convert-prefetch oids done)
  (when done (commit) (clearcaches))
  (unless done
    (prefetch-oids! oids)
    (let ((new-oids (os/convert-oid oids)))
      (prefetch-oids! (difference new-oids oids))
      (lock-oids! new-oids))))

(define (os/convert-pool pool)
  (do-choices-mt (f (pool-elts pool) (config 'nthreads 4)
		    convert-prefetch 100000)
    (let* ((v (oid-value f))
	   (new-oid (os/convert-oid f))
	   (new-value (os/convert v)))
      (unless (identical? v new-value)
	(set-oid-value! new-oid new-value)))))

(define (convert-hashtable table)
  (let ((newtable (make-hashtable)))
    (do-choices-mt (f (getkeys table) (config 'nthreads 4))
      (add! newtable (os/convert f) (os/convert (get table f))))
    newtable))

(define (os/convert-file filename)
  (let ((content (file->dtype filename)))
    (logger %notice! "Converting " (file-size filename) " bytes in "filename)
    (let ((newcontent (os/convert content)))
      (logger %notice! "Converted content of " filename)
      (let ((newlen (dtype->file newcontent filename 100000)))
	(logger %notice! "Wrote " newlen " bytes to " filename)))))

(comment
 (make-oidpool "newdata/xbrico.pool" new-xbrico-base
	       (pool-capacity xbrico-pool) (pool-load xbrico-pool)
	       #[] #() #[]
	       "xbrico.beingmeta.com")
 (make-oidpool "newdata/places.pool" new-places-base
	       (pool-capacity places-pool) (pool-load places-pool)
	       #[] #() #[]
	       "placedb.beingmeta.com")
 (make-oidpool "newdata/names.pool" new-names-base
	       (pool-capacity names-pool) (pool-load names-pool)
	       #[] #() #[]
	       "namedb.beingmeta.com"))

(comment
 (define new-xbrico-pool (use-pool "newdata/xbrico.pool"))
 (define new-names-pool (use-pool "newdata/names.pool"))
 (define new-places-pool (use-pool "newdata/places.pool")))

