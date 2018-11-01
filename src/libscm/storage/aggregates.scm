;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'storage/aggregates)

(use-module '{ezrecords stringfmts logger varconfig fifo texttools})
(use-module '{storage/adjuncts storage/filenames})
(use-module '{storage/flex})

(module-export! '{aggregate/branch aggregate/merge! aggregate/merge})

(define-init %loglevel %notice%)

(define default-branchsize 16000)
(varconfig! aggregate:branchsize default-branch-size)

(define (keyslot->string keyslot partition)
  (if (symbol? keyslot)
      (downcase keyslot)
      #f))

(define (aggregate/branch root (opts #f))
  (let ((partitions
	 (for-choices (partition (indexctl root 'partitions))
	   (let* ((keyslot (indexctl partition 'keyslot))
		  (branchsize (getopt opts 'branchsize default-branchsize))
		  (ix (open-index 
			  (glom
			    (if (symbol? keyslot) (downcase keyslot)
				(basename (index-id partition))	)
			    "-t" (threadid))
			`#[type tempindex register #f
			   keyslot ,keyslot
			   size ,(suggest-hash-size branchsize)])))
	     (indexctl ix 'props 'root partition)
	     ix))))
    (make-aggregate-index partitions #[register #f])))

(define (aggregate/merge! branch)
  (do-choices (partition (indexctl branch 'partitions))
    (let ((root (indexctl partition 'props 'root)))
      (slotindex/merge! root partition))))
(define aggregate/merge aggregate/merge!)
