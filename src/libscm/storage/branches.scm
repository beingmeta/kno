;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'storage/branches)

(use-module '{ezrecords stringfmts logger varconfig fifo texttools})
(use-module '{storage/adjuncts storage/filenames})
(use-module '{storage/flex})

(module-export! '{index/branch branch/commit index-branch?})
;; Aliases
(module-export! '{branch/commit! branch/merge!
		  aggregate/branch aggregate/merge!
		  aggregate/merge})

(define-init %loglevel %notice%)

(define default-branchsize 16000)
(varconfig! aggregate:branchsize default-branchsize)

(define (index-branch? ix)
  (and (tempindex? ix) (indexctl ix 'props 'root)))

(define (keyslot->string keyslot partition)
  (if (symbol? keyslot)
      (downcase keyslot)
      #f))

(define (index/branch root (opts #f))
  (if (aggregate-index? root)
      (if (exists? (indexctl root 'props 'front))
	  (index/branch (indexctl root 'props 'front) opts)
	  (let ((branches (index/branch (indexctl root 'partitions) opts)))
	    (if (not (ambiguous? branches))
		branches
		(make-aggregate-index branches #[register #f]))))
      (let* ((keyslot (indexctl root 'keyslot))
	     (branchsize (getopt opts 'branchsize default-branchsize))
	     (index-name
	      (glom (if (symbol? keyslot) (downcase keyslot)
			(basename (index-id root))	)
		"-e" (->exact (elapsed-time))
		"-t" (threadid)
		"-r" (number->string (random 65536) 16)))
	     (ix (open-index index-name
		   `#[type tempindex register #f
		      keyslot ,keyslot
		      size ,(suggest-hash-size branchsize)])))
	(indexctl ix 'props 'root root)
	ix)))
(define aggregate/branch index/branch)

(define (branch/commit! branch (root))
  (set! root (indexctl branch 'props 'root))
  (if (aggregate-index? branch)
      (branch/commit! (indexctl branch 'partitions))
      (if (and (tempindex? branch) root)
	  (if (indexctl root 'keyslot)
	      (slotindex/merge! root branch)
	      (index/merge! root branch))
	  (irritant branch |Not A Branch Index| branch/merge!))))
(define branch/merge! branch/commit!)
(define aggregate/merge branch/commit!)
(define aggregate/merge! branch/commit!)
