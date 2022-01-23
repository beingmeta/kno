;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'knodb/branches)

(use-module '{ezrecords text/stringfmts logger varconfig fifo texttools})
(use-module '{knodb/adjuncts knodb/filenames knodb/flexindex})
(use-module '{knodb})

(module-export! '{index/branch branch/commit index-branch?})
;; Aliases
(module-export! '{branch/commit! branch/merge!
		  aggregate/branch aggregate/merge!
		  aggregate/merge})

(define-init %loglevel %warn%)

(define default-branchsize 16000)
(varconfig! aggregate:branchsize default-branchsize)

(define (index-branch? ix)
  (and (tempindex? ix) (indexctl ix 'props 'root)))

(define (keyslot->string keyslot partition)
  (if (symbol? keyslot)
      (downcase keyslot)
      #f))

(define (index/branch root (opts #f))
  (if (and (aggregate-index? root) (not (flexindex? root)))
      (let ((branches (index/branch (indexctl root 'partitions) opts)))
	(if (not (ambiguous? branches))
	    branches
	    (make-aggregate-index branches #[register #f shared #f])))
      (let* ((keyslot (indexctl root 'keyslot))
	     (branchsize (getopt opts 'branchsize default-branchsize))
	     (index-name
	      (glom (if (symbol? keyslot) (downcase keyslot)
			(basename (index-id root)))
		"-e" (->exact (elapsed-time))
		"-t" (threadid)
		"-r" (number->string (random 65536) 16)))
	     (ix (open-index index-name
		   `#[type tempindex register #f shared #f
		      keyslot ,keyslot
		      size ,(suggest-hash-size branchsize)])))
	(indexctl ix 'props 'root root)
	ix)))
(define aggregate/branch index/branch)

(define (branch/commit! branch (direct #f))
  (local root (indexctl branch 'props 'root))
  (if (aggregate-index? branch)
      (branch/commit! (indexctl branch 'partitions))
      (cond ((not (and (tempindex? branch) (exists? root) root))
	     (logwarn |NotBranchIndex|
	       "The index " branch " is not a branch index. Commiting normally...")
	     (commit branch))
	    ((not (identical? (indexctl root 'keyslot) (indexctl branch 'keyslot)))
	     (logwarn |InconsistentKeySlots| 
	       "\n root   " root   " " (indexctl root 'keyslot)
	       "\n branch " branch " " (indexctl branch 'keyslot))
	     (slotindex/merge! root branch))
	    ((flexindex? root)
	     (let* ((curfront (indexctl root 'props 'front))
		    (margin (change-load branch))
		    (front (flexindex/front root [margin margin])))
	       (when (exists? front)
		 (unless (identical? curfront front)
		   (lognotice |NewFront| "For margin of " margin " on " root
			      "\n  cur=" (indexctl curfront 'livecount) "/" (indexctl curfront 'capacity) "=" curfront
			      "\n  new=" (indexctl front 'livecount) "/" (indexctl front 'capacity) "=" front))
		 (if direct (index/save! front branch) (index/merge! front branch))
		 (indexctl front 'props 'reserved 0))))
	    (direct (index/save! root branch))
	    (else (logdebug |BranchMerge|
		    "Merging " (indexctl branch 'metadata 'adds) " key values " " into " root)
		  (index/merge! root branch)))))
(define branch/merge! branch/commit!)
(define aggregate/merge branch/commit!)
(define aggregate/merge! branch/commit!)
