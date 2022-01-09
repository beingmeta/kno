;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/indexes)

(use-module '{fifo engine text/stringfmts logger varconfig})
(use-module '{knodb/hashindexes})

(define-init %loglevel %notice%)

(module-export! '{index/copy! index/pack! index/merge!
		  index/copy-keys!
		  index/install!
		  merge-index})

;;; Top level functions

(define (index/copy! from to (opts #f))
  (logdebug |Index/Copy| from " ==> " to)
  (let ((out (if (index? to) to
		 (if (string? to)
		     (open-index to (get-write-opts opts))
		     (irritant from |NotIndex| index/copy!)))))
    (do-choices from 
      (let ((in (if (index? from) from
		    (if (string? from)
			(open-index from (get-read-opts opts))
			(irritant from |NotIndex| index/copy!)))))
	(cond ((overlaps? (indexctl in 'metadata 'type) {"kindex" "hashindex"})
	       (hashindex/copy-keys! in out opts))
	      (else (index/copy-keys! in out opts)))))))

(define (index/pack! from (to #f) (opts #f))
  (logdebug |Index/Pack| from " ==> " to)
  (let* ((in (if (index? from) from
		 (if (string? from)
		     (open-index from (get-read-opts opts))
		     (irritant from |NotIndex| index/copy!))))
	 (infile (if (string? from) from (index-source in)))
	 (outfile (if to
		      (if (index? to) (index-source to)
			  to)
		      (index-source in)))
	 (inplace (and (file-exists? outfile)
		       (equal? (realpath infile) (realpath outfile))))
	 (out (if (and (file-exists? outfile) (not inplace)
		       (not (getopt opts 'overwrite (config 'OVERWRITE #f))))
		  (irritant outfile |OutputAlreadyExists|)
		  (get-new-index outfile in opts)))
	 (copier (cond ((testopt opts 'copier 'generic) index/copy-keys!)
		       ((getopt opts 'copier) (getopt opts 'copier))
		       ((overlaps? (indexctl in 'metadata 'type) {"kindex" "hashindex"})
			hashindex/copy-keys!)
		       (else index/copy-keys!)))
	 (ok #f))
    (onerror (begin (copier in out #f) (set! ok #t)))
    (when ok
      (index/install! out outfile))
    ok))

(define (merge-index in out (opts #f) (tail) (mincount))
  (default! tail (getopt opts 'tail))
  (default! mincount (getopt opts 'mincount (and tail 1)))
  (let* ((copy-opts `(#[tail ,tail mincount ,mincount] . ,opts)) (ok #f))
    (lognotice |MergeIndexes| "Merging " (index-source in))
    (let ((copier (cond ((testopt opts 'copier 'generic) index/copy-keys!)
			((getopt opts 'copier) (getopt opts 'copier))
			((overlaps? (indexctl in 'metadata 'type) {"hashindex" "kindex"})
			 hashindex/copy-keys!)
			(else index/copy-keys!))))
      (copier in out copy-opts))
    (close-index in))
    ;; (indexctl out 'metadata 'merges
    ;; 	      (qchoice (indexctl out 'metadata 'merges)
    ;; 		       `#[from ,(if (string? from) from
    ;; 				    (index-source from)from)
    ;; 			  tail ,tailfile
    ;; 			  timestamp ,(gmtimestamp)
    ;; 			  session ,(config 'sessionid)
    ;; 			  mincount ,(getopt opts 'mincount)
    ;; 			  maxcount ,(getopt opts 'mincount)]))
  in)

(defambda (index/merge! from outfile (opts #f))
  (let* ((tailfile (getopt opts 'tailfile))
	 (merge (open-index from opts))
	 (out (if (file-exists? outfile)
		  (open-index outfile opts)
		  (get-new-index outfile merge opts)))
	 (tail (and tailfile
		    (if (file-exists? tailfile)
			(open-index tailfile opts)
			(get-new-index tailfile merge opts))))
	 (copy-opts `(#[tail ,tail] . ,opts))
	 (ok #f))
    (onerror 
	(begin
	  (do-choices (in merge)
	    (lognotice |MergeIndexes| "Merging " (index-source in))
	    (let ((copier (cond ((testopt opts 'copier 'generic) index/copy-keys!)
				((getopt opts 'copier) (getopt opts 'copier))
				((overlaps? (indexctl in 'metadata 'type) {"hashindex" "kindex"})
				 hashindex/copy-keys!)
				(else index/copy-keys!))))
	      (copier in out copy-opts))
	    (close-index in))
	  (set! ok #t)))
    (indexctl out 'metadata 'merges
	      (qchoice (indexctl out 'metadata 'merges)
		       `#[from ,(if (string? from) from
				    (index-source from)from)
			  tail ,tailfile
			  timestamp ,(gmtimestamp)
			  session ,(config 'sessionid)
			  mincount ,(getopt opts 'mincount)
			  maxcount ,(getopt opts 'mincount)]))
    (when ok
      ;; Handle copying of any files in temporary locations
      (index/install! out outfile)
      (when tail (index/install! tail tailfile)))
    ok))

(define (index/install! index file)
  (logdebug |Index/Install| index " ==> " file)
  (close-index index)
  (unless (equal? (realpath (index-source index)) (realpath file))
    (when (file-exists? file)
      (if (config 'unsafe #f)
	  (remove-file file)
	  (move-file! file (glom file ".bak"))))
    (onerror
	(move-file! (index-source index) file)
	(lambda (x)
	  (logwarn |RenameFailed|
	    "Couldn't rename " (index-source index) " to " file ", using shell")
	  (exec/cmd "mv" (index-source index) file)))))

;;;; Support functions

(define default-index-size 31415)
(define default-minsize 100)
(varconfig! INDEX:MINSIZE default-minsize)
(varconfig! INDEX:SIZE default-index-size)

(define (get-keycount index)
  (let ((v (indexctl index 'keycount)))
    (unless (and (exists? v) v)
      (set! v (indexctl index 'load)))
    (unless (and (exists? v) v)
      (set! v (indexctl index 'metadata 'keys)))
    (if (and (exists? v) v) v #f)))

(define (get-new-size old opts (oldsize))
  (let* ((base-count (or (get-keycount old)
			 (getopt opts 'minsize default-index-size)))
	 (maxload (getopt opts 'maxload (config 'maxload 1.5)))
	 (newsize (max (getopt opts 'newsize (config 'newsize 0))
		       (* (+ base-count (getopt opts 'addkeys 0)) maxload))))
    (info%watch old base-count maxload newsize "\nOPTS" opts)
    (->exact newsize)))
(define (get-new-type old opts)
  (getopt opts 'type
	  (config 'NEWTYPE 
		  (config 'TYPE 
			  (or (indexctl old 'metadata 'type)
			      'kindex)))))

(define (get-new-index filename old opts)
  (if (and (file-exists? filename)
	   (not (equal? (realpath filename) 
			(realpath (index-source old))))
	   (not (getopt opts 'overwrite)))
      (begin (logwarn |Existing|
	       "Using existing output file index " filename)
	(open-index filename (get-write-opts opts)))
      (let* ((n-keys (indexctl old 'keycount))
	     (size (getopt opts 'newsize (get-new-size old opts)))
	     (type (get-new-type old opts))
	     (new-opts (frame-create #f
			 'type type 'size size
			 'register (getopt opts 'register #t)
			 'keyslot (indexctl old 'metadata 'keyslot)))
	     (new (make-index (glom filename ".part") (cons new-opts opts))))
	(logwarn |NewIndex|
	  "Created new " type " " filename "(.part) "
	  "with size " ($num size))
	new)))

(define (get-read-opts opts)
  `(#[readonly #t
      cachelevel ,(getopt opts 'cachelevel (config 'force:cachelevel 2))
      register ,(getopt opts 'register #t)
      repair (getopt opts 'repair (config 'repair #f))]
    . ,opts))
(define (get-write-opts opts)
  `(#[readonly #f
      cachelevel ,(getopt opts 'cachelevel (config 'force:cachelevel 2))
      register ,(getopt opts 'register #t)
      repair (getopt opts 'repair (config 'repair #f))]
    . ,opts))

;;; Default key copier, uses fetchn

(define (key-copier keys batch-state loop-state task-state)
  (let* ((tail (try (get loop-state 'tail) #f))
	 (mincount (get loop-state 'mincount))
	 (tailcount (and tail (get loop-state 'tailcount)))
	 (maxcount (get loop-state 'maxcount))
	 (input (get loop-state 'input))
	 (output (get loop-state 'output))
	 (headout (make-hashtable (choice-size keys)))
	 (tailout (and tail (make-hashtable (choice-size keys))))
	 (stopkeys (indexctl output 'metadata 'stopkeys))
	 (drop-count 0)
	 (copy-count 0)
	 (tail-count 0)
	 (over-count 0)
	 (value-count 0))
    (let* ((keyvec (choice->vector (difference keys stopkeys)))
	   (keyvals (index/fetchn input keyvec)))
      (if (and (not mincount) (not maxcount) (not tail))
	  (let ((i 0) (nkeys (length keyvec)))
	    (while (< i nkeys)
	      (add! output (elt keyvec i) (elt keyvals i))
	      (set! value-count (+ value-count (choice-size (elt keyvals i))))
	      (set! i (1+ i))))
	  (let ((i 0) (nkeys (length keyvec))
		(key #f) (vals #f) (nvals 0) (copied #f))
	    (while (< i nkeys)
	      (set! key (elt keyvec i))
	      (set! vals (elt keyvals i))
	      (set! nvals (choice-size vals))
	      (set! copied #t)
	      (cond ((= nvals 0) (set! copied #f))
		    ((and maxcount (> nvals maxcount))
		     (set! over-count (1+ over-count))
		     (set! copied #f))
		    ((and mincount (> nvals mincount)) (set! copied #f))
		    ((and tailcount (< nvals tailcount))
		     (set! tail-count (1+ tail-count))
		     (if tailout
			 (add! tailout key vals)
			 (set! copied #f)))
		    (else (add! headout key vals)))
	      (cond (copied
		     (set! value-count (+ value-count nvals))
		     (set! copy-count (1+ copy-count)))
		    (else (set! drop-count (1+ drop-count))))
	      (set! i (1+ i))))))
    (index-merge! output headout)
    (when tailout (index-merge! tail tailout))
    (table-increment! batch-state 'keys copy-count)
    (table-increment! batch-state 'drops drop-count)
    (table-increment! batch-state 'tops over-count)
    (table-increment! batch-state 'tails tail-count)
    (table-increment! batch-state 'values value-count)))

(define (index/copy-keys! in out (opts #f))
  (logdebug |Index/copy-keys| in " ==> " out "\n" opts)
  (let* ((started (elapsed-time))
	 (keys (getkeys in))
	 (tail (getopt opts 'tail {}))
	 (using-counts (or (exists? tail)
			   (testopt opts '{maxcount mincount tailcount})))
	 (counters
	  (vector 'keys 'values
		  (and (exists? tail) 'tails)
		  (and (testopt opts '{mincount maxcount}) 'drops)
		  (and (testopt opts 'maxcount) 'tops))))
    (lognotice |Copying|
      (choice-size keys) " keys"
      " from " (index-source in) 
      " to " (index-source out))
    (engine/run key-copier keys
		`#[loop #[input ,in output ,out
			  tail ,(getopt opts 'tail)
			  maxcount ,(getopt opts 'maxcount)
			  tailcount ,(and (getopt opts 'tail) (getopt opts 'tailcount))
			  mincount ,(getopt opts 'mincount)]
		   count-term "key"
		   onerror {stopall signal}
		   logcontext ,(stringout "Copying " (if (index? in) (index-source in) in))
		   logcounters ,(remove #f counters)
		   counters ,(difference (elts counters) #f)
		   logrates ,(difference (elts counters) #f)
		   batchsize ,(getopt opts 'batchsize (config 'BATCHSIZE 10000))
		   batchrange ,(getopt opts 'batchrange (config 'BATCHRANGE 8))
		   nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
		   checktests ,(engine/interval (getopt opts 'savefreq (config 'savefreq 60)))
		   checkpoint {,out ,tail}
		   logfreq ,(getopt opts 'logfreq (config 'LOGFREQ 30))
		   checkfreq ,(getopt opts 'checkfreq (config 'checkfreq 15))
		   logchecks #t
		   started ,started])))
