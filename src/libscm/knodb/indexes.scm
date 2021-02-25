;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
Copyright (C) 2020-2021 beingmeta, LLC

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
  (let* ((in (if (index? from) from
		 (if (string? from)
		     (open-index from (get-read-opts opts))
		     (irritant from |NotIndex| index/copy!))))
	 (infile (if (string? from) from (index-source in)))
	 (outfile (or to (index-source in)))
	 (rarefile (getopt opts 'rarefile))
	 (inplace (and (file-exists? outfile)
		       (equal? (realpath infile) (realpath outfile))))
	 (out (if (and (file-exists? outfile) (not inplace)
		       (not (getopt opts 'overwrite (config 'OVERWRITE #f))))
		  (irritant outfile |OutputAlreadyExists|)
		  (get-new-index outfile in opts)))
	 (rare (and rarefile (get-new-index rarefile in opts)))
	 (copy-opts `(#[rare ,rare] . ,opts))
	 (copier (cond ((testopt opts 'copier 'generic) index/copy-keys!)
		       ((getopt opts 'copier) (getopt opts 'copier))
		       ((overlaps? (indexctl in 'metadata 'type) {"kindex" "hashindex"})
			hashindex/copy-keys!)
		       (else index/copy-keys!)))
	 (ok #f))
    (onerror (begin (copier in out copy-opts) (set! ok #t)))
    (when ok
      (index/install! out outfile)
      (when rare (index/install! rare rarefile)))
    ok))

(define (merge-index in out (opts #f) (rare) (minthresh))
  (default! rare (getopt opts 'rare))
  (default! minthresh (getopt opts 'minthresh (and rare 1)))
  (let* ((copy-opts `(#[rare ,rare minthresh ,minthresh] . ,opts)) (ok #f))
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
    ;; 			  rare ,rarefile
    ;; 			  timestamp ,(gmtimestamp)
    ;; 			  session ,(config 'sessionid)
    ;; 			  minthresh ,(getopt opts 'minthresh)
    ;; 			  maxthresh ,(getopt opts 'minthresh)]))
  in)

(defambda (index/merge! from outfile (opts #f))
  (let* ((rarefile (getopt opts 'rarefile))
	 (merge (open-index from opts))
	 (out (if (file-exists? outfile)
		  (open-index outfile opts)
		  (get-new-index outfile merge opts)))
	 (rare (and rarefile
		    (if (file-exists? rarefile)
			(open-index rarefile opts)
			(get-new-index rarefile merge opts))))
	 (copy-opts `(#[rare ,rare] . ,opts))
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
			  rare ,rarefile
			  timestamp ,(gmtimestamp)
			  session ,(config 'sessionid)
			  minthresh ,(getopt opts 'minthresh)
			  maxthresh ,(getopt opts 'minthresh)]))
    (when ok
      ;; Handle copying of any files in temporary locations
      (index/install! out outfile)
      (when rare (index/install! rare rarefile)))
    ok))

(define (index/install! index file)
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
  (let* ((rare   (try (get loop-state 'rare) #f))
	 (minthresh (get loop-state 'minthresh))
	 (rarethresh (and rare (get loop-state 'rarethresh)))
	 (maxthresh (get loop-state 'maxthresh))
	 (input  (get loop-state 'input))
	 (output (get loop-state 'output))
	 (outhash (make-hashtable (choice-size keys)))
	 (rarehash (and rare (make-hashtable (choice-size keys))))
	 (stopkeys (indexctl output 'metadata 'stopkeys))
	 (copy-count 0)
	 (rare-count 0)
	 (value-count 0))
    (let* ((keyvec (choice->vector (difference keys stopkeys)))
	   (keyvals (index/fetchn input keyvec)))
      (if (and (not minthresh) (not maxthresh) (not rare))
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
		    ((and maxthresh (> nvals maxthresh)) (set! copied #f))
		    ((and minthresh (> nvals minthresh)) (set! copied #f))
		    ((and rarethresh (< nvals rarethresh))
		     (set! rare-count (1+ rare-count))
		     (if rarehash
			 (add! rarehash key vals)
			 (set! copied #f)))
		    (else (add! outhash key vals)))
	      (when copied
		(set! value-count (+ value-count nvals))
		(set! copy-count (1+ copy-count)))
	      (set! i (1+ i))))))
    (index-merge! output outhash)
    (when rarehash (index-merge! rare rarehash))
    (table-increment! batch-state 'copied copy-count)
    (table-increment! batch-state 'rarekeys rare-count)
    (table-increment! batch-state 'values value-count)))

(define (index/copy-keys! in out (opts #f))
  (let ((started (elapsed-time))
	(rare (getopt opts 'rare {}))
	(keys (getkeys in)))
    (lognotice |Copying|
      (choice-size keys) " keys"
      " from " (index-source in) 
      " to " (index-source out))
    (engine/run key-copier keys
		`#[loop #[input ,in output ,out
			  rare ,(getopt opts 'rare)
			  maxthresh ,(getopt opts 'maxthresh)
			  rarethresh ,(and (getopt opts 'rare) (getopt opts 'rarethresh))
			  minthresh ,(getopt opts 'minthresh)]
		   count-term "keys"
		   onerror {stopall signal}
		   counters {copied rarekeys values}
		   logcontext ,(stringout "Copying " (if (index? in) (index-source in) in))
		   logrates {copied rarekeys values}
		   batchsize ,(getopt opts 'batchsize (config 'BATCHSIZE 10000))
		   batchrange ,(getopt opts 'batchrange (config 'BATCHRANGE 8))
		   nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
		   checktests ,(engine/interval (getopt opts 'savefreq (config 'savefreq 60)))
		   checkpoint {,out ,rare}
		   logfreq ,(getopt opts 'logfreq (config 'LOGFREQ 30))
		   checkfreq ,(getopt opts 'checkfreq (config 'checkfreq 15))
		   logchecks #t
		   started ,started])))
