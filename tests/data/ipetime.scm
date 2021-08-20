#!/usr/bin/fdexec
;;; -*- Mode: fdscript; -*-

;;; Init stuff

(set-default-cache-level! 2)
(use-pool (append (config-get 'gnosys "/home/sources/gnosys/gnosys") ".pool"))
(use-index (append (config-get 'gnosys "/home/sources/gnosys/gnosys") ".index"))
;(use-pool (append "/data/bg/" {"brico" "xbrico" "names" "places"} ".pool"))
;(use-index (append "/data/bg/" {"brico" "xbrico" "names" "places"} ".index"))
(use-pool (config-get 'bground "bground@bailey"))
(use-index (config-get 'bground "bground@bailey"))
;(use-pool "bground@localhost")
;(use-index "bground@localhost")

(define english (?? 'obj-name 'english))
(define gn/concepts (?? 'obj-name 'gn/concepts))

(define image-pool (use-pool (config-get 'imagedb "dvodb@bailey")))
(define image-index (open-index (config-get 'imagedb "dvodb@bailey")))
;(define image-pool (use-pool "/home/fdaemon/dvo/dvodb.pool"))
;(define image-index (open-index "/home/fdaemon/dvo/dvodb.index"))

(define timeinc-pool (use-pool "/data/timeinc/timeinc.pool"))
(define time-content-pools
  (use-pool (append "/data/timeinc/"
		    {"time_content"
		     "fortune_content"
		     "people_content"
		     "ew_content"}
		    ".pool")))

;;; Operations over ipeval records

(define cacheflush #f)

(define (get-total-fetches records)
  (let ((sum 0))
    (doseq (record records)
      (set! sum (+ sum (first record))))
    sum))
(define (get-total-exec-time records)
  (let ((sum 0))
    (doseq (record records)
      (set! sum (+ sum (second record))))
    sum))
(define (get-final-exec-time records)
  (second (elt records (1- (length records)))))
(define (get-total-fetch-time records)
  (let ((sum 0))
    (doseq (record records)
      (set! sum (+ sum (third record))))
    sum))

;;; 100% speedup is twice as fast or half the time
;;; 200% speedup is three times as fast or 1/3 the time
;;; queries/second 1/wo 1/with
(define (compute-percent-speedup with without)
  (* 100.0 (/ (- (/ 1 with) (/ 1 without)) (/ 1 without))))

;;; Running an ipeval test

(define sample-size 5)

(define (ipevaltest fcn arg)
  (let ((v1 (fcn arg)) (t1 0.0) (t2 0.0))
    (dotimes (i sample-size)
      (clearcaches)
      (if cacheflush (system "../../etc/usememory " cacheflush))
      (set! t1 (+ t1 (second (%timeval (fcn arg))))))
    (set! t1 (/ t1 sample-size))
    (dotimes (i sample-size)
      (clearcaches)
      (if cacheflush (system "../../etc/usememory " cacheflush))
      (set! t2 (+ t2 (second (%timeval (ipeval (fcn arg)))))))
    (set! t2 (/ t2 sample-size))
    (clearcaches)
    (if cacheflush (system "../../etc/usememory " cacheflush))
    (let ((track-record (third (track-ipeval (fcn arg)))))
      (vector t1 t2 (compute-percent-speedup t2 t1)
	      arg (choice-size v1) (qc v1)
	      (length track-record)
	      (get-total-fetches track-record)
	      (get-total-exec-time track-record)
	      (get-total-fetch-time track-record)
	      (get-final-exec-time track-record)))))

;;;; Accessing test result fields

(define (wo/ipeval-time rec) (elt rec 0))
(define (w/ipeval-time rec) (elt rec 1))
(define (ipeval-speedup rec) (elt rec 2))
(define (trial-input rec) (elt rec 3))
(define (trial-output-size rec) (elt rec 4))
(define (trial-output rec) (elt rec 5))
(define (ipeval-cycles rec) (elt rec 6))
(define (ipeval-fetches rec) (elt rec 7))
(define (ipeval-exec-time rec) (elt rec 8))
(define (ipeval-fetch-time rec) (elt rec 9))
(define (ipeval-final-exec-time rec) (elt rec 10))

;;; Running a set of tests

(define (ipevaltests fcn args)
  (let ((speedup-sum 0)
	(reftime-sum/w 0)
	(reftime-sum/wo 0)
	(n 0))
    (do-choices (item args i)
      (let ((sample (ipevaltest fcn item)))
	(lineout i "\t"
		 (trial-output-size sample) "\t"
		 (ipeval-fetches sample) "\t"
		 (ipeval-cycles sample) "\t"
		 (* 1000.0 (wo/ipeval-time sample)) "\t"
		 (* 1000.0 (w/ipeval-time sample)) "\t"
		 (ipeval-speedup sample) "\t"
		 (/ (* 1000.0 (wo/ipeval-time sample))
		    (ipeval-fetches sample)) "\t"
		 (/ (* 1000.0 (w/ipeval-time sample))
		    (ipeval-fetches sample)) "\t"
		 (* 1000.0 (ipeval-exec-time sample)) "\t"
		 (* 1000.0 (ipeval-fetch-time sample)) "\t"
		 (* 1000.0 (ipeval-final-exec-time sample)) "\t"
		 (if (sequence? (trial-input sample))
		     (length (trial-input sample))
		   1) "\t"
		 (trial-input sample))
	(set! speedup-sum (+ speedup-sum (ipeval-speedup sample)))
	(set! reftime-sum/w (+ reftime-sum/w
			       (/ (* 1000.0 (w/ipeval-time sample))
				  (ipeval-fetches sample))))
	(set! reftime-sum/wo (+ reftime-sum/wo
				(/ (* 1000.0 (wo/ipeval-time sample))
				   (ipeval-fetches sample))))
	(set! n (+ n 1))))
    (lineout "## Mean speedup=" (/ speedup-sum n))
    (lineout "## Total reftime speedup="
	     (compute-percent-speedup reftime-sum/w reftime-sum/wo))))

;;; Descent tests

(define (get-hyponyms x)
  (choice x (get-hyponyms (get x 'hyponym))))
(define (get-subclasses x)
  (choice x (get-subclasses (get x @?specls))))

(define (get-hyponyms2 x) (get* x 'hyponym))
(define (get-subclasses2 x) (get* x @?specls))

(define (get-subclasses3 x) (get x @?specls*))

;;; Generating sample data

(define (generate-descent-samples)
  (dtype->file (?? 'has 'hyponym) "descent.samples"))

(define (generate-fake-query image n)
  (map (lambda (x) (pick-one (get x english)))
       (choice->list (pick-n (get image gn/concepts) n))))

(define (random-image)
  (let ((image (random-oid image-pool)))
    (until (exists? (get image gn/concepts))
      (set! image (random-oid image-pool)))
    image))

(define (generate-query-samples)
  (let ((queries {}))
    (dotimes (i 500)
      (let ((q (generate-fake-query (random-image) 2)))
	(lineout q)
	(set+! queries q)))
    (dotimes (i 500)
      (let ((q (generate-fake-query (random-image) 3)))
	(lineout q)
	(set+! queries q)))
    (dotimes (i 500)
      (let ((q (generate-fake-query (random-image) 4)))
	(lineout q)
	(set+! queries q)))
    (dotimes (i 500)
      (let ((q (generate-fake-query (random-image) 5)))
	(lineout q)
	(set+! queries q)))
    (dtype->file queries "query.samples")))

(define (generate-refanalyze-samples)
  (dtype->file (pool-elts timeinc-pool) "refanalyze.samples"))

;;; Diambiguation tests

(define (make-keylist keys)
  (map (lambda (key) (cons key (qchoice (?? english key))))
       keys))
(define (2index x)
  (if (string? x) (open-index x) x))

(define (get-corpus-scores keylist indexarg) 
  (let* ((corpus-scores (make-hashtable 1024))
	 (indexvec (if (vector? indexarg) (map 2index indexarg)
		     (vector (2index indexarg))))
	 (n-levels (length indexvec))) 
    (dolist (key keylist)
      (let ((meanings (cdr key)))
	(doseq (index indexvec i)
	  (do-choices (doc (find-frames index
			     gn/concepts (get meanings @?specls*)))
	    ;; (gdb i)
	    (hashtable-increment! corpus-scores doc (- n-levels i))))))
    corpus-scores))
(define (get-meaning-scores keylist indexarg)
  (let* ((indexvec (if (vector? indexarg) (map 2index indexarg)
		     (vector (2index indexarg))))
	 (corpus-scores (get-corpus-scores keylist indexvec))
	 (meaning-scores (make-hashtable))
	 (n-meanings 0))
    (dolist (key keylist)
      (do-choices (meaning (cdr key))
	(do-choices (instance (find-frames (elts indexvec)
				gn/concepts (get meaning @?specls*)))
	  (hashtable-increment! meaning-scores meaning
				(get corpus-scores instance)))))
    meaning-scores))

(define (disambig-keylist keylist)
  (let ((scores (get-meaning-scores keylist image-index)))
    (map (lambda (entry)
	   (cons (car entry)
		 (qchoice (try (largest (cdr entry) scores)
			       (cdr entry)))))
	 keylist)))

(define (do-disambig keyseq)
  (disambig-keylist (make-keylist keyseq)))

;;; Generating IPEVAL sample timelines

(define (generate-timeline records)
  (let ((time 0.0))
    (doseq (record records i)
      (set! time (+ time (second record)))
      (lineout i "\t"
	       (+ time (/ (third record) 2)) "\t"
	       (first record) "\t"
	       (second record) "\t"
	       (third record))
      (set! time (+ time (third record))))))

(define (timeline fcn arg)
  (let ((tracking (track-ipeval (fcn arg))))
    (generate-timeline (third tracking))
    (first tracking)))

;;; The main event

(define (main fcname trials infile)
  (let ((inputs (pick-n (file->dtype infile) trials))
	(fcn (eval fcname)))
    (lineout "Testing " fcn " on " (choice-size inputs) " inputs")
    (ipevaltests fcn (qc inputs))
    #f))

