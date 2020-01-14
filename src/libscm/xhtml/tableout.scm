;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved

(in-module 'xhtml/tableout)

(use-module '{webtools xhtml})

(module-export! '{tableout tableout/value})

(define (tableout arg
		  (opts '()) (class)
		  (skipempty) (slotfns) (handlers) (skip)
		  (maxdata) (maxdatafn) (recur)
		  (sortfn))
  (if (string? opts) (set! opts `#[class ,opts]))
  (default! class (getopt opts 'class "fdjtdata"))
  (default! skipempty (getopt opts 'skipempty #f))
  (default! skip (getopt opts 'skip {}))
  (default! slotfns (getopt opts 'slotfns #f))
  (default! sortfn (getopt opts 'sortfn #f))
  (default! maxdata (getopt opts 'maxdata 1024))
  (default! maxdatafn (getopt opts 'maxdatafn #f))
  (default! recur (getopt opts 'recur #f))
  (default! handlers (getopt opts 'handlers {}))
  (let ((keys (getkeys arg)))
    (table* ((class class))
      (doseq (key (if sortfn
		      (if (applicable? sortfn)
			  (sortfn (qc keys))
			  (lexsorted keys))
		      (choice->vector keys)))
	(unless (overlaps? key skip)
	  (let ((values (get arg key))
		(handled #f))
	    (when (exists? handlers)
	      (doseq (h handlers)
		(when (and (not handled) (applicable? h))
		  (set! handled (h arg key (qc values)))
		  (if (not (bound? handled)) (set! handled #f)
		      (if (fail? handled) (set! handled #f))))))
	    (cond (handled) ;; Taken care of
		  ((and slotfns (test slotfns key))
		   ((get slotfns key) arg key (qc values)))
		  ((not (bound? values))
		   (unless skipempty
		     (tr (th key) (td* ((class "empty")) "Oddly empty value"))))
		  ((empty? values)
		   (unless skipempty
		     (tr (th key) (td* ((class "empty")) "No values"))))
		  (else (do-choices (value values i)
			  (tr (if (zero? i) (th key) (th))
			      (td (tableout/value value arg key recur
						  maxdata maxdatafn))))))))))))

(define (tableout/value data t key (recur #f) (maxdata 1024) (maxdatafn #f))
  (when (number? recur)
    (if (or (zero? recur) (negative? recur))
	(set! recur #f)
	(set! recur (-1+ recur))))
  (if (and maxdata
	   (or (string? data) (packet? data))
	   (> (length data) maxdata))
      (if maxdatafn
	  (maxdatafn t key data)
	  (let ((type (try (pickstrings (get t (intern (glom key "_TYPE"))))
			   (pickstrings (get t (intern (glom key "-TYPE"))))
			   #f)))
	    (xmlout
	      (anchor* (datauri data (or type "application"))
		  ((target "_blank") (class "datauri"))
		(length data)
		(if (packet? data) " bytes" " characters")
		" of " (if type (xmlout "'" type "'")
			   "unknown type")
		" starting with ")
	      (slice data 0 (min maxdata 42))
	      "...")))
      (cond ((symbol? data) (span ((class "symbol")) (xmlout data)))
	    ((or (not recur) (string? data) (number? data))
	     (xmlout data))
	    ((not (or (vector? data) (pair? data) (slotmap? data)))
	     (xmlout data))
	    ((shortenough data))
	    ((vector? data)
	     (vecout data t key recur maxdata maxdatafn))
	    ((pair? data)
	     (if (proper-list? data)
		 (listout data t key recur maxdata maxdatafn)
		 (if (pair? (cdr data))
		     (iplistout data t key recur maxdata maxdatafn)
		     (consout data t key recur maxdata maxdatafn))))
	    ((slotmap? data)
	     (slotmapout data t key recur maxdata maxdatafn))
	    (else (xmlout data)))))

(define (shortenough val)
  (let ((string (stringout val)))
    (cond ((< (length string) 128 ) (xmlout string) #t)
	  (else #f))))

(define (vecout vec t key recur maxdata maxdatafn)
  (span ((class "label")) "Vector of " (length vec) " elements")
  (xmlblock OL ()
    (doseq (e vec)
      (xmlblock LI ()
	(tableout/value e t key recur maxdata maxdatafn)))))
(define (listout lst t key recur maxdata maxdatafn)
  (span ((class "label")) "List of " (length lst) " elements")
  (xmlblock OL ()
    (doseq (e lst)
      (xmlblock LI ()
	(tableout/value e t key recur maxdata maxdatafn)))))
(define (consout lst t key recur maxdata maxdatafn)
  (xmlblock DL ()
    (xmlblock DT () "CAR")
    (xmlblock DD () (car lst))
    (xmlblock DT () "CDR")
    (xmlblock DD () (cdr lst))))
(define (iplistout lst t key recur maxdata maxdatafn)
  (span ((class "label")) "Improper list of " (length lst) "+1 elements")
  (xmlblock DL ()
    (let ((scan lst) (count 1))
      (while (pair? scan)
	(xmlblock "DT" () count)
	(xmlblock "DD" ()
	  (tableout/value (car scan) t key recur
			  maxdata maxdatafn))
	(set! count (1+ count))
	(set! scan (cdr scan)))
      (xmlblock "DT" () "TAIL")
      (xmlblock "DD" ()
	(tableout/value scan t key recur
			maxdata maxdatafn)))))

(define (slotmapout data t key recur maxdata maxdatafn)
  (xmlblock DL ()
    (doseq (skey (lexsorted (getkeys data)))
      (do-choices (v (get data skey))
	(xmlblock DT () skey)
	(xmlblock DD ()
	  (tableout/value v data skey #f maxdata maxdatafn))))))
















