;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'calltrack)

;;; This is a simple profiling tool for the FramerD scheme implementation.
;;;  It reads in the calltrack files written out when profiling is enabled
;;   and creates a database which can be interrogated to find out where
;;   your program is spending its time.
;; The CALLTRACK data only covers function applications, so any time spent in
;;  special form execution (which doesn't bottom out in function application) 
;;  is credited towards the function itself.

(use-module 'texttools)

(module-export! '{ct/load ct/simple ct/report ct/summary ct/detailed ct/callpoints})

(module-export!
 '{
   ctd/contexts
   ctd/callees
   ctd/fndata
   ctd/calldata
   
   cte/callcount
   cte/runtime
   cte/objloads
   cte/keyfetches})

;;; Reading the records from a calltrack log file

;;; Read a line of calltrack data.
(define (line-parser line pos)
  (let ((next (position #\Space line pos)))
    (if next
	(if (= next pos) (line-parser line (1+ pos))
	    (cons (string->lisp (subseq line pos next))
		  (line-parser line (1+ next) )))
	(list (string->lisp (subseq line pos))))))

(define (parse-items in)
  (let ((v (read in)))
    (if (eof-object? v) '()
	(cons v (parse-items in)))))
(define (parse-line line pos)
  (let ((in (open-input-string line)))
    (dotimes (i pos) (getchar in))
    (->vector (parse-items in))))

(define (line->record line)
  (append (->vector (parse-line line 1)) #(0.0 0 0)))

;;; Building the database

;;; The database stores accumulated execution information for both
;;;  individual functions and for 'call points' indicating a backtrace
;;;  of functions.  The backtrace is constructed while examining the calltrack
;;;  log file.

;;; The database is a vector of four tables:
;;;   CONTEXTS (elt 0) maps function names into their various call points 
;;;   CALLEES (elt 1) maps function names into the subordinate call points
;;;   FNTABLE (elt 2) maps functions into accumulated profiling information
;;;   CXTTABLE (elt 3) maps call points into accumulated profiling information

(define (record-callcount v) (elt v 1))
(define (record-elapsed v) (elt v 1))
(define (record-objects v) (elt v 2))
(define (record-fetches v) (elt v 3))

(define (handle-calltrack-entry call return context cxttable fntable)
  ;; (lineout "handle-calltrack-entry: \n\t" call "\n\t" return)
  (let ((delta (list->vector (cons 1 (vector->list (map - (subseq return 1) (subseq call 1))))))
	(cxtentry (get cxttable context))
	(fnentry (get fntable (first call))))
    (if (fail? cxtentry)
	(store! cxttable context delta)
	(store! cxttable context (map + cxtentry delta)))
    (if (fail? fnentry)
	(store! fntable (first call) delta)
	(store! fntable (first call) (map + cxtentry delta)))))

(define (handle-calltrack-entry call return context cxttable fntable)
  ;; (lineout "handle-calltrack-entry: \n\t" call "\n\t" return)
  (let ((runtime (- (record-elapsed return)
		    (record-elapsed call)))
	(objects (- (record-objects return)
		    (record-objects call)))
	(fetches (- (record-fetches return)
		    (record-fetches call)))
	(cxtentry (get cxttable context))
	(fnentry (get fntable (first call))))
    (if (fail? cxtentry)
	(store! cxttable context (vector 1 runtime objects fetches))
	(store! cxttable context
		(vector (1+ (elt cxtentry 0))
			(+ runtime (elt cxtentry 1))
			(+ objects (elt cxtentry 2))
			(+ fetches (elt cxtentry 3)))))
    (if (fail? fnentry)
	(store! fntable (first call) (vector 1 runtime objects fetches))
	(store! fntable (first call)
		(vector (1+ (elt fnentry 0))
			(+ runtime (elt fnentry 1))
			(+ objects (elt fnentry 2))
			(+ fetches (elt fnentry 3)))))))

(define (handle-calltrack-entry call return context cxttable fntable)
  ;; (lineout "handle-calltrack-entry: \n\t" call "\n\t" return)
  (let ((delta (->vector (cons 1 (->list (map - (subseq return 1) (subseq call 1))))))
	(cxtentry (get cxttable context))
	(fnentry (get fntable (first call))))
    (if (fail? cxtentry)
	(store! cxttable context delta)
	(store! cxttable context (map + cxtentry delta)))
    (if (fail? fnentry)
	(store! fntable (first call) delta)
	(store! fntable (first call) (map + fnentry delta)))))

(define (ct/load file)
  (let* ((f (open-input-file file))
	 (callstack '())
	 (callcontext '())
	 (contexts (make-hashtable))
	 (callees (make-hashtable))
	 (cxttable (make-hashtable))
	 (fntable (make-hashtable))
	 (clocktime #f) (objs #f) (keys #f)
	 (line (getline f))
	 (fields '(CALLS TIME OIDS KEYS))
	 (startup #t))
    (until (eof-object? line)
      ;; (lineout "calltrack: " line)
      ;; (lineout "callstack: " callstack)
      (cond ((eq? (elt line 0) #\#)
	     (set! line (getline f)))
	    ((eq? (elt line 0) #\:)
	     (set! fields (segment (subseq line 1) " "))
	     (set! line (getline f)))
	    ((and (eq? (elt line 0) #\<) startup)
	     (set! line (getline f)))
	    ((and (eq? (elt line 0) #\<) (pair? callstack))
	     (let ((return (parse-line line 1))
		   (call (car callstack)))
	       ;; (lineout "return: " return)
	       (when (not (equal? (first call) (first return)))
		 (message "Inconsistent call/return:\n\t" call "\n\t" return))
	       (handle-calltrack-entry
		call return callcontext cxttable fntable)
	       (set! callstack (cdr callstack))
	       (set! callcontext (cdr callcontext))
	       (set! line (getline f))))
	    ((eq? (elt line 0) #\<)
	     (message "Unpushed pop: " line)
	     (set! line (getline f)))
	    ((eq? (elt line 0) #\>)
	     (set! startup #f)
	     (let* ((record (parse-line line 1))
		    (newcontext (cons (first record) callcontext)))
	       ;; (lineout "call: " record)
	       (add! contexts (first record) newcontext)
	       (when (pair? callcontext)
		 (add! callees (car callcontext) newcontext))
	       (set! callstack (cons record callstack))
	       (set! callcontext newcontext)
	       (set! line (getline f))))
	    (else (message "Bad data line: " line)
		  (set! line (getline f)))))
    (vector contexts callees fntable cxttable fields)))

;;;; Accessing records

(define (ctd/contexts cs) (elt cs 0))
(define (ctd/callees cs) (elt cs 1))
(define (ctd/fndata cs) (elt cs 2))
(define (ctd/calldata cs) (elt cs 3))

(define (cte/callcount c) (elt c 0))
(define (cte/runtime c) (elt c 1))
(define (cte/objloads c) (elt c 2))
(define (cte/keyfetches c) (elt c 3))

(define (relative-callcxt callcxt relative)
  (if (null? callcxt) '()
      (if (eq? callcxt relative) '()
	  (cons (car callcxt) (relative-callcxt (cdr callcxt) relative)))))


;;;; Report functions

(define (ct/simple tables fn)
  (if (pair? fn)
      (let ((data (get (ctd/calldata tables) fn)))
	(if (fail? data)
	    (lineout "The trackpoint " fn " never occurred")
	    (lineout "Trackpoint: " fn "\n"
		     "This trackpoint was invoked "
		     (cte/callcount data) " times, taking "
		     (cte/runtime data) " seconds, retrieving "
		     (cte/objloads data) " objects, and fetching "
		     (cte/keyfetches data) " index keys")))
      (let ((data (get (ctd/fndata tables) fn)))
	(if (fail? data) (message fn " was not called")
	    (lineout fn " was called " (cte/callcount data) " times, taking "
		     (cte/runtime data) " seconds, retrieving "
		     (cte/objloads data) " objects, and fetching "
		     (cte/keyfetches data) " index keys")))))

(define (ct/report tables fn (relative #f))
  (if (pair? fn)
      (let ((fnname (if (pair? fn) (car fn) fn))
	    ;; (if relative (relative-callcxt fn relative) fn)
	    (data (get (elt tables 3) fn)))
	(if (fail? data) (lineout "The trackpoint " fnname " was not called")
	    (let* ((children (get (elt tables 1) fn))
		   (childdata
		    (calltrack-sum (qc (get (elt tables 3) children)))))
	      (lineout "The trackpoint " fnname
		       " occurred " (elt data 0) " times, taking "
		       (elt data 1) " seconds, retrieving "
		       (elt data 2) " objects, and fetching "
		       (elt data 3) " index keys")
	      (if (exists? children)
		  (lineout "The trackpoint " fn " itself took "
			   (- (elt data 1) (elt childdata 1)) " seconds, "
			   "retrieving " (- (elt data 2) (elt childdata 2))
			   " objects, and fetching "
			   (- (elt data 3) (elt childdata 3))
			   " index keys")
		  (lineout
		      "The trackpoint " fnname " made no calls of its own")))))
      (let ((data (get (elt tables 2) fn)))
	(if (fail? data) (lineout fn " was not called")
	    (let* ((children (get (elt tables 1) fn))
		   (childdata (calltrack-sum (qc (get (elt tables 3) children)))))
	      (lineout fn " was called " (elt data 0) " times, taking "
		       (elt data 1) " seconds, retrieving "
		       (elt data 2) " objects, and fetching "
		       (elt data 3) " index keys")
	      (if (exists? children)
		  (lineout
		      "Calling " fn " itself took "
		      (- (elt data 1) (elt childdata 1)) " seconds, retrieving "
		      (- (elt data 2) (elt childdata 2)) " objects, and fetching "
		      (- (elt data 3) (elt childdata 3)) " index keys")
		  (lineout fn
		    " didn't call any other procedures, "
		    "spending all its time on itself.")))))))

(define (ct/detailed tables fn)
  (ct/simple tables fn)
  (let* ((callpoints
	  (filter-choices (callpoint (getkeys (ctd/calldata tables)))
	    (position fn callpoint)))
	 (fns (car callpoints))
	 (combined (make-hashtable)))
    (do-choices (fn fns)
      (let* ((calls (filter-choices (callpoint callpoints)
		      (eq? (car callpoint) fn)))
	     (calldata (calltrack-sum (qc (get (ctd/calldata tables) calls)))))
	(store! combined fn calldata)))
    (doseq (fn (rsorted fns (lambda (x) (cte/runtime (get combined x)))))
      (let ((data (get combined fn)))
	(lineout fn " was called " (elt data 0) " times, taking "
		 (elt data 1) " seconds, retrieving "
		 (elt data 2) " objects, and fetching "
		 (elt data 3) " index keys")))))

(define (ct/callpoints tables fn)
  (ct/simple tables fn)
  (let* ((callpoints
	  (filter-choices (callpoint (getkeys (ctd/calldata tables)))
	    (position fn callpoint))))
    (doseq (callpoint (rsorted callpoints second))
      (ct/simple tables callpoint))))

(define (calltrack-sum calls)
  (let ((count 0) (secs 0) (objects 0) (keys 0))
    (do-choices (data calls)
      (when (exists? data)
	(set! count (+ count (elt data 0)))
	(set! secs (+ secs (elt data 1)))
	(set! objects (+ objects (elt data 2)))
	(set! keys (+ keys (elt data 3)))))
    (vector count secs objects keys)))

(define (ct/summary ctdata)
  (doseq (fn (reverse (sorted (getkeys (ctd/contexts ctdata))
			      (lambda (x) (cte/runtime (get (ctd/fndata ctdata) x))))))
    (ct/report ctdata fn)))




