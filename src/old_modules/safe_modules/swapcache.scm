;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'swapcache)

(use-module '{reflection ezrecords logger varconfig})

(module-export!
 '{swc/make swc/get swc/modtime
   swc/store! swc/cache swc/cache* swc/cache+
   swapcache/make swapcache/store!
   swapcache/get swapcache/modtime
   swapcache/cache swapcache/cache* swapcache/cache+
   swapcache/swap! swapcache/swap?!})

(define-init %loglevel %notice%)
(define-init trace-values #f)

(define-init swapcache:max 1000)
(varconfig! swapcache:max swapcache:max)

(define-init swapcache:init 200)
(varconfig! swapcache:init swapcache:init)

(define swapcache:default-opts #[])
(varconfig! swapcache:opts swapcache:opts)

(defrecord (swapcache mutable)
  (id (getuuid))
  (front (make-hashtable swapcache:init))
  (back (make-hashtable swapcache:init))
  (max swapcache:max)
  (init swapcache:init)
  (trace #f)
  (opts (frame-create swapcache:default-opts)))

(define (swc/make (opts swapcache:default-opts) (front #f) (back #f))
  (let ((id (getopt opts 'id (getuuid)))
	(init (getopt opts 'init swapcache:init))
	(max (getopt opts 'max swapcache:max))
	(trace (getopt opts 'trace #f)))
    (cons-swapcache id (or front (make-hashtable init))
		    (or back (make-hashtable))
		    max init trace opts)))
(define (swapcache/make . args)
  (apply swc/make args))

(define (swc/get swc key (dflt {}) (v))
  (try (car (get (swapcache-front swc) key))
       (if (test (swapcache-back swc) key)
	   (begin (set! v (get (swapcache-back swc) key))
	     (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	       (swapcache/swap! swc))
	     (if (exists? v)
		 (begin (store! (swapcache-front swc) key v)
		   (car v))
		 dflt))
	   dflt)))
(define (swapcache/get swc key (dflt {})) (swc/get swc key (qc dflt)))

(define (swc/store! swc key value)
  (if (test (swapcache-front swc) key)
      (store! (swapcache-front swc) key (cons value (gmtimestamp)))
      (begin
	(when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	  (swapcache/swap! swc (swapcache-max swc)))
	(store! (swapcache-front swc) key (cons value (gmtimestamp))))))
(define (swapcache/store! swc key value) (swc/store! swc key value))

(define (swc/modtime swc key)
  (try (cdr (get (swapcache-front swc) key)) #f))
(define (swapcache/modtime swc key) (swc/modtime swc key))

;;;; Cache functions

(define (swc/cache swc key proc (v))
  (try (car (get (swapcache-front swc) key))
       (if (test (swapcache-back swc) key)
	   (begin (set! v (get (swapcache-back swc) key))
	     (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	       (swapcache/swap! swc))
	     (store! (swapcache-front swc) key v)
	     (car v))
	   (begin (set! v (proc key))
	     (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	       (swapcache/swap! swc))
	     (store! (swapcache-front swc) key (cons v (gmtimestamp)))
	     v))))
(define (swapcache/cache swc key proc) (swc/cache swc key proc))

(define (swc/cache* swc proc . args)
  (let* ((key (apply vector (or (procedure-name proc) proc) args))
	 (cached (get (swapcache-front swc) key)))
    (try (car cached)
	 (begin (set! cached (get (swapcache-back swc) args))
	   (tryif (exists? cached)
	     (begin
	       (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
		 (swapcache/swap! swc))
	       (store! (swapcache-front swc) key cached)
	       (car cached))))
	 (let ((v (apply proc args)))
	   (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	     (swapcache/swap! swc))
	   (store! (swapcache-front swc) args (cons v (gmtimestamp)))
	   v))))
(define (swapcache/cache* swc key proc . args)
  (apply swc/cache* swc key proc args))

(define (swc/cache+ swc key proc . args)
  (try (car (get (swapcache-front swc) key))
       (let ((v (get (swapcache-back swc) key)))
	 (tryif (exists? v)
	   (begin
	     (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	       (swapcache/swap! swc))
	     (store! (swapcache-front swc) key v)
	     (car v))))
       (let ((v (apply proc args)))
	 (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	   (swapcache/swap! swc))
	 (store! (swapcache-front swc) key (cons v (gmtimestamp)))
	 v)))
(define (swapcache/cache+ swc key proc . args)
  (apply swc/cache+ swc key proc args))

;;;; Doing swaps

(define swapcache/swap!
  (slambda (swc (thresh #f))
    (if (swapcache-trace swc)
	(logwarn |SWAPCACHE/swap|
	  "Swapping swapcache " (swapcache-id swc) 
	  " (#!" (number->string (hashptr swc) 16) ")"
	  " @" (table-size (swapcache-front swc)))
	(lognotice |SWAPCACHE/swap|
	  "Swapping swapcache " (swapcache-id swc) 
	  " (#!" (number->string (hashptr swc) 16) ")"
	  " @" (table-size (swapcache-front swc)) ))
    (when (> (table-size (swapcache-front swc))
	     (or thresh (swapcache-max swc)))
      (set-swapcache-back! swc (swapcache-front swc))
      (set-swapcache-front! swc (make-hashtable (swapcache-init swc))))))

(define (swapcache/swap?! swc (thresh #f))
  (when (> (table-size (swapcache-front swc))
	   (or thresh (swapcache-max swc)))
    (swapcache/swap! swc)))

;;; Simple test
(comment
 (define swctest (swc/make #[max 5 id swctest]))
 (define (compute-string n)
   (logwarn |ComputeString| n)
   (number->string n))
 (define (compute-string2 n)
   (logwarn |ComputeString2| n)
   (cons n (number->string n)))
 (define (cached-get n)
   (swc/cache swctest n compute-string))
 ;; This should show compute-string called less for smaller numbers
 ;; because they stay in the cache.
 (begin (dotimes (i 9) (cached-get i))
   (dotimes (i 5) (lineout "Got " (cached-get i) " for " i))
   (dotimes (i 9) (lineout "Got " (cached-get i) " for " i)))
 (set! swctest (swc/make #[max 5 id swctest]))
 (define (cached-get2 n)
   (swc/cache* swctest compute-string2 n))
 (begin (dotimes (i 9) (cached-get2 i))
   (dotimes (i 5) (lineout "Got " (write (cached-get2 i)) " for " i))
   (dotimes (i 9) (lineout "Got " (write (cached-get2 i)) " for " i))))



