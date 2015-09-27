;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

(in-module 'swapcache)

(use-module '{reflection ezrecords logger varconfig})

(module-export!
 '{swc/make swc/get swc/store!
   swapcache/make swapcache/get swapcache/store!
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
  (try (get (swapcache-front swc) key)
       (if (test (swapcache-back swc) key)
	   (begin (set! v (try (get (swapcache-back swc) key)
			       dflt))
	     (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	       (swapcache/swap! swc))
	     (store! (swapcache-front swc) key v)
	     v)
	   dflt)))
(define (swapcache/get swc key (dflt {})) (swc/get swc key (qc dflt)))

(define (swc/store! swc key value)
  (if (test (swapcache-front swc) key)
      (store! (swapcache-front swc) key value)
      (begin
	(when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	  (swapcache/swap! swc (swapcache-max swc)))
	(store! (swapcache-front swc) key value))))
(define (swapcache/store! swc key value) (swc/store! swc key value))

(define (swc/cache swc key proc (v))
  (try (get (swapcache-front swc) key)
       (if (test (swapcache-back swc) key)
	   (begin (set! v (get (swapcache-back swc) key))
	     (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	       (swapcache/swap! swc))
	     (store! (swapcache-front swc) key v)
	     v)
	   (begin (set! v (proc key))
	     (when (> (table-size (swapcache-front swc)) (swapcache-max swc))
	       (swapcache/swap! swc))
	     (store! (swapcache-front swc) key v)
	     v))))
(define (swapcache/cache swc key proc) (swc/cache swc key proc))

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
 (define (cached-get n)
   (swc/cache swctest n compute-string))
  ;; This should show compute-string called less for smaller numbers
  ;; because they stay in the cache.
  (begin (dotimes (i 9) (cached-get i))
    (dotimes (i 5) (cached-get i))
    (dotimes (i 9) (cached-get i))))
