;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/dtermcache)
;;; Background dterm computation (which can take a while)

;;; This provides a dterm cache which is automatically filled by
;;; background threads.  This keeps threads from blocking on dterm
;;; computation, which can take a while for some concepts.

;;; When a dterm is requested, the cache is checked; if there is a
;;; value there, it is returned (and a #f is converted to a {}).  If
;;; there isn't a value in the cache, #f is stored in the table and
;;; the request (a pair of a concept and a language) is pushed into a
;;; fifo.  Some number of background threads (configured by
;;; DTERMTHREADS) are reading from this FIFO, computing dterms
;;; and storing them back in the cache.

(use-module '{brico brico/dterms brico/xdterms fifo cachequeue logger})

(module-export!
 '{
   cached-dterm require-dterm request-dterm
   cached-xdterm require-xdterm request-xdterm
   lazy-dterm
   })

(define-init %loglevel %notice%)

;;;; The dterm cache

(define-init dterm-cache #f)
(define-init dterm-queue #f)
(define-init dterm-nthreads 4)

(defslambda (setup-dtermqueue! value)
  (set! dterm-cache value)
  (set! dterm-queue (cachequeue value dterm-nthreads)))

(define (dtermqueue-config var (val))
  (cond ((not (bound? val)) dterm-cache)
	((fixnum? val) (set! dterm-nthreads val))
	((string? val)
	 (setup-dtermqueue! (open-index val)))
	((or (index? val) (table? val))
	 (setup-dtermqueue! val))
	(else (setup-dtermqueue! val))))
(config-def! 'dtermqueue dtermqueue-config)

(define (cached-dterm concept (language english))
  (unless dterm-queue (setup-dtermqueue! (make-hashtable)))
  (cq/get dterm-queue get-dterm concept language))

(define (request-dterm concept language)
  (try (or (get-dterm/cached concept language) {})
       (cachecall/probe find-dterm concept language #f)
       (begin
	 (unless dterm-queue (setup-dtermqueue! (make-hashtable)))
	 (cq/get dterm-queue get-dterm concept language))))

(define (require-dterm concept (language english))
  (unless dterm-queue (setup-dtermqueue! (make-hashtable)))
  (let ((dterm (get-xdterm concept language)))
    (store! dterm-cache (list get-dterm concept language) dterm)
    dterm))

(define (lazy-dterm concept language)
  (get dterm-cache (list get-dterm concept language)))

(define (cached-xdterm concept (language english))
  (unless dterm-queue (setup-dtermqueue! (make-hashtable)))
  (try (cq/get dterm-queue get-dterm concept language)
       (cq/get dterm-queue get-xdterm concept language)))

(define (request-xdterm concept language)
  (unless dterm-queue (setup-dtermqueue! (make-hashtable)))
  (cq/get dterm-queue get-xdterm concept language))

(define (require-xdterm concept (language english))
  (unless dterm-queue (setup-dtermqueue! (make-hashtable)))
  (let ((dterm (get-xdterm concept language)))
    (store! dterm-cache (list get-xdterm concept language) dterm)
    dterm))

(define (lazy-dterm concept language)
  (get dterm-cache (list get-dterm concept language)))



(define (ignore x) (fail))


