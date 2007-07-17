;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module 'optimize)

(define (get-baseoids-from-base base capacity)
  (choice base
	  (tryif (> capacity 0)
		 (get-baseoids-from-base (oid-plus base capacity)
					 (- capacity (* 1024 1024))))))
(define (get-baseoids pool)
  (get-baseoids-from-base (pool-base pool) (pool-capacity pool)))

(define (make-hash-index-from file from . pools)
  (let* ((source (open-index from))
	 (keys (index-keysvec source))
	 (slotfreq (make-hashtable))
	 (baseoids (->vector (get-baseoids pools))))
    (message "The index " (write from) " contains " (length keys) " keys")
    (doseq (key keys)
      (when (pair? key) (hashtable-increment! slotfreq (car key))))
    (message "Creating new hash index for "
	     (choice-size (getkeys slotfreq)) " slotids with "
	     (length baseoids) " baseoids")
    (make-hash-index file (* 2 (length keys))
		     (rsorted (getkeys slotfreq) slotfreq)
		     baseoids #f (config 'DTYPEV2 #f))
    (message "Populating hash index " file " with "
	     (length keys) " items from " (write from))
    (populate-hash-index (open-index file) source 250000 keys)))

(define (main file from . pools)
  (optimize! make-hash-index-from)
  (apply make-hash-index-from file from pools))
