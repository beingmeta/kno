;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module 'tighten)

(define (get-baseoids pools)
  (if (null? pools) '()
      (let* ((pool (use-pool (car pools)))
	     (base (pool-base pool))
	     (capacity (pool-capacity pool)))
	(append (get-baseoids-from-base base capacity)
		(get-baseoids (cdr pools))))))

(define (get-baseoids-from-base base capacity)
  (cons base
	(if (> capacity 0)
	    (get-baseoids-from-base (oid-plus base capacity)
				    (- capacity (* 1024 1024)))
	    '())))

(define (remove-dups lst)
  (if (null? lst) lst
      (cons (car lst)
	    (remove-dups (remove (car lst) (cdr lst))))))

(define (make-hash-index-from file from . pools)
  (let* ((source (open-index from))
	 (keys (index-keysvec source))
	 (slotfreq (make-hashtable))
	 (baseoids (->vector (remove-dups (get-baseoids pools)))))
    (message "The index " (write from) " contains " (length keys) " keys")
    (doseq (key keys)
      (when (pair? key) (hashtable-increment! slotfreq (car key))))
    (message "Creating new hash index for "
	     (choice-size (getkeys slotfreq)) " slotids with "
	     (length baseoids) " baseoids")
    (make-hash-index file (- (* 2 (length keys)))
		     (rsorted (getkeys slotfreq) slotfreq)
		     baseoids #f (config 'DTYPEV2 #f))
    (message "Populating hash index " file " with "
	     (length keys) " items from " (write from))
    (populate-hash-index (open-index file) source 250000 keys)))

(define (main file from . pools)
  (tighten! make-hash-index-from)
  (apply make-hash-index-from file from pools))
