(in-module 'hashstats)

(module-export! '{hashstats hs/summary})

(define hashstats
  (ambda (keys hashfns (sizes #f))
    (for-choices (hashfn hashfns)
      (for-choices (size sizes)
	(when (inexact? size)
	  (set! size (inexact->exact (* size (choice-size keys)))))
	(let ((buckets (make-hashtable (* 3 (choice-size keys))))
	      (bucketsizes (make-hashtable (* 3 (choice-size keys)))))
	  (do-choices (key keys)
	    (let ((hashvalue (if size (remainder (hashfn key) size)
				 (hashfn key))))
	      (add! buckets hashvalue key)
	      (hashtable-increment! bucketsizes hashvalue)))
	  (vector buckets bucketsizes (choice-size keys) hashfn size))))))

(define (hs/summary report)
  (let ((buckets (elt report 0))
	(bucketsizes (elt report 1))
	(n-keys (elt report 2))
	(hashfn (elt report 3))
	(size (elt report 4))
	(sum 0) (squaresum 0) (n 0) (highest 0)
	(max 0) (maxkey #f) (direct 0) (empty 0))
    (do-choices (key (getkeys bucketsizes))
      (when (> key highest) (set! highest key))
      (let ((nkeys (get bucketsizes key)))
	(set! n (1+ n))
	(set! sum (+ sum nkeys))
	(set! squaresum (+ squaresum (* nkeys nkeys)))
	(when (> nkeys max)
	  (set! max nkeys) (set! maxkey key))
	(cond ((zero? nkeys) (set! empty (+ empty 1)))
	      ((= nkeys 1) (set! direct (+ direct 1))))))
    (printout "Storing " n-keys " keys using " hashfn
	      (if size (printout " across " size " buckets")) ".\n"
	      "Averaging " (/~ sum n) " keys per bucket over "
	      n " buckets, " empty " empty, " direct " direct, "
	      (- n direct empty) " collisions\n"
	      "The highest bucket used was " highest
	      " and the most loaded bucket, " maxkey ", had "
	      max " keys.\n")))

