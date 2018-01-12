;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimize varconfig logger mttools})

(define (interval-string secs (precise #t))
  (let* ((days (inexact->exact (floor (/ secs (* 3600 24)))))
	 (hours (inexact->exact
		 (floor (/ (- secs (* days 3600 24))
			   3600))))
	 (minutes (inexact->exact
		   (floor (/ (- secs (* days 3600 24) (* hours 3600))
			     60))))
	 (secs (- secs (* days 3600 24) (* hours 3600) (* minutes 60))))
    (stringout
	(cond ((= days 1) "one day, ")
	      ((> days 0) (printout days " days, ")))
      (cond ((= hours 1) "one hour, ")
	    ((> hours 0) (printout hours " hours, ")))
      (cond ((= minutes 1) "one minute, ")
	    ((> minutes 0) (printout minutes " minutes, ")))
      (cond ((= secs 1) "one second")
	    ((< secs 1) (printout secs " seconds"))
	    (else (printout (inexact->string secs 2) " seconds"))))))

(define (short-interval-string secs (precise #t))
  (if (< secs 180)
      (stringout (if (< secs 10) secs (inexact->string secs 2))
	" secs")
      (let* ((days (inexact->exact (floor (/ secs (* 3600 24)))))
	     (hours (inexact->exact
		     (floor (/ (- secs (* days 3600 24))
			       3600))))
	     (minutes (inexact->exact
		       (floor (/ (- secs (* days 3600 24) (* hours 3600))
				 60))))
	     (seconds (- secs (* days 3600 24) (* hours 3600) (* minutes 60))))
	(stringout
	    (cond ((= days 1) "one day, ")
		  ((> days 0) (printout days " days, ")))
	  (when (> hours 0) (printout hours ":"))
	  (printout 
	      (if (and (> hours 0) (< minutes 10)) "0")
	    minutes ":")
	  (printout (if (< seconds 10) "0")
	    (cond ((> secs 600) (inexact->exact (round seconds)))
		  ((>= secs 10) (inexact->string seconds 2))
		  (else seconds)))))))

(define (fudgeit base fudge)
  (cond ((< base 1000) 1000000)
	((inexact? fudge)
	 (inexact->exact (truncate (* fudge base))))
	((> fudge (* 2 base)) fudge)
	(else (* 2 base))))

(define (get-keyblock sizes start chunksize)
  (if (>= start (length sizes)) 0
      (do ((i (1+ start) (1+ i))
	   (n (cdr (elt sizes start)) (+ n (cdr (elt sizes i))))
	   (lim (length sizes)))
	  ((or (>= i lim) (> n chunksize))
	   i))))

(define (repack-report nkeys nvals total from to started)
  (let* ((elapsed (- (elapsed-time) started))
	 (rate (/ nvals elapsed)))
    (message (inexact->string (/ (* 100.0 nkeys) total) 2) "%: "
	     "Just copied +" nkeys "/" total " keys (" nvals " values) in "
	     (short-interval-string elapsed) )))

(define (repack-index from to (fudge 2.0))
  (let* ((in (open-index from))
	 (sizes (rsorted (index-sizes in) cdr))
	 (newsize (fudgeit (length sizes) fudge))
	 (cyclesize (quotient (length sizes) 100))
	 (out (begin (make-file-index to newsize) (open-index to)))
	 (chunksize (config 'chunksize (* 8 65536)))
	 (keycount 0) (valcount 0)
	 (started (elapsed-time)))
    (message "Repacking " (length sizes) " keys from " (write from)
	     " into " newsize " slots in " (write to))
    (do ((start 0 end)
	 (end (get-keyblock sizes 0 chunksize)
	      (get-keyblock sizes end chunksize))
	 (lastreport 0))
	((or (zero? end) (>= end (length sizes))))
      (let ((subvec (subseq sizes start end))
	    (nvalues 0))
	(doseq (elt subvec) (set! nvalues (+ nvalues (cdr elt))))
	(message "Now fetching " (- end start) " keys and " nvalues " values")
	(prefetch-keys! in (car (elts subvec)))
	(doseq (key+size subvec)
	  (when (> (- keycount lastreport) cyclesize)
	    (set! lastreport keycount))
	  (store! out (car key+size) (get in (car key+size)))
	  (set! keycount (1+ keycount))
	  (set! valcount (+ valcount (cdr key+size))))
	(swapout in)
	(commit out)
	(repack-report keycount valcount (length sizes) from to started)
	(swapout out)))))

(define (main in (out #f))
  (tighten! (choice get-keyblock repack-index))
  (if out (repack-index in out (config 'fudge 3.0))
      (let ((tmpfile (string-append in ".part")))
	(repack-index in tmpfile)
	(rename-file in (string-append in ".old"))
	(rename-file tmpfile in))))

(when (config 'optimize #t) (optimize!))


