(in-module 'brico/dtermcache)

(use-module '{brico brico/dterms fifo})

(module-export! '{cached-dterm lanuch-dtermdaemons})

;;;; The dterm cache

(unless (bound? dterm-cache)
  (define dterm-cache (make-hashtable)))
(unless (bound? dterm-fifo)
  (define dterm-fifo (make-fifo)))
(unless (bound? dterm-threads)
  (define dterm-threads {}))

(define (cached-dterm concept (language english))
  (unless (exists? dterm-threads) (launch-dtermdaemons))
  (if (singleton? (?? language (get-norm concept language)))
      (get-norm concept language)
      (or (try (get dterm-cache (cons concept language))
	       (begin (store! dterm-cache (cons concept language) #f)
		      (fifo-push dterm-fifo (cons concept language))
		      (fail))))))

(define (ignore x) (fail))

(define (dtermdaemon fifo)
  (let ((entry (fifo-pop fifo)))
    (while (pair? entry)
      (unless (string? (get dterm-cache entry))
	(onerror (store! dterm-cache entry
			 (find-dterm (car entry) (cdr entry)))
		 ignore))
      (set! entry (fifo-pop fifo)))))

(defslambda (launch-dtermdaemons)
  (unless (exists? dterm-threads)
    (dotimes (i (config 'DTERMTHREADS 3))
      (set+! dterm-threads (spawn (dtermdaemon dterm-fifo))))))
