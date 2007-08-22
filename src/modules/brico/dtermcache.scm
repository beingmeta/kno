(in-module 'brico/dtermcache)

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

(use-module '{brico brico/dterms fifo})

(module-export! '{cached-dterm launch-dtermdaemons})

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
	       (cache-compute-dterm concept language))
	  (fail))))

(defslambda (cache-compute-dterm concept language)
  (try (get dterm-cache (cons concept language))
       (begin (store! dterm-cache (cons concept language) #f)
	      (fifo-push dterm-fifo (cons concept language))
	      (fail))))

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
