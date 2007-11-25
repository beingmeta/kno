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

(use-module '{brico brico/dterms fifo logger})

(module-export!
 '{cached-dterm
   require-dterm request-dterm
   launch-dtermdaemons})

(define %loglevel %info!)

;;;; The dterm cache

(define-init dterm-precache {})
(define-init dterm-cache (make-hashtable))
(define-init dterm-fifo (make-fifo))
(define-init dterm-threads {})

(config! 'dtermcaches dterm-cache)

(define (cached-dterm concept (language english))
  (unless (exists? dterm-threads) (launch-dtermdaemons))
  (if (singleton? (?? language (get-norm concept language)))
      (cache-dterm! concept language (get-norm concept language))
      (or (try (get dterm-cache (cons concept language))
	       (cache-compute-dterm concept language))
	  (fail))))

(defslambda (cache-compute-dterm concept language)
  (unless (exists? dterm-threads) (launch-dtermdaemons))
  (try (get dterm-cache (cons concept language))
       (begin (store! dterm-cache (cons concept language) #f)
	      (fifo-push dterm-fifo (cons concept language))
	      (fail))))

(define (request-dterm concept language)
  (try (get dterm-cache (cons concept language))
       (cache-compute-dterm concept language)))

(define (cache-dterm! concept language dterm)
  (synchro-lock cache-compute-dterm)
  (store! dterm-cache (cons concept language) dterm)
  (synchro-unlock cache-compute-dterm)
  dterm)

(define (require-dterm concept (language english))
  (if (singleton? (?? language (get-norm concept language)))
      (get-norm concept language)
      (or (try (get dterm-cache (cons concept language))
	       (let ((dterm (find-dterm concept language)))
		 (cache-dterm! concept language dterm)
		 dterm))
	  (fail))))

(define (ignore x) (fail))

(define (dtermdaemon fifo)
  (let ((entry (fifo-pop fifo)))
    (while (pair? entry)
      (unless (string? (get dterm-cache entry))
	(logdebug "dtermdaemon processing " entry)
	(onerror (let ((dterm (find-dterm (car entry) (cdr entry))))
		   (logdebug "dtermdaemon yielded " (write dterm)
			     " for " entry)
		   (store! dterm-cache entry dterm))
		 (lambda (ex)
		   (logerror "dtermdaemon error processing " entry ": "
			     ex)
		   #f)))
      (set! entry (fifo-pop fifo)))))

(defslambda (launch-dtermdaemons)
  (unless (exists? dterm-threads)
    (loginfo "Launching " (config 'DTERMTHREADS 3) " dtermdaemons")
    (dotimes (i (config 'DTERMTHREADS 3))
      (set+! dterm-threads (spawn (dtermdaemon dterm-fifo))))))
