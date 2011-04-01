(in-module 'gnosys/utils/urlwords)

(use-module 'texttools)
(use-module 'gnosys/utils/glomming)

(module-export! 'geturlwords)

;;; This gets the "words" out of a URL

(define (geturlwords url)
  (getcompounds (deglom (geturlelts url)) #t))

(define (geturlelts-raw url)
  (choice (elts (get url 'path))
	  (elts (difference (subseq (segment (get url 'hostname) ".") 0 -1)
			    {"co" "ac" "gov" "com" "edu" "org" "gv"}))
	  (if (position #\. (get url 'name))
	      (subseq (get url 'name) 0 (position #\. (get url 'name)))
	      (get url 'name))))
(define (geturlelts url)
  (filter-choices (string (geturlelts-raw url))
    (and (> (length string) 1)
	 (not (char-numeric? (elt string 0))))))

