(in-module 'gnosys/utils/glomming)

(use-module 'texttools)
(use-module 'brico)
(use-module 'morph)
;; Just for english now
(use-module 'chopper/en)
(use-module 'morph/en)

(module-export! '{deglom getcompounds})

;;; Breaking up glommed words

(define (deglommer string start i)
  (if (>= start (length string)) '()
      (if (> i (length string)) (fail)
	  (let* ((substring (subseq string start i))
		 (alternate (if (capitalized? substring)
				(downcase substring)
				(capitalize substring))))
	    (choice (tryif (exists? (get dictionary substring))
			   (cons substring (deglommer string i (+ i 2))))
		    (tryif (exists? (get dictionary alternate))
			   (cons alternate
				 (deglommer string i (+ i 2))))
		    (deglommer string start (+ i 1)))))))

(define (nounp x (language @?english)) (?? language x 'type 'noun))
(define (verbp x (language @?english)) (?? language x 'type 'verb))
(define (adjectivep x (language @?english)) (?? language x 'type 'adjective))
(define (adverbp x (language @?english)) (?? language x 'type 'adverb))

;; This is a supposedly language independent version which gets overridden
(define (generic-wordp string language)
  (or (nounp string language)
      (verbp string language)
      (adjectivep string language)
      (adverbp string language)
      (and (capitalized? string) (nounp (downcase string)))
      (exists? (get-noun-root (downcase string) language
			      (lambda (x) (nounp x language))))
      (exists? (get-verb-root (downcase string) language
			      (lambda (x) (verbp x language))))))

(define (wordp string (language @?english))
  (if (eq? language @?english)
      (and (or (exists? (get dictionary string))
	       (exists? (get dictionary (capitalize string)))
	       (exists? (get dictionary (downcase string)))
	       (exists? (noun-root (downcase string) nounp))
	       (exists? (verb-root (downcase string) verbp)))
	   (not (closed-class-word? string))
	   (not (closed-class-word? (downcase string))))
      (generic-wordp string language)))

(define (deglommer string (language @?english))
  (do ((i 2 (1+ i))
       (lim (- (length string) 2)))
      ((or (> i lim)
	   (and (wordp (subseq string 0 i) language)
		(wordp (subseq string i) language)))
       (if (> i lim) (fail)
	   (list (stdspace (subseq string 0 i))
		 (stdspace (subseq string i)))))))

(define (deglom string (language @?english))
  (cond ((equal? string "") (fail))
	((wordp string) (list string))
	((position #\Space string) (segment string " "))
	((position #\_ string) (segment string "_"))
	((position #\- string) (segment string "-"))
	((position #\. string) (segment string "."))
	(else (deglommer string language))))

;;; Reglomming

(define (find-compound start wordlist fragments)
  (if (null? wordlist) (fail)
      (tryif (test fragments start)
	     (try
	      (let ((probe (append start " " (car wordlist))))
		(choice (if (exists? (get dictionary probe)) probe (fail))
			(if (get fragments probe)
			    (find-compound probe (cdr wordlist) fragments)
			    (fail))))
	      (let ((probe (append start " " (capitalize (car wordlist)))))
		(choice (if (exists? (get dictionary probe)) probe (fail))
			(if (get fragments probe)
			    (find-compound probe (cdr wordlist) fragments)
			    (fail))))))))

(define (probe-compound head wordlist singleok)
  (if (null? wordlist)
      (tryif (and singleok (> (length head) 2)
		  (not (closed-class-word? head))
		  (or (exists? (get dictionary head))
		      (wordp head)
		      (exists? (noun-root head nounp)))
		  (not (hashset-get stop-words head)))
	     head)
      (choice (tryif (get fragments head)
		     (find-compound head wordlist fragments))
	      (tryif (and (capitalized? head)
			  (exists?
			   (?? 'names
			       (stdstring (append head " " (car wordlist))))))
		     (append head " " (car wordlist)))
	      (tryif (and singleok (> (length head) 2)
			  (not (closed-class-word? head))
			  (not (hashset-get stop-words head)))
		     head))))

(define (getcompounds wordlist (singleok #f))
  (if  (null? wordlist) (fail)
       (if (string? wordlist)
	   (getcompounds (getwords wordlist) singleok)
	   (choice (try (probe-compound (car wordlist) (cdr wordlist) singleok)
			(if (capitalized? (car wordlist))
			    (probe-compound (downcase (car wordlist))
					    (cdr wordlist) singleok)
			    (tryif (not (hashset-get stop-words (car wordlist)))
				   (probe-compound (capitalize (car wordlist))
						   (cdr wordlist) singleok))))
		   (getcompounds (cdr wordlist) singleok)
		   (let ((combo (list->phrase wordlist)))
		     (tryif (or (wordp combo)
				(noun-root combo nounp)
				(verb-root combo verbp))
			    combo))))))