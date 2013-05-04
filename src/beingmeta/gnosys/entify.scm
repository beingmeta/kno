;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'gnosys/entify)

;;; This does knowledge based entity extraction from free text.
;;;  In principle it is vaugely language neutral though in practice,
;;;  it requires some kind of morphological regularization (though not
;;;  grammatical) to be really effective.

(define version "$Id$")

(use-module '{brico brico/lookup texttools tagger morph})
(use-module '{gnosys gnosys/disambiguate})

(module-export! '{entify extract-concepts text->ambentries})

(define logit comment)
;; (define logit message)

(define %volatile 'logit)

(define (oddcaps? string)
  (let* ((words (elts (words->vector string)))
	 (caps (pick words capitalized?))
	 (real-words (pick words length > 3)))
    (> (* (choice-size caps) 2) (choice-size real-words))))

;;; Extraction+Disambiguation

(define (extract-concepts text language (disambig #t))
  (let ((ambentries (text->ambentries text language)))
    ;; Now generate phrase/concept pairs, disambiguating if requested.
    (if disambig
	(for-choices (entry (disambiguate ambentries disambig))
	  (tryif (exists? (ambentry-resolved entry))
		 (cons (ambentry-word entry) (qc (ambentry-resolved entry)))))
	(for-choices (entry ambentries)
	  (tryif (exists? (ambentry-resolved entry))
		 (cons (ambentry-word entry) (qc (ambentry-resolved entry))))))))

;;;; TEXT->AMBENTRIES

;;; This implements knowledge based entity extraction which doesn't
;;; rely on a conventional structural parser.  The idea is to identify
;;; terms and compounds which have matches in the knowledge base, and
;;; to return ambentries based on those terms and compounds.

;;; The algorithm basically looks to divide a string into compound
;;; names or phrases which have matches in the knowledge base.  These
;;; are converted into ambentries which can then be disambiguated.

;;; The algorithm uses "fragment indexing" in BRICO.  In fragment
;;; indexing, a concept described by a compound phrase (for example,
;;; 'The New Brady Bunch') is indexed by special keys based on the
;;; fragments of the compound.  If the compound contains a string 's',
;;; the concept is indexed with a key (for the appropriate language
;;; slotid) of '(list s)'.  The initial and final terms of the
;;; compound are also indexed as '(list #f s)' and '(list s '#f)'
;;; respectively.  So, 'The New Brady Bunch' would be indexed under:
;;;  @?en={(#f "The") ("The") ("New") ("Brady") ("Bunch") ("Bunch" #f)}

;;; The algorithm is simple.  The text is broken into a vector of
;;; words (using words->vector) and the core loop starts at a position
;;; in this vector (initially zero).  It then moves forward in the
;;; vector as long as there might be (based on fragment indexing) a
;;; phrase that corresponds to the range of words.  This means that
;;; the initial word is indexed as a prefix, the final word is indexed
;;; as a suffix, and all the intervening words are indexed as
;;; components.

;;; When it has reached a maximum range, it starts looking backward
;;; to actually find a phrase that matches.  When it does so, it
;;; returns that phrase and the algorithm starts again positioned at
;;; the end of that phrase.

(define (text->ambentries text language (justcaps #f) (oddcaps))
  (let* ((oddcaps (if (bound? oddcaps) oddcaps (oddcaps? text)))
	 (start (elapsed-time))
	 (fetchdone #f)
	 (alldone #f)
	 (langmod (get-langmod language))
	 (stopwords (and langmod (get langmod 'stop-words)))
	 (basev (if justcaps
		    (caps->vector text 3)
		    (words->vector text)))
	 (wordv (map (lambda (w)
		       (choice w
			       (tryif (and oddcaps (capitalized? w))
				      (vary-word (downcase w) language #f #t))
			       (vary-word w language #f #t)))
		     basev))
	 (allwords (elts wordv))
	 (start (elapsed-time))
	 (results {})
	 (len (length wordv))
	 (i 0))
    (prefetch-keys! (cons language
			  (choice (list allwords)
				  (list #f allwords)
				  (list allwords #f))))
    (set! fetchdone (elapsed-time))
    (logit "WORDV=" wordv)
    (while (< i len)
      (let* ((bound (probe-forward language wordv i len))
	     (found (search-backward language wordv i bound)))
	(logit "i=" i "; bound=" bound "; found=" found)
	(if (exists? found)
	    (begin (set+! results
			  (term->ambentry (cdr found) language))
		   (set! i (car found)))
	    (begin
	      (unless (or (get stopwords (elt wordv i))
			  (get stopwords (downcase (elt wordv i))))
		(set+! results
		       (let ((concepts (lookup-word (elt wordv i) language)))
			 (make-ambentry (elt basev i) (elt wordv i) language
					concepts (singleton concepts)))))
	      (set! i (1+ i))))))
    (set! alldone (elapsed-time))
    (logit "TEXT->AMBENTRIES took "
	   (- alldone start) "="
	   (- fetchdone start) "+" (- alldone fetchdone)
	   " secs for :" text)
    results))

(define (caps->vector string (keeplen 0))
  (remove #f (map (lambda (w)
		    (and (or (capitalized? w) (<= (length w) keeplen)) w))
		  (words->vector string))))

(define (probe-forward language wordv start len)
  (let ((candidates (?? language (list #f (elt wordv start))))
	(lastlive #f)
	(i (1+ start)))
    (while (and (< i len) (exists? candidates))
      (set! candidates
	    (intersection candidates
			  (?? language (list (elt wordv i)))))
      (when (exists? (?? language (list (elt wordv i) #f)))
	(set! lastlive i))
      (set! i (1+ i)))
    (if lastlive (1+ lastlive) (fail))))

(define (search-backward language wordv start end)
  (logit "SEARCH-BACKWARD start=" start "; end=" end)
  (if (<= (- end start) 1) (fail)
      (let* ((phrases (seq->phrase wordv start end))
	     (concepts (lookup-word phrases language)))
	(logit "phrases=" phrases "; concepts=" concepts)
	(if (exists? concepts)
	    (cons end (qc (filter-choices (phrase phrases)
			    (exists? (lookup-word phrase language)))))
	    (search-backward language wordv start (-1+ end))))))

;;; Finding variant forms of words

(define (poss-norm string)
  (cond ((has-suffix string "'s") (subseq string 0 -2))
	((has-suffix string "s'") (subseq string 0 -1))
	((and (= (length string) 2) (has-suffix string "."))
	 (subseq string 0 1))
	(else (fail))))

(define (get-variants word language (trylower #f))
  (choice word (poss-norm word)
	  (tryif (not (capitalized? word))
		 (choice (get-noun-root word language)
			 (get-verb-root word language)))
	  (tryif (and trylower (capitalized? word))
		 (let ((lwd (downcase word)))
		   (choice lwd (get-noun-root lwd language)
			   (get-verb-root lwd language))))))

;;; Prefetching

(define (preprobe-compounds language wordv (trylower #f))
  (let ((compounds {}) (len (length wordv)))
    (dotimes (start (length wordv))
      (let ((candidates
	     (?? language (list #f (get-variants (elt wordv start) language trylower))))
	    (scan (1+ start))
	    (endpoints {}))
	(while (and (< scan len) (exists? candidates))
	  (when (overlaps?
		 candidates
		 (?? language (list (get-variants (elt wordv (-1+ scan)) language trylower)
				    #f)))
	    (set+! endpoints scan))
	  (set! candidates
		(intersection candidates
			      (?? language
				  (list (get-variants (elt wordv scan) language trylower)))))
	  (set! scan (1+ scan)))
	(when (and (exists? candidates) (= scan len)
		   (exists? (?? language
				(list (get-variants (elt wordv (-1+ scan)) language trylower))))) 
	  (set+! endpoints len))
	(set+! compounds (choice (elt wordv start)
				 (seq->phrase wordv start endpoints)))))
    compounds))




