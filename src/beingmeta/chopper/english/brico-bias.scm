(use-module 'brico)
(config! 'cachelevel 2)
(config! 'bricosource "/data/bg/brico")
(load-component "../english.scm")
(define custom-dictionary
  (file->dtype (get-component "custom.table")))

(define *closed-tags*
  '{preposition
    glue conjunction punctuation sentence-end
    modal-aux aux negator be-aux be-verb
    subject-pronoun object-pronoun pronoun
    definite-article indefinite-article quantifier
    singular-determiner plural-determiner determiner})

(define (bfreq term pos)
  (let ((sum 0))
    (do-choices (word (?? 'word term 'type pos))
      (set! sum (+ sum (try (get word 'freq) 0))))
    sum))

(define (bump-weight weight)
  (cons (car weight) (1+ (cdr weight))))

(define (change-weights term)
  (when (and (fail? (?? @?english term))
	     (exists? (?? @?english (capitalize term))))
    (lineout ";;; Bumping " term " down in favor of " (capitalize term))
    (store! dictionary term (bump-weight (get dictionary term))))
  (when (and (not (overlaps? (car (get dictionary term)) *closed-tags*))
	     (not (char-numeric? (first term)))
	     (exists? (?? 'word term)))
    (let* ((current (get dictionary term))
	   (tags (car current))
	   (possible (filter-choices (pos '{noun verb adjective adverb})
		       (exists (?? @?english term 'type pos))))
	   (likely (filter-choices (pos '{noun verb adjective adverb})
		     (> (bfreq term pos) 1)))
	   (primary
	    (choice (tryif (capitalized? term) 'proper-name)
		    (largest possible
			     (lambda (pos)
			       (if (zero? (bfreq term pos)) 0
				   (1+ (quotient (bfreq term pos) 5)))))))
	   (new (choice (cons primary 0)
			(for-choices (wt current)
			  (if (overlaps? primary (car wt)) (fail) wt)))))
      (when (exists? possible)
	(unless (identical? new current)
	  (lineout ";; Changing POS data for " (write term))
	  (lineout ";;   from " current)
	  (lineout ";;   to   " new)
	  (store! dictionary term new))))))

(define (set-weights! term . weights)
  (dolist (weight weights)
    (drop! dictionary term
	   (cons (car weight) (get (get dictionary term) (car weight))))
    (add! dictionary term weight)))

(define (main)
  (message "Starting key prefetch")
  (prefetch-keys! (cons (choice 'word @?english)
			(choice (getkeys dictionary)
				(capitalize (getkeys dictionary)))))
  (message "Starting OID prefetch")
  (prefetch-oids! (?? (choice 'word @?english) (getkeys dictionary)))
  (message "Starting to change weights")
  (do-choices (term (getkeys dictionary))
    (unless (or (exists? (get custom-dictionary term))
		(exists? (verb-root term))
		(exists? (noun-root term)))
      (change-weights term)))
  (message "Starting to zap inflections")
  (do-choices (term (getkeys dictionary))
    (when (and (not (get custom-dictionary term))
	       (fail? (?? @?english term))
	       (or (exists? (noun-root term))
		   (exists? (verb-root term))))
      (store! dictionary term {})))
  (prefetch-keys! (cons 'sense-category 'noun.substance))
  (prefetch-oids! (?? 'sense-category 'noun.substance))
  (let ((custom-terms (getkeys custom-dictionary)))
    (set-weights! (difference (get (?? 'sense-category 'adj.pert) @?english)
			      custom-terms)
		  '(noun-modifier . 0) '(adjective . 1))
    (set-weights! (difference (get (?? 'sense-category 'noun.substance)
				   @?english)
			      custom-terms)
		  '(solitary-noun . 1))
    (set-weights! (difference (get (?? 'has @?ingredient-of) @?english)
			      custom-terms)
		  '(solitary-noun . 0)))
  (commit)
  (dtype->file dictionary "dictionary.table"))


