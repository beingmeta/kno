(in-module 'brico)

;; For index-name, at least
(use-module 'texttools)

(module-export!
 '{brico-pool
   brico-index
   english spanish french
   genls genls* specls specls*
   parts parts* part-of part-of*
   members members* member-of member-of*
   ingredients ingredients* ingredient-of ingredient-of*
   isa inverse =is= disjoint implies implies*
   get-gloss get-short-gloss get-expstring gloss
   index-string index-name index-gloss index-kindof index-frame*
   basic-concept-frequency concept-frequency use-corpus-frequency})

(define bricosource #f)
(define brico-pool {})
(define brico-index {})
(define english {})
(define spanish {})
(define french {})
(define genls {})
(define genls* {})
(define specls {})
(define specls* {})
(define parts {})
(define parts* {})
(define part-of {})
(define part-of* {})
(define members {})
(define members* {})
(define member-of {})
(define member-of* {})
(define ingredients {})
(define ingredients* {})
(define ingredient-of {})
(define ingredient-of* {})
(define isa {})
(define =is= {})
(define inverse {})
(define disjoint {})
(define implies {})
(define implies* {})

(define (get-gloss concept (language #f))
  (try (get concept (?? 'type 'gloss 'language (or language english)))
       (get concept 'gloss)))
(define (get-short-gloss concept (language #f))
  (let ((s (get-gloss concept language)))
    (if (position #\; s)
	(subseq s 0 (position #\; s))
	s)))
(define (get-expstring concept (language english))
  (stringout
      (let ((words (get concept (choice language 'names))))
	(if (fail? words) (set! words (get concept (choice english 'names))))
	(if (fail? words) (printout "Je n'ai pas les mots.")
	    (do-choices (word words i)
	      (if (position #\Space word)
		  (printout (if (> i 0) " . ") "\"" word "\"")
		  (printout (if (> i 0) " . ") word)))))
    (printout " < ")
    (forgraph (lambda (x)
		(do-choices (word (get x (choice language 'names)) i)
		  (if (position #\Space word)
		      (printout (if (> i 0) " . " " ") "\"" word "\"")
		      (printout (if (> i 0) " . " " ") word))))
	      (get concept implies) implies)))

(define bricosource-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) bricosource)
	  ((equal? val bricosource)
	   bricosource)
	  ((exists? brico-pool)
	   (message "Redundant configuration of BRICOSOURCE "
		    "which is already provided from " brico-pool)
	   #f)
	  (else
	   (set! bricosource val)
	   (use-pool val) (set! brico-index (use-index val))
	   (set! brico-pool (name->pool "brico.framerd.org"))
	   (if (exists? brico-pool)
	       (begin
		 (set! english (?? '%id 'english))
		 (set! french (?? '%id 'fr))
		 (set! spanish (?? '%id 'es))
		 (set! genls (?? '%id 'genls))
		 (set! genls* (?? '%id 'genls*))
		 (set! specls (?? '%id 'specls))
		 (set! specls* (?? '%id 'specls*))
		 (set! parts (?? '%id 'parts))
		 (set! parts* (?? '%id 'parts*))
		 (set! part-of (?? '%id 'part-of))
		 (set! part-of* (?? '%id 'part-of*))
		 (set! members (?? '%id 'members))
		 (set! members* (?? '%id 'members*))
		 (set! member-of (?? '%id 'member-of))
		 (set! member-of* (?? '%id 'member-of*))
		 (set! ingredients (?? '%id 'ingredients))
		 (set! ingredients* (?? '%id 'ingredients*))
		 (set! ingredient-of (?? '%id 'ingredient-of))
		 (set! ingredient-of* (?? '%id 'ingredient-of*))
		 (set! isa (?? '%id 'isa))
		 (set! inverse (?? '%id 'inverse))
		 (set! =is= (?? '%id '=is=))
		 (set! disjoint (?? '%id 'disjoint))
		 (set! implies (?? '%id 'implies))
		 (set! implies* (?? '%id 'implies*))
		 #t)
	       (begin (set! brico-index {})
		      #f))))))
(config-def! 'bricosource bricosource-config)

;;; Indexing functions

(define (index-string index frame slot (value #f) (window 2))
  (let ((values (if value value (get frame slot))))
    (do-choices (v values)
      (let* ((stdspaced (stdspace v))
	     (baseform (basestring stdspaced)))
	(index-frame index frame slot stdspaced)
	(index-frame index frame slot baseform)
	(when window
	  (let* ((words (words->vector stdspaced))
		 (basewords (words->vector baseform))
		 (frags (choice (vector->frags words window)
				(vector->frags basewords window))))
	    (index-frame index frame slot frags)))))))

(define (index-name index frame slot (value #f) (window 2))
  (let ((values (if value value (get frame slot))))
    (do-choices (v values)
      (let* ((downspaced (downcase (stdspace v)))
	     (baseform (basestring downspaced)))
	(index-frame index frame slot downspaced)
	(index-frame index frame slot baseform)
	(when window
	  (let* ((words (words->vector downspaced))
		 (basewords (words->vector baseform))
		 (frags (choice (vector->frags words window)
				(vector->frags basewords window))))
	    (index-frame index frame slot frags)))))))

(define (index-kindof index frame slot)
  (let ((v (get frame slot)))
    (when (exists? v)
      (index-frame index frame slot (get v @?kindof*)))))

(define (index-frame* index frame slot base)
  (do ((g (get frame base) (difference (get g base) seen))
       (seen frame (choice g seen)))
      ((empty? g))
    (index-frame index frame slot g)))

(define (index-gloss index frame slotid (value #f))
  (let* ((wordlist (getwords (or value (get frame slotid))))
	 (gloss-words (elts wordlist)))
    (index-frame index frame slotid
		 (choice gloss-words (porter-stem gloss-words)))))

;;; Displaying glosses

(define (gloss f (slotid GLOSS))
  (lineout f "\t" (get f slotid)))

;;; Getting concept frequency information

(define (basic-concept-frequency concept language term)
  (let ((sum 0) (language (or language english)))
    (do-choices (wf (if term (?? 'of concept 'word term 'language language)
			(?? 'of concept)))
      (set! sum (+ sum (try (get wf 'freq) 0))))
    ;; If there is no frequency data, use the native language occurence
    ;;  as a single instance.
    (if (zero? sum) 
	(if (test concept language term) 1 0)
	sum)))

;; This is a list of functions to get concept/term frequency information.
;;  Each item is a car of a name (which is an arbitrary value) and a function
;;  of three arguments (concept, language, term) that returns a count of
;;  either co-occurences of term in language with concept or the absolute
;;  frequency of the concept if language and term are false.
(define freqfns (list (cons 'basic basic-concept-frequency)))

(define (concept-frequency concept (language #f) (term #f))
  (let ((sum 0))
    (dolist (method freqfns)
      (let ((freq ((cdr method) concept language term)))
	(when (and (exists? freq) (number? freq))
	  (set! sum (+ sum freq)))))
    sum))

;;; Configuring freqfns

(define (edit-freqfns name fn scan)
  (if (null? scan) (fail)
      (if (and name (equal? (car (car scan)) name))
	  (cons (cons name fn) (cdr scan))
	  (cons (car scan) (edit-freqfns name fn (cdr scan))))))

(define (freqfns-config var (val))
  (if (bound? val)
      (if (pair? val)
	  (set! freqfns
		(try (edit-freqfns (car val) (cdr val) freqfns)
		     (cons val freqfns)))
	  (set! freqfns (cons (cons #f val) freqfns)))
      disambigfns))
(config-def! 'freqfns freqfns-config)

;; This returns a method for using indexed corpora to provided
;; absolute or contingent concept frequency information.
(define use-corpus-frequency
  (ambda (corpus conceptslot wordslot (langslot #f))
    (cons
     (vector (qc corpus) conceptslot wordslot)
     (lambda (concept language word)
       (if (not language)
	   (choice-size (find-frames corpus conceptslot concept))
	   (if langslot
	       (choice-size (find-frames corpus
			      conceptslot concept
			      wordslot word
			      langslot language))
	       (choice-size (find-frames corpus
			      conceptslot concept
			      wordslot word))))))))

