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
   get-gloss
   get-short-gloss
   get-expstring
   gloss
   basic-concept-frequency
   index-name})

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

(define (get-prefixes wordlist)
  (let ((ixes {}))
    (dotimes (prefixlen (1- (length wordlist)))
      (set+! ixes
	     (stringout (doseq (elt wordlist i)
			  (when (< i (1+ prefixlen))
			    (printout (if (> i 0) " ") elt))))))
    ixes))
(define (get-suffixes wordlist)
  (let ((ixes {}))
    (dotimes (suffixlen (1- (length wordlist)))
      (set+! ixes
	     (stringout (doseq (elt wordlist i)
			  (when (>= i suffixlen)
			    (printout (if (> i suffixlen) " ") elt))))))
    ixes))

(define (index-name index frame name)
  (let* ((std (stdstring name))
	 (segs (segment std " ")))
    (index-frame index frame 'names std)
    (index-frame index frame 'frags (elts segs))
    (when (> (length segs) 1)
      (index-frame index frame 'prefix (get-prefixes segs))
      (index-frame index frame 'suffix (get-suffixes segs)))))

;;; Displaying glosses

(define (gloss f (slotid GLOSS))
  (lineout f "\t" (get f slotid)))

;;; Getting concept frequency information

(define (basic-concept-frequency concept (term #f) (language #f))
  (let ((sum 1) (language (or language english)))
    (do-choices (wf (if term (?? 'of concept 'word term 'language language)
			(?? 'of concept)))
      (set! sum (+ sum (try (get wf 'freq) 0))))
    sum))

