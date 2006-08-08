(in-module 'brico)

;; For index-name, at least
(use-module 'texttools)

(module-export!
 '{brico-pool
   brico-index
   english spanish french
   genls genls* kindof kindof* specls specls*
   parts parts* part-of part-of*
   members members* member-of member-of*
   ingredients ingredients* ingredient-of ingredient-of*
   isa inverse =is= disjoint implies implies*
   get-gloss get-short-gloss get-expstring gloss language-map gloss-map
   index-string index-name index-gloss index-kindof index-frame*
   basic-concept-frequency concept-frequency use-corpus-frequency})

(define bricosource #f)
(define brico-pool {})
(define brico-index {})
(define english {})
(define english-gloss {})
(define spanish {})
(define french {})
(define genls {})
(define genls* {})
(define kindof {})
(define kindof* {})
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
(define language-map (make-hashtable))
(define gloss-map (make-hashtable))

(define (get-gloss concept (language #f))
  (try (tryif (or (not language) (eq? language english)) (get concept 'gloss))
       (get concept (?? 'type 'gloss 'language (or language english)))
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
		 (set! english-gloss (?? 'type 'gloss 'language english))
		 (set! french (?? '%id 'fr))
		 (set! spanish (?? '%id 'es))
		 (set! genls (?? '%id 'genls))
		 (set! genls* (?? '%id 'genls*))
		 (set! genls (?? '%id 'kindof))
		 (set! genls* (?? '%id 'kindof*))
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
		 (do-choices (l (?? 'type 'language))
		   (store! language-map (get l 'key) l)
		   (store! language-map (get l 'langid) l))
		 (do-choices (l (?? 'type 'gloss))
		   (store! gloss-map (get l 'key) l))
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

(define (index-kindof index frame slot (values))
  (let ((v (if (bound? values) values (get frame slot))))
    (when (exists? v)
      (index-frame index frame slot (get v kindof*)))))

(define (index-frame* index frame slot base)
  (do ((g (get frame base) (difference (get g base) seen))
       (seen frame (choice g seen)))
      ((empty? g))
    (index-frame index frame slot g)))

(define (index-gloss index frame slotid (value #f))
  (let* ((wordlist (getwords (or value (get frame slotid))))
	 (gloss-words (filter-choices (word (elts wordlist))
			(< (length word) 16))))
    (index-frame index frame slotid
		 (choice gloss-words (porter-stem gloss-words)))))

(define kindof*-slotids
  '{@1/2c274{PARTOF}
    @1/2c277{INGREDIENT-OF}
    @1/2c279{MEMBER-OF}
    @1/2c27e{ISA}})

(define concept-slotids
  {@1/2ab4d{DEFTERMS}
   @1/2ab57{REFTERMS}
   @1/2b74c{INCLUDES}
   @1/2c275{PARTS}
   @1/2c276{INGREDIENTS}
   @1/2c278{MEMBERS}
   @1/2c27b{KINDOF*}
   @1/2c281{PARTOF*}})

(define kindof @1/2c272{KINDOF})
(define defterms @1/2ab4d{DEFTERMS})
(define defines @1/2ab55{DEFINES})
(define refterms @1/2ab57{REFTERMS})
(define referenced @1/2ab5a{REFERENCED})

(define (index-concept index concept)
  (index-frame index concept 'has (getslots concept))
  (index-frame index concept '{type wikiref sense-category %norm})
  (index-string index concept english (get concept 'words) 1)
  (index-name index concept 'names (qc (get concept 'names)) 1)
  (index-name index concept 'names (qc (pick  (cdr (get concept '%words)) capitalized?)) 1)
  (do-choices (slotid kindof*-slotids)
    (index-kindof index concept slotid (qc (%get concept slotid))))
  (do-choices (slotid concept-slotids)
    (index-frame index concept slotid (%get concept slotid)))
  ;; This handles the case of explicit inverse pointers.
  ;;  If we want to add a pointer R from X to Y and
  ;;   we can't or don't want to modify X, we store
  ;;   an explicit inverse ((inv R) Y)=X which will be
  ;;   found by the inverse inference methods.
  ;; Otherwise, we don't index the @?defines and @?referenced
  ;;  slots because it is easier to just get @?defterms
  ;;  and @?refterms.
  (when (%test concept defines)
    (index-frame index (%get concept defines) defterms concept))
  (when (test concept referenced)
    (index-frame index (%get concept referenced) refterms concept))
  (when (test concept 'gloss)
    (index-frame index concept 'has english-gloss))
  (when (test concept '%glosses)
    (index-frame index concept 'has
		 (get gloss-map (car (get concept '%glosses)))))
  (do-choices (xlation (get concept '%words))
    (let ((lang (get language-map (car xlation))))
      (index-string index concept lang (cdr xlation) 1))))

(define (indexer-prefetch oids)
  (prefetch-oids! oids)
  (let ((kovalues (get oids kindof*-slotids)))
    (let ((visited (choice->hashset kovalues)))
      (do ((scan kovalues
		 (reject (%get visited kindof) visited)))
	  ((empty? scan))
	(prefetch-oids! scan)
	(hashset-add! visited scan)))))

(module-export! '{index-concept indexer-prefetch})

;;; Displaying glosses

(define (gloss f (slotid GLOSS))
  (lineout f "\t" (get f slotid)))

;;; Getting concept frequency information

(define (basic-concept-frequency concept (language #f) (term #f))
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

