;;; -*- Mode: Scheme; -*-

(in-module 'domutils/index)

(use-module '{reflection
	      fdweb xhtml texttools domutils
	      morph varconfig logger})

(module-export! '{dom/index! dom/adjust-index!})
(module-export! '{dom/textanalyze apply-refrules})

(define %loglevel %debug!)
(define default-dom-slots '{id class name})

(define default-indexrules (make-hashtable))
(define (indexrule-config var (val))
  (if (bound? val)
      (if (pair? val)
	  (add! default-indexrules (car val) (cdr val))
	  (set+! default-dom-slots val))
      default-indexrules))
(config-def! 'DOM:INDEXRULES indexrule-config)

(define default-analyzers {})
(varconfig! dom:analyzer default-analyzers #f choice)

(defambda (dom/index! index xml (settings #[])
		      (indexslots) (cacheslots) (indexrules)
		      (analyzers) (useids))
  (default! indexslots (try (get settings 'indexslots) default-dom-slots))
  (default! cacheslots (get settings 'cacheslots))
  (default! indexrules (try (get settings 'indexrules) default-indexrules))
  (default! analyzers (try (get settings 'analyzers) default-analyzers))
  (default! useids (try (get settings 'useids) #t))
  (if (pair? xml)
      (dolist (elt xml)
	(dom/index! index elt settings 
		    indexslots cacheslots
		    indexrules analyzers useids))
      (when  (table? xml)
	(let* ((content (get xml '%content))
	       (indexval (if useids (get xml 'id) xml))
	       (eltinfo (dom/lookup indexrules xml))
	       (slots (intersection
		       (choice (pick indexslots symbol?)
			       (car (pick indexslots pair?))
			       (pick eltinfo symbol?)
			       (car (pick eltinfo pair?)))
		       (getkeys xml)))
	       (rules (pick (choice (pick indexslots pair?)
				    (pick eltinfo pair?))
			    slots)))
	  ;; (%WATCH "DOMINDEX" indexval slots rules)
	  (when (test settings 'idmap)
	    (add! (get settings 'idmap) (get xml 'id) xml))
	  (add! index (cons 'has slots) indexval)
	  (when (exists? indexval)
	    (do-choices (slotid slots)
	      (add! index
		    (if (test rules slotid)
			(cons slotid
			      ((get rules slotid)
			       (get xml slotid)))
			(cons slotid (get xml slotid)))
		    indexval))
	    (do-choices (analyzer (choice analyzers (pick eltinfo procedure?)))
	      (do-choices (slot.val (analyzer xml settings))
		(when (overlaps? (car slot.val) cacheslots)
		  (add! xml (car slot.val) (cdr slot.val)))
		(add! index (cons (car slot.val) (cdr slot.val))
		      indexval))))
	  (when (exists? content)
	    (dolist (elt content)
	      (dom/index! index elt settings
			  indexslots cacheslots
			  indexrules analyzers useids)))))))

;;;; Simple text analysis

(define default-refrules {})
(varconfig! dom:refrules default-refrules #f choice)

(define default-rootfn {})
(varconfig! dom:rootfn default-rootfn)

(define (dom/textanalyze xml settings)
  (unless (test settings 'refrules)
    (if (and (test settings 'stopwords) (get settings 'stopwords))
	(store! settings 'refules
		(cachecall compute-refrules (get settings 'stopwords)))
	(store! settings 'refules *basic-refrules*)))
  ;; Get element text and initialize variables.  Note that we strip
  ;;  markup from the text just in case the XML parsing left some
  ;;  markup in the text comment (for example, comments).
  (let* ((text (strip-markup (dom/textify xml)))
	 (rootfn (try (get settings 'rootfn) default-rootfn))
	 (phrasemap (get settings 'phrasemap))
	 (stopwords (get settings 'stopwords))
	 (rootmap (get settings 'rootmap))
	 (refrules (try (get settings 'refrules)
			(compute-refrules (try stopwords #f))))
	 (cache (choice (get settings 'cache)
			(tryif (test settings 'cache 'textslots)
			  '{words roots rootv wordv}))))
    ;; Extract features
    (let* ((wordv (words->vector text #t))
	   (rootv (tryif (and (exists? phrasemap) phrasemap (exists? rootfn))
		    (forseq (word wordv)
		      (if (or (get stopwords word)
			      (and (capitalized? word)
				   (get stopwords (downcase word))))
			  word
			  (try (rootfn word) word)))))
	   (allrefs (getrefs text refrules stopwords))
	   (allwords (choice (reject (elts wordv) string-matches? '(ispunct+))
			     allrefs))
	   (allroots (for-choices (word allwords)
		       (if (or (get stopwords word)
			       (and (capitalized? word)
				    (get stopwords (downcase word))))
			   (fail)
			   (try (rootfn word) word))))
	   (rejects (choice (pick allroots stopwords)
			    (pick allroots downcase stopwords)
			    (for-choices (ref allrefs)
			      (tryif
				  (string-starts-with? ref #((isalpha+) ". "))
				({seq->phrase elts} (words->vector ref) 1)))))
	   (words allwords)
	   (roots (difference allroots rejects)))
      ;; Cache features as specified
      (store! xml (intersection 'words cache) words)
      (store! xml (intersection 'roots cache) roots)
      (store! xml (intersection 'refs cache) allrefs)
      (store! xml (intersection 'rootv cache) rootv)
      (store! xml (intersection 'wordv cache) wordv)
      (when #t ;; (overlaps? cache #t)
	(store! xml 'words words)
	(store! xml 'roots roots)
	(store! xml 'refs allrefs)
	(store! xml 'rootv rootv)
	(store! xml 'wordv wordv))
      ;; Update the rootmap
      (do-choices (word allwords) (add! rootmap word (rootfn word)))
      ;; Generate index features
      (choice (cons 'terms roots)
	      (cons 'words allwords)
	      (tryif (exists? rootv)
		(for-choices (phrase (get phrasemap roots))
		  (tryif (search phrase rootv)
		    (cons 'terms (seq->phrase phrase)))))))))

(defambda (getrefs text refrules stopwords)
  "Get references, typically based on capitalization"
  (tryif (not (uppercase? text))
    (filter-choices (ref (apply-refrules refrules text))
      (let* ((pos (position #\Space ref))
	     (hd (and pos (subseq ref 0 pos))))
	(and pos
	     (not (get stopwords hd))
	     (not  (get stopwords (downcase hd))))))))

(defambda (getrefs/in text language)
  (let* ((langmod (get-langmod language))
	 (stop-words (get langmod 'stop-words))
	 (refrules (cachecall compute-refrules stop-words)))
    (getrefs text refrules stop-words)))

(module-export! '{getrefs getrefs/in})

(defambda (reduce-map map slotids)
  (for-choices map
    (let ((f (frame-create #f)))
      (do-choices (slotid slotids)
	(add! f slotid (get map slotid)))
      f)))

(define (apply-refrule rule text)
  (let* ((pattern (get rule 'pattern))
	 (transformer (try (get rule 'transformer) #f))
	 (labels (get rule 'labels))
	 (matches (gather pattern text)))
    (cond ((and (fail? labels) transformer) (transformer matches))
	  ((fail? labels) matches)
	  ((and (singleton? labels) transformer)
	   (transformer (get (text->frame pattern matches) labels)))
	  (transformer
	   (transformer (reduce-map (text->frame pattern matches) labels)))
	  ((singleton? labels)
	   (get (text->frame pattern matches) labels))
	  (else (reduce-map  (text->frame pattern matches) labels)))))

(defambda (apply-refrules refrules text)
  (tryif (exists? refrules)
    (let* ((simple (reject refrules slotmap?))
	   (complex (pick refrules slotmap?))
	   (refs (gather simple text)))
      (do-choices (rule complex)
	(set+! refs (apply-refrule rule text)))
      refs)))

(add! default-indexrules *block-text-tags* dom/textanalyze)

;;; Name matchers

(define name-prefixes
  {"Dr." "Mr." "Mrs." "Ms." "Miss." "Mmme" "Fr." "Rep." "Sen."
   "Prof." "Gen." "Adm." "Col." "Lt." "Gov." "Maj." "Sgt."})

(define name-glue {"de" "van" "von" "St."})
(define name-preps {"to" "of" "from"})

(define solename
  #[PATTERN
    #((islower)  (spaces)
      (label solename (capword))
      (spaces*) {(ispunct) (eol) (islower)})
    LABELS SOLENAME])

(define uncapped-rule
  `#[PATTERN
     #({(bol) "." "," "\"" "'"} (spaces)
       (label term (capword) ,downcase))
     LABELS TERM])

(define (make-name-pattern (stop-words #f) (glue #{}))
  (let ((xstop-words (and stop-words (make-hashset))))
    (when stop-words
      (do-choices (word (hashset-elts stop-words))
	(hashset-add! xstop-words (capitalize word))))
    `(PREF #(,name-prefixes
	     (* #((spaces)
		  {(capword) ,glue
		   #((isupper) ".") #((isupper) "." (isupper) ".")}))
	     (spaces)
	     ,(if xstop-words
		  `(hashset-not ,xstop-words (capword))
		  '(capword)))
	   #(,(if xstop-words
		  `(hashset-not ,xstop-words (capword))
		  '(capword))
	     (* #((spaces)
		  {(capword) ,glue
		   #((isupper) ".") #((isupper) "." (isupper) ".")}))
	     (spaces)
	     ,(if xstop-words
		  `(hashset-not ,xstop-words (capword))
		  '(capword))))))

(define (compute-refrules stop-words)
  (let* ((basic (choice (make-name-pattern stop-words (qc))
			(make-name-pattern stop-words (qc name-glue))
			(make-name-pattern stop-words (qc name-preps))))
	 (extended
	  (for-choices (core basic)
	    (frame-create #f
	      'pattern
	       `#((islower) (spaces)
		  (label name ,core)
		  (spaces*) {(ispunct) (eol) (islower)})
	       'labels 'name))))
    (choice basic extended solename uncapped-rule)))

(define *basic-refrules* (compute-refrules #f))

;; (config! 'dom:refrules basic-name-pattern)
;; (config! 'dom:refrules simple-name-pattern)
;; (config! 'dom:refrules compound-name-pattern)
;; (config! 'dom:refrules `#[pattern ,embedded-basic-name labels name])
;; (config! 'dom:refrules `#[pattern ,embedded-simple-name labels name])
;; (config! 'dom:refrules `#[pattern ,embedded-compound-name labels name])

;;; Fixing index case

(define (adjust-text-index! doc slotid)
  "This adjusts the results of indexing after the whole \
   document has been analyzed."
  (when (test doc 'rootmap)
    (let* ((index (get doc 'index))
	   (idmap (get doc 'idmap))
	   (rootmap (get doc 'rootmap))
	   (words (get (getkeys index) slotid))
	   (caps (pick words capitalized?))
	   (lowset (choice->hashset (pick words lowercase?)))
	   (dropcaps (pick caps downcase lowset)))
      (do-choices (dropcap dropcaps)
	(%debug "Replacing capitalized " (write dropcap) " with lower case "
		(write (downcase dropcap)))
	(add! rootmap dropcap (downcase dropcap))
	(let* ((findings (find-frames index slotid dropcap))
	       (nodes (for-choices (id findings) (try (get idmap id) id))))
	  (add! nodes slotid (downcase dropcap))
	  (drop! nodes slotid dropcap)
	  (drop! index (cons slotid dropcap) findings)
	  (add! index (cons slotid (downcase dropcap)) findings))))))

(define (dom/adjust-index! doc)
  (adjust-text-index! doc 'terms))

