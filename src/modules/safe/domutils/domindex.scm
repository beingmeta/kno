;;; -*- Mode: Scheme; -*-

(in-module 'domutils/index)

(use-module '{reflection fdweb xhtml texttools domutils varconfig logger})

(module-export! '{dom/index! dom/adjust-index!})
(module-export! '{dom/textanalyze apply-refrules})

(define %loglevel %debug!)
(define default-dom-slots '{id class name})

(define default-indexrules (make-hashtable))
(define (indexrule-config var (val))
  (if (bound? val)
      (if (pair? val)
	  (add! default-indexrules (car val) (cdr val)))
      default-indexrules))
(config-def! 'DOM:INDEXRULES indexrule-config)

(define default-rootfn #f)
(varconfig! dom:rootfn default-rootfn)

(define default-refrules {})
(varconfig! dom:refrules default-refrules #f choice)

(define default-analyzers {})
(varconfig! dom:analyzer default-analyzers #f choice)

(defambda (dom/index! index xml (settings #[])
		      (indexslots) (cacheslots)
		      (indexrules) (useids)
		      (analyzers))
  (default! indexslots (try (get settings 'indexslots) default-dom-slots))
  (default! cacheslots (get settings 'cacheslots))
  (default! useids (try (get settings 'useids) #t))
  (default! indexrules (try (get settings 'indexrules) default-indexrules))
  (default! analyzers (try (get settings 'analyzers) default-analyzers))
  (if (pair? xml)
      (dolist (elt xml)
	(dom/index! index elt settings
		    indexslots cacheslots
		    indexrules useids analyzers))
      (when  (table? xml)
	(let ((content (get xml '%content))
	      (indexval (if useids (get xml 'id) xml))
	      (eltinfo (dom/lookup indexrules xml)))
	  (when (test settings 'idmap)
	    (add! (get settings 'idmap) (get xml 'id) xml))
	  (when (exists? indexval)
	    (do-choices (slotid (difference
				 (choice indexslots
					 (pick eltinfo symbol?)
					 (pick eltinfo pair?))
				 (tryif useids 'id)))
	      (if (pair? slotid)
		  (add! index
			(cons slotid ((cdr slotid) (get xml (car slotid))))
			indexval)
		  (add! index
			(cons slotid (get xml slotid))
			indexval)))
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
			  indexrules useids analyzers)))))))

;;;; Simple text analysis

(define (dom/textanalyze xml settings)
  ;; Get element text and initialize variables
  (let* ((text (dom/textify xml))
	 (rootfn (try (get settings 'rootfn) default-rootfn))
	 (refrules (try (get settings 'refrules) default-refrules))
	 (phrasemap (get settings 'phrasemap))
	 (stopwords (get settings 'stopwords))
	 (rootmap (get settings 'rootmap))
	 (cache (choice (get settings 'cache)
			(tryif (test settings 'cache 'textslots)
			       '{words roots rootv wordv}))))
    ;; Extract features
    (let* ((wordv (words->vector text))
	   (rootv (tryif (and (exists? phrasemap) (exists? rootfns))
		    (forseq (word wordv)
		      (if (or (get stopwords word)
			      (and (capitalized? word)
				   (get stopwords (downcase word))))
			  word (try (rootfn word) word)))))
	   (refv (getrefs text refrules stopwords))
	   (allrefs (seq->phrase refv))
	   (allwords (choice (elts wordv) allrefs))
	   (allroots (for-choices (word allwords) (try (rootfn word) word)))
	   (rejects (choice (pick allroots stopwords)
			    (pick allroots downcase stopwords)))
	   (refelts (elts refv))
	   (refroots (for-choices (word refelts) (try (rootfn word) word)))
	   (words (difference allwords refelts))
	   (roots (difference allroots (choice refelts refroots rejects))))
      ;; Cache features as specified
      (store! xml (intersection 'words cache) words)
      (store! xml (intersection 'roots cache) roots)
      (store! xml (intersection 'refs cache) allrefs)
      (store! xml (intersection 'rootv cache) rootv)
      (store! xml (intersection 'wordv cache) wordv)
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
  (tryif (not (uppercase? text))
    (filter-choices
	(refv (words->vector (apply-refrules refrules text)))
      (or (get stopwords (first refv))
	  (get stopwords (downcase (first refv)))))))

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

(define (make-name-pattern (stop-words #f) (glue #{}))
  (let ((xstop-words (and stop-words (make-hashset))))
    (when stop-words
      (do-choices (word (hashset-elts stop-words))
	(hashset-add! xstop-words (capitalize word))))
    `#(,(if xstop-words
	    `{(hashset-not ,xstop-words (capword)) ,name-prefixes}
	    `{(capword) ,name-prefixes})
       (* #((spaces)
	    {(capword) ,glue
	     #((isupper) ".") #((isupper) "." (isupper) ".")}))
       (spaces) (capword))))

(define basic-name-pattern
  (make-name-pattern #f (qc)))
(define simple-name-pattern
  (make-name-pattern #f (qc name-glue)))
(define compound-name-pattern
  (make-name-pattern #f (qc name-preps)))

(define embedded-basic-name
  `#((islower)  (spaces)
     (label name ,basic-name-pattern)
     (spaces*) {(ispunct) (eol) (islower)}))
(define embedded-simple-name
  `#((islower)  (spaces)
     (label name ,simple-name-pattern)
     (spaces*) {(ispunct) (eol) (islower)}))
(define embedded-compound-name
  `#((islower)  (spaces)
     (label name ,simple-name-pattern)
     (spaces*) {(ispunct) (eol) (islower)}))

(define solename
  #((islower)  (spaces)
    (label solename (capword))
    (spaces*) {(ispunct) (eol) (islower)}))

(config! 'dom:refrules basic-name-pattern)
(config! 'dom:refrules simple-name-pattern)
(config! 'dom:refrules compound-name-pattern)
(config! 'dom:refrules `#[pattern ,embedded-basic-name labels name])
(config! 'dom:refrules `#[pattern ,embedded-simple-name labels name])
(config! 'dom:refrules `#[pattern ,embedded-compound-name labels name])

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

