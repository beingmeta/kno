;;; -*- Mode: Scheme; -*-

(in-module 'domutils/index)

(use-module '{reflection fdweb xhtml texttools domutils varconfig})

(module-export! '{dom/index! dom/textanalyze})
(module-export! '{apply-refrules})

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
  (let* ((text (dom/textify xml))
	 (rootfn (try (get settings 'rootfn) default-rootfn))
	 (refrules (try (get settings 'refrules) default-refrules))
	 (refstops (get settings 'refstops))
	 (phrasemap (get settings 'phrasemap))
	 (allcaps (uppercase? text))
	 (refs (reject (tryif (not allcaps)
			      (apply-refrules refrules text))
		       refstops))
	 (wordv (words->vector text))
	 (rootv (and (and rootfn (map rootfn wordv))))
	 (refwords (elts (words->vector refs)))
	 (words (difference (elts wordv) refwords))
	 (roots (difference (elts rootv) refwords)))
    (when (exists? refs) (store! xml 'refs refs))
    (store! xml 'words wordv)
    (store! xml 'roots rootv)
    (choice (cons 'refs refs)
	    (cons 'words words)
	    (cons 'roots roots)
	    (tryif (exists? phrasemap)
		   (for-choices (phrase (get phrasemap words))
		     (tryif (search phrase wordv)
			    (cons 'words (seq->phrase phrase)))))
	    (tryif (exists? phrasemap)
		   (for-choices (phrase (get phrasemap roots))
		     (tryif (search phrase rootv)
			    (cons 'words (seq->phrase phrase))))))))

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
