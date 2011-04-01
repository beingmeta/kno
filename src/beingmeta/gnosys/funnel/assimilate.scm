(in-module 'gnosys/funnel/assimilate)

(module-export! '{indexentry->concepts})
(module-export! '{update-concepts update-concepts-for index-concepts})
(module-export! '{seedmap seedmap! save-seedmap})
(module-export! '{entailsmap entailsmap! save-entailsmap})
(module-export! '{literalsmap literalsmap! save-literalsmap})

(use-module '{fdweb texttools})
(use-module '{morph morph/en})
(use-module '{brico brico/lookup})
(use-module '{gnosys gnosys/disambiguate gnosys/nlp gnosys/entify})
(use-module '{optimize mttools})

(unless (config 'booksrc #f)
  (when (file-exists? (get-component "default.cfg"))
    (load-config (get-component "default.cfg"))))

(define book-pool (use-pool (config 'BOOKSRC)))
(define book-index (use-index (config 'BOOKSRC)))

(define datadir (config 'DATADIR (get-component "data")))

(define report-ambigs #f)

;;; Seedmap

(define seedmap-file (CONFIG 'SEEDMAP (mkpath datadir "seedmap.table"))) 
(define seedmap
  (if (file-exists?  seedmap-file)
      (file->dtype seedmap-file)
      (make-hashtable)))

(define (seedmap! term concept (save #f))
  (add! seedmap term concept)
  (message (write term) " ==> " (get seedmap term))
  (when save (save-seedmap)))
(define (save-seedmap)
  (message "Saving seedmap to " seedmap-file)
  (dtype->file seedmap seedmap-file))

;; These are concepts known to be in the index
(define xcore (get seedmap (getkeys seedmap)))
(define xseedmap (deep-copy seedmap))

;;; Entailsmap

(define entailsmap-file
  (CONFIG 'ENTAILSMAP (mkpath datadir "entailsmap.table"))) 
(define entailsmap
  (if (file-exists?  entailsmap-file)
      (file->dtype entailsmap-file)
      (make-hashtable)))

(define (entailsmap! term concept (save #f))
  (add! entailsmap term concept)
  (message (write term) " ==> " (get entailsmap term))
  (when save (save-entailsmap)))
(define (save-entailsmap)
  (message "Saving entailsmap to " entailsmap-file)
  (dtype->file entailsmap entailsmap-file))

;;; Literalsmap

(define literalsmap-file
  (CONFIG 'LITERALSMAP (mkpath datadir "literals.table"))) 
(define literalsmap
  (if (file-exists?  literalsmap-file)
      (file->dtype literalsmap-file)
      (make-hashtable)))

(define (literalsmap! term concept (save #f))
  (add! literalsmap term concept)
  (message (write term) " ==> " (get literalsmap term))
  (when save (save-literalsmap)))
(define (save-literalsmap)
  (message "Saving literalsmap to " literalsmap-file)
  (dtype->file literalsmap literalsmap-file))

;;; Other

(define bsmrelated (file->dtype (mkpath datadir "related.table")))
(define bsmexpansions (file->dtype (mkpath datadir "expansions.table")))

;;; Data sources

(define concept-index-filename (append (config 'booksrc) "_concepts.index"))
(define concept-index
  (if (file-exists? concept-index-filename)
      (use-index concept-index-filename)
      (begin (make-hash-index concept-index-filename 1000000)
	     (use-index concept-index-filename))))
(define ci concept-index)

(define (big-prefetch)
  (prefetch-oids! (pool-elts book-pool))
  (let ((words (get (pool-elts book-pool) 'words)))
    (prefetch-keys! (cons {@?en_norm @?en} words))))

(define (inv-get table value)
  (filter-choices (key (getkeys table))
    (test table key value)))

;;; Simple NLP (assuming English)

(define preps {"of" "by" "in" "from" "for" "with"})
(define inpreps (string-append " " preps " "))
(define prepreps (string-append preps " "))
(define postpreps (string-append " " preps))

(define (compound-root term)
  (try ;; e.g. "in addresses"
       (tryif (exists has-prefix term prepreps)
	      (let ((stripped
		     (stdspace (subseq term (position #\Space term)))))
		(try (noun-root stripped) stripped)))
       ;; e.g. "active verbs for"
       (tryif (exists has-suffix term postpreps)
	      (let ((stripped
		     (stdspace (subseq term 0 (rposition #\Space term)))))
		(try (noun-root stripped) stripped)))
       ;; e.g. "sentences"
       (noun-root term)))

(define (strip-literals string)
  (textsubst string '(SUBST #("'" (not> "'") "'") "")))
(define (get-literals string)
  (gather #("'" (not> "'") "'") string))

(define (indexterm->ambentries term (map seedmap))
  (let* ((entries (choice (probe->ambentry term @?en)
			  (text->ambentries term @?en))))
    (ambentry/handmap entries map)))

(define (indexentry->ambentries indexentry (map seedmap))
  (let* ((entries (indexterm->ambentries (get indexentry 'terms) map))
	 (initresolved (ambentry-resolved entries))
	 (frags (choice (get indexentry 'frags)  (get indexentry 'terms)))
	 (words (choice frags (noun-root frags) (verb-root frags)))
	 (given (for-choices (word words)
		  (try (get map (cons word initresolved))
		       (get map (cons word words))
		       (get map word))))
	 (exclude (for-choices (word words)
		    (try (get map (cons* 'not word initresolved))
			 (get map (cons* 'not word words))
			 (get map (cons 'not word)))))
	 (dentries
	  (disambiguate
	   (ambentry/exclude
	    (ambentry/given entries given)
	    exclude))))
    dentries))

(define (getcompounds string)
  (let* ((wordv (words->vector (choice string (downcase string))))
	 (frags (vector->frags wordv (length wordv))))
    (seq->phrase (reject (remove #f frags) length 1))))

;; (define (indexentry->concepts indexentry (map seedmap))
;;   (let* ((entries (indexterm->ambentries (get indexentry 'terms) map))
;; 	 (initresolved (ambentry-resolved entries))
;; 	 (frags (choice (get indexentry 'frags)
;; 			(get indexentry 'terms)
;; 			(getcompounds (get indexentry 'terms))))
;; 	 (words (choice frags (noun-root frags) (verb-root frags)))
;; 	 (given (choice
;; 		 (for-choices (word words)
;; 		   (try (get map (cons word initresolved))
;; 			(get map (cons word words))
;; 			(get map word)))
;; 		 initresolved))
;; 	 (exclude (for-choices (word words)
;; 		    (try (get map (cons* 'not word initresolved))
;; 			 (get map (cons* 'not word words))
;; 			 (get map (cons 'not word)))))
;; 	 (literal-concepts (get literalsmap (get indexentry 'literals)))
;; 	 (dentries
;; 	  (disambiguate
;; 	   (ambentry/exclude
;; 	    (ambentry/given entries given)
;; 	    exclude)))
;; 	 (resolved (ambentry-resolved dentries)))
;;     (pickoids (difference
;; 	       (choice given literal-concepts
;; 		       (intersection resolved xcore)
;; 		       (get entailsmap resolved)
;; 		       (get entailsmap (?? @?specls* resolved)))
;; 	       exclude))))

(define (strip-literal-fragments string)
  (textsubst string '(SUBST #("'" (not> "'") (opt #("'" (isalpha) (not> "'")))  "'") " ")))

(define (indexentry->concepts indexentry (map seedmap))
  (let* ((terms (get indexentry 'terms))
	 (wordvec (words->vector (strip-literal-fragments terms)))
	 (phrases (rsorted (vector->frags wordvec 3 #f) length))
	 (allwords (elts (getwords terms)))
	 (words-used {})
	 (concepts {}))
    (doseq (phrase phrases)
      (let* ((string (seq->phrase phrase))
	     (roots (choice string (noun-root string) (verb-root string))))
	(unless (or (hashset-get stop-words string)
		    (overlaps? roots words-used)
		    (overlaps? (elts phrase) words-used))
	  (let* ((given (try (get map (cons string allwords))
			     (get map (cons roots allwords))
			     (get map roots)))
		 (meanings
		  (pickoids
		   (try given
			(get map roots)
			(tryif (not (test map roots #f))
			       (let ((poss (try (lookup-word string @?en 1)
						(tryif (capitalized? string)
						       (lookup-word (downcase string) @?en 1)))))
				 (try (singleton poss)
				      (singleton (filter-choices poss
						   (overlaps? (?? @?specls* poss) xcore))))))))))
	    ;; (%watch string given meanings)
	    (cond ((or (exists? given) (singleton? meanings))
		   ;; (choice meanings (funnel-query meanings))
		   (set+! concepts (or meanings {}))
		   (set+! words-used (get (or meanings {}) @?en))
		   (when meanings (set+! words-used (elts phrase))))
		  ((exists? meanings)
		   (when report-ambigs
		     (message "Cannot resolve " (write string) " among"
			      (do-choices (m meanings) (xmlout " " m))))))))))
    (choice concepts (get literalsmap (get-literals terms)))))

(defambda (find-missing-maps terms)
  (do-choices (term terms)
    (let ((meanings (lookup-word term @?en #t)))
      (when (and (exists? meanings) (not (overlaps? meanings xcore)))
	(lineout ";; Possibly Missing map for " term)
	(when (singleton? meanings)
	  (pprint `(add! seedmap
			 ,(try (noun-root term) (verb-root term) term)
			 ,(brico/lookup term)))
	  (lineout))))))

(defslambda (report-concepts entry concepts)
  (message "Found " (choice-size concepts) " concepts for " entry)
  (do-choices (concept concepts)
    (message "      " concept)))

(define (update-concepts-for indexentry (smap seedmap))
  (message "Updating concepts for " indexentry)
  (let* ((concepts (indexentry->concepts indexentry smap))
	 (concepts* (choice concepts (?? @?specls* concepts)))
	 (concepts+ (choice concepts (pickoids (get entailsmap concepts*)))))
    (report-concepts indexentry (qc concepts))
    (store! indexentry 'concepts concepts+)
    ;; (add! indexentry 'concepts (get handmap indexentry))
    ;; (drop! indexentry 'concepts (get handmap (cons 'not indexentry)))
    ))

(define (update-concepts (smap seedmap))
  (do-choices (ie (?? 'type 'indexentry))
    (update-concepts-for ie smap)))

;;; Indexing concepts

(define (index-concepts (index concept-index))
  (do-choices (entry (find-frames book-index 'type 'indexentry))
    (index-entry-concepts entry index)))

(define (index-entry-concepts entry (index concept-index))
  (let* ((refs (get entry 'refs))
	 (pages (get refs 'pages))
	 (concepts (get entry 'concepts))
	 (concepts* (choice concepts (?? @?specls* concepts)))
	 (concept (difference concepts (get (get entry 'super) 'concepts)))
	 (conceptsv (sorted concepts))
	 (conceptv (sorted concept))
	 (entailed (choice concepts*
			   (car (pick (get entailsmap concepts*) pair?)))))
    ;; Index concepts directly
    (index-frame concept-index (choice entry refs pages)
      'concepts concepts)
    ;; Index the pair (LIST concept) to indicate that concept
    ;;  is directly defined on the entry/ref/page
    (index-frame concept-index (choice entry refs pages)
      'concepts (list concepts))
    ;; Index under all the genls of the concept
    (index-frame concept-index (choice entry refs pages)
      'concepts entailed)
    ;; Index derived slots
    (index-frame concept-index entry 'concept concept)
    (index-frame concept-index entry 'conceptsv conceptsv)
    (index-frame concept-index entry 'conceptv conceptv)))

;;; Importing audit entries

(define (import-audit entry)
  (let ((terms (car entry)))
    (dolist (value (cdr entry))
      (if (oid? value) (add! seedmap terms value)
	  (if (string? value)
	      (if (test seedmap value)
		  (add! seedmap terms (get seedmap value))
		  (warning "No entry in seedmap for " (write value)))
	      (warning "Odd audit entry " (write value) " for " terms))))))

