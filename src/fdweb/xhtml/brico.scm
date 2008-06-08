;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

(in-module 'xhtml/brico)

(use-module '{reflection texttools})
(use-module '{fdweb xhtml xhtml/clickit})
(use-module '{brico brico/dterms brico/analytics})

;; We use these for languages (at least)
(define sub* markup*fn)
(define sup* markup*fn)

(define (attrib-true? x)
  (and x (overlaps? x {#t "yes" "true" "on" "yeah" "oui" "ja" "si"})))

(define (slotid->xmlval slotid)
  "Generates an XML value for use with CSS attribute selectors"
  (cond ((string? slotid) string)
	((symbol? slotid) (symbol->string slotid))
	((not (oid? slotid)) (stringout slotid))
	((not (test slotid '%id)) (stringout (oid->string slotid)))
	((string? (get slotid '%id)) (get slotid '%id))
	((symbol? (get slotid '%id)) (symbol->string (get slotid '%id)))
	(else (oid->string slotid))))

(module-export! '{attrib-true? slotid->xmlval})

;; Language related exports
(module-export! '{getlanguages getlanguage get-languages get-language})
(module-export! '{get-preferred-languages get-preferred-language})

;; Dialogs
(module-export! '{languagebox languagesbox languagedropbox languagesdropbox})

;; Displaying words with language subscripts
(module-export! '{showterm wordform output-words})

;; Displaying dterms in HTML with markup around the disambiguator
;;  and subscripts for the fr$ syntax
(module-export! '{dterm->html langterm->html})

;; Displaying dterms in HTML with markup around the disambiguator
;;  and subscripts for the fr$ syntax
(module-export! '{concept->html concept->anchor})

;; Displaying concepts in various ways
(module-export! '{conceptsummary conceptsummary/prefetch!})

;;;; Getting language information

(define all-languages (?? 'type 'language))
(define default-languages
  (map (lambda (x) (?? 'langid x)) '(en es nl pt fi fr de it)))
(define default-language (?? 'langid 'en))

(define (lookup-langid langid)
  (try (if (string? langid) (?? 'langid (intern langid)) (fail))
       (?? 'langid langid)
       (?? 'langid (downcase langid))
       (?? 'langid (subseq langid 0 2))
       (?? 'langid (downcase (subseq langid 0 2)))))

(define (remove-duplicates lst)
  (if (null? lst) '()
      (if (member (car lst) (cdr lst)) (remove-duplicates (cdr lst))
	  (cons (car lst) (remove-duplicates (cdr lst))))))

(define (get-preferred-languages)
  (try (if (exists? (cgiget 'accepted-languages))
	   ;; If the have one or zero languages accepted,
	   ;; add in the defaults just for flash value
	   (let* ((ordered-prefs (sorted (elts (cgiget 'accepted-languages))
					 cdr))
		  (langs (remove-duplicates
			  (map lookup-langid
			       (map car (->list (reverse ordered-prefs)))))))
	     (if (< (length (cgiget 'accepted-languages)) 3)
		 (append langs
			 (choice->list (difference (elts default-languages)
						   (elts langs))))
	       langs))
	 default-languages)     
       default-languages))

(define (get-preferred-language)
  (first (get-preferred-languages)))

(define (get-languages (var 'languages))
  (try (cgiget var) (elts (get-preferred-languages))))
(define getlanguages get-languages)
(define (get-language (var 'language))
  (if (cgitest (intern (stringout "X_" var)))
      (let ((lang (cgiget (intern (stringout "X_" var)))))
	(message "Switching language to " lang)
	(cgiset! var lang)
	lang)
      (try (cgiget var)
	   (first (get-preferred-languages)))))
(define getlanguage get-language)

(define (get-language-name language) (get language '%id))

;; This is to be used in browse URI generation
;; (define (get-browse-language f)
;;   (let* ((var 'language)
;; 	 (preferred (get-preferred-languages))
;; 	 (languages (try (cgiget var #t) (car preferred))))
;;     languages))


;;; Language Dialogs

;; This is used to figure out the language and displaying an option box
(define (languagebox  %env (name 'language)
		      (onchange #f) (action #f) (selectbox #f)
		      (title #f))
  (let* ((var (if (symbol? name) name (string->lisp name)))
	 (language (get-language var))
	 (languages (get-languages)))
    (cgiset! var language)
    (unless (overlaps? language languages)
      (cgiset! 'languages (choice language languages)))
    (xmlout
      (span (class "langbox_title") (if title (xmleval title %env)))
      (span (class (if (> (choice-size languages) 3) "langbox" "langbox_rigid"))
	(span (class "hotcheck")
	  (display-checkbox var language #t onchange #f)
	  (span (class "language") (get-language-name language)))
	(doseq (lang (sorted (difference languages language) 'english-names))
	  (span (class "hotcheck")
	    (display-checkbox var lang #f onchange #f)
	    (span (class "language") (get-language-name lang)))
	  (xmlout "  "))
	(when (attrib-true? selectbox)
	  (span (class "nobreak")
	    (strong " or ")
	    (xmlblock SELECT (name (stringout "X_" var))
	      (xmlblock OPTION (value {}) "Pick a different language")
	      (doseq (l (sorted (difference all-languages
					    (choice language languages))
				get-language-name))
		(when (string? (get l '%id))
		  (xmlblock OPTION (value l) (get-language-name l)))))))
	(if action
	    (xmlelt 'input 'type "SUBMIT" 'name 'action 'value
		    action))))
    (xmlout)))

(define (languagesbox
	 name (onchange #f) (action #f) (selectbox #t) (multiple #t))
  (let* ((var (if (string? name) (string->symbol name) name))
	 (preferred (get-preferred-languages))
	 (languages (try (cgiget var) (car preferred))))
    (if (and (fail? (cgiget var)) (= (length preferred) 1) (not selectbox))
	(xmlout)
      (span (class (if (> (choice-size languages) 3) "langbox" "langbox_rigid"))
	(dolist (lang preferred)
	  (span (class "nobreak")
	    (display-checkbox var lang (overlaps? lang languages) onchange #t)
	    (span (class "language") (get-language-name lang)))
	  (xmlout "  "))
	(do-choices (language languages)
	  (unless (member language preferred)
	    (span (class "nobreak")
	      (display-checkbox var language (overlaps? language languages) onchange #t)
	      (span (class "language") (get-language-name language)))
	    (xmlout "  ")))
	(when (overlaps? (downcase selectbox) {"yes" "yah" "t" "#t" "1" "y"})
	  (xmlblock SELECT (name var)
	    (doseq (l (sorted  all-languages get-language-name))
	      (xmlblock option ((value l)) (get-language-name l)))))
	(if action (xmlelt 'input 'type 'submit 'name 'action 'value
			   action))))
    (xmlout)))

(define (languagedropbox
	 (name "LANGUAGE") (languagesarg #f) (onchange #f) (action #f) (selectbox #t))
  (let* ((var (if (symbol? name) name (intern name)))
	 (languages (if (and (exists? languagesarg) languagesarg)
			(if (sequence? languagesarg) languagesarg
			    (sorted languagesarg))
		      (get-preferred-languages)))
	 (language (try (cgiget var) (car languages))))
    (xmlblock SELECT ((name "LANGUAGE"))
      (do-choices (lang language)
	(xmlblock OPTION ((value lang)) (get-language-name lang)))
       (doseq (l languages)
	 (unless (or (eq? l language)
		     (not (string? (get l '%id))))
	   (xmlblock OPTION ((value l)) (get-language-name l)))))
    (xmlout)))

(define (languagesdropbox
	 id (languagesarg #f) (onchange #f) (action #f) (selectbox #t))
  (let* ((var (if (symbol? id) id (intern id)))
	 (languages (if (and (exists? languagesarg) languagesarg)
			(if (sequence? languagesarg) languagesarg
			    (sorted languagesarg))
		      (get-preferred-languages)))
	 (language (try (cgiget var) (car languages))))
    (xmlblock SELECT ((name id) (size 5) "MULTIPLE")
      (do-choices (lang language)
	(xmlblock OPTION ((value lang) "SELECTED")
	  (get-language-name lang)))
      (doseq (l languages)
	(unless (eq? l language)
	  (xmlblock OPTION ((value l)) (get-language-name l)))))
    language))


(define (display-checkbox var val selected onclick (multi #f))
  (if onclick
      (xmlempty 'input
		'type (if multi "checkbox" "radio")
		'name (symbol->string var) 'value val
		'onclick onclick
		(if selected "CHECKED" ""))
      (xmlempty 'input
		'type (if multi "checkbox" "radio")
		'name (symbol->string var)
		'value val
		(if selected "CHECKED" "") )))

;;;; Displaying words

(define (showterm word (quoted #f))
  (if quoted
      (if (position #\Space word)
	  (let ((pos (position #\Space word)))
	    (if (and (> (length word) pos) (eq? (elt word (1+ pos)) #\())
		(xmlout word)
		(xmlout "\"" word "\"")))
	  (xmlout word))
      (xmlout word)))

(define (wordform word (concept #f) (action #f))
  (if concept
      (let* ((languages (get-languages-for word (qc concept)))
	     (langid (try (get (pick-one (pick languages 'iso639/1)) 'iso639/1)
			  (pick-one (get (pick-one languages) '{iso639/b iso639/t})))))
	(span (class "wordform" xml:lang langid)
	  (cond ((not action) (showterm word))
		((string? action)
		 (anchor (fdscripturl action 'word word 'language languages 'wordsearch "yes")
		   (showterm word)))
		((applicable? action)
		 (anchor (action word) (showterm word)))
		(else (showterm word)))
	  (display-langids (qc languages))))
      (span (class "wordform")
	(cond ((not action) (showterm word))
	      ((string? action)
	       (anchor (fdscripturl action 'word word 'wordsearch "yes")
		 (showterm word)))
	      ((applicable? action)
	       (anchor (action word) (showterm word)))
	      (else (showterm word))))))

;;; Displaying language information for wordforms

(define (get-languages-for word concepts)
  (choice (if (test concepts 'words word) @?english {})
	  (get language-map
	       (for-choices (translation (get concepts '%words))
		 (if (if (string? (cdr translation))
			 (equal? word (cdr translation))
			 (equal? word (cadr translation)))
		     (car translation)
		     (fail))))))
(define (get-langname lang)
  (pick-one (get lang 'english-names)))

(define (display-langids languages)
  (cond ((empty? languages)
	 (sub* ((class "langids")) "?"))
	((= (choice-size languages) 1)
	 (sub* ((class "langids")
		(title (get-langname languages)))
	       (try (get languages 'iso639/1)
		    (get languages 'iso639/b)
		    (get languages 'iso639/t))))
	(else
	 (sub* ((class "langids"))
	       (doseq (elt (sorted languages 'iso639/1) i)
		 (if (> i 0) (xmlout ";"))
		 (span ((title (get-langname elt)))
		   (try (get elt 'iso639/1)
			(get elt 'iso639/b)
			(get elt 'iso639/t))))))))

;;; Displaying words for a concept

(define (get-sorted-words concept language)
  (try (tryif (eq? language @?english) (get concept 'ranked))
       (let ((norms (get concept (get norm-map language))))
	 (append (choice->vector norms)
		 (sorted (difference (get concept language) norms))))))

(define (output-words c (language (get-language))
		      (languages #{}) (wordlim #f) (shown #f)
		      (searchurl #f))
  "This outputs words to XHTML and returns the words output"
  (let ((shown (or shown {}))
	(monolingual (empty? (difference languages language)))
	(count 0))
    (doseq (word (get-sorted-words c language))
      (cond ((overlaps? word shown))
	    ((or (not wordlim) (< count wordlim))
	     (xmlout
	       (if (> count 0) " . ")
	       (if monolingual (wordform word) (wordform word c searchurl))
	       " ")
	     (set+! shown word)
	     (set! count (1+ count)))
	    (else)))
    (do-choices (lang (difference languages language))
      (doseq (word (get-sorted-words c lang))
	(cond ((overlaps? word shown))
	      ((or (not wordlim) (< count wordlim))
	       (xmlout 
		(if (> count 0) " . ")
	       (wordform word c searchurl)
		" ")
	       (set+! shown word)
	       (set! count (1+ count)))
	      (else))))))

;;; Outputting terms to HTML

(define (langterm->html string (start 0) (end #f))
  "This outputs a dterm for HTML, primarily converting xx$ language prefixes \
   into subscripts."
  (unless end (set! end (length string)))
  (let ((pos (or (textsearch #((isalpha) (isalpha) "$") string start end)
		 (position "$" string start end))))
    (if pos
	(let ((langid (if (eqv? (elt string pos) #\$) "en"
			  (subseq string pos (+ pos 2))))
	      (wordstart (if (eqv? (elt string pos) #\$) (1+ pos)
			     (+ 3 pos))))
	  (xmlout (subseq string start pos)
		  (subseq string wordstart end)
		  (span ((class "langid")) langid)))
	(xmlout (subseq string start end)))))

(define (dterm->html string (start 0))
  "This outputs a dterm for HTML, primarily converting xx$ language prefixes \
   into subscripts."
  (let ((disambig-start
	 (textsearch `(CHOICE "," ":" #((spaces*) "(")) string start)))
    (if disambig-start
	(begin (langterm->html string 0 disambig-start)
	       (span ((class "disambig"))
		 (langterm->html string disambig-start)))
	(if (position #\$ string) (langterm->html string)
	    (xmlout string)))))

;;; Showing concepts

(define dterm-fcn get-dterm)

(define title-gloss #t)

(define (concept->html tag (language #f) (var #f) (selected))
  (let* ((oid (if (and (pair? tag) (exists oid? (cdr tag)))
		  (cdr tag)
		  (and (oid? tag) tag)))
	 (language (or language (get-language)))
	 (dterm (tryif oid (dterm-fcn oid language)))
	 (gloss (tryif oid (pick-one (smallest (get-gloss oid language)
					       length))))
	 (text (if (pair? tag) (car tag)
		   (if (string? tag) tag
		       (get-norm oid language))))
	 (selected (default selected (and var (cgitest var tag)))))
    (span ((class "concept")
	   (oid (if oid oid))
	   (gloss (ifexists gloss))
	   (dterm (ifexists dterm))
	   (resolved (if oid "yes"))
	   (title (if title-gloss (ifexists gloss)))
	   (tag tag)
	   (text text))
      (when var
	(if selected
	    (input type "CHECKBOX" name var value tag "CHECKED")
	    (input type "CHECKBOX" name var value tag)))
      (span ((class "taghead")) text))))

(define (concept->anchor tag (url #f) (language #f) (var #f) (selected))
  (let* ((oid (if (and (pair? tag) (oid? (cdr tag)))
		  (cdr tag)
		  (and (oid? tag) tag)))
	 (language (or language (get-language)))
	 (dterm (tryif oid (dterm-fcn oid language)))
	 (gloss (pick-one
		 (tryif oid (smallest (get-gloss oid language) length))))
	 (text (if (pair? tag) (car tag)
		   (if (string? tag) tag
		       (get-norm oid language))))
	 (selected (default selected (and var (cgitest var tag)))))
    (anchor* (or url oid)
	((class "concept")
	 (oid (if oid oid))
	 (dterm (ifexists dterm))
	 (gloss (ifexists gloss))
	 (title (if title-gloss (ifexists gloss)))
	 (text text))
      (when var
	(if selected
	    (input type "CHECKBOX" name var value tag "CHECKED")
	    (input type "CHECKBOX" name var value tag)))
      text)))

(define dtermdisplay-config
  (slambda (var (val))
    (cond ((not (bound? val)) dterm-fcn)
	  ((equal? dterm-fcn val))
	  (else (set! dterm-fcn val)))))
(config-def! 'dtermdisplay dtermdisplay-config)

(define titlegloss-config
  (slambda (var (val))
    (cond ((not (bound? val)) title-gloss)
	  ((equal? title-gloss val))
	  (else (set! title-gloss val)))))
(config-def! 'titlegloss titlegloss-config)

;;;; Showing a concept with more context

#|
(define (showconcept c (language #f) (expansion #f) (wordlim 2))
  (if (fail? c) (xmlout)
    (let ((languages (or language (get-languages 'language)))
	  (expval (if expansion (get c expansion) (fail))))
      (anchor* c (title (stdspace (get-gloss c (qc languages))) class "concept")
	       (output-words c language (qc languages) wordlim))
      (when (exists? expval)
	(span (style "cursor: help;"
		     title
		     "the concept to the right is more general than the concept to the left")
	  " « "))
      (do-choices (expval (get c expansion) i)
	(if (> i 0) (xmlout " . "))
	(if (oid? expval) (showconcept expval (qc language))
	  (xmlout expval)))
      (xmlout))))
|#

;;; SHOWSLOT

(define (showslot elt concept slotid (opts #[]))
  (let* ((language (getopt opts 'language (get-language)))
	 (inferlevel (getopt opts 'infer 1))
	 (label (getopt opts 'label (getid slotid language)))
	 (values (getopt opts 'value (get+ concept slotid inferlevel)))
	 (seen (getopt opts 'seen #f))
	 (hide (getopt opts 'hide seen))
	 (topvalues (getopt opts 'topvalues))
	 (showvalues (if seen
			 (reject (difference values topvalues) hide)
			 values))
	 (anchorify (getopt opts 'anchorify))
	 (limit (getopt opts 'limit #f))
	 (sortfn (getopt opts 'sortby getabsfreq))
	 (showvec (append (rsorted (intersection topvalues values) sortfn)
			  (if (and limit (> (choice-size showvalues) limit))
			      (subseq (rsorted showvalues sortfn) 0 limit)
			      (rsorted showvalues sortfn))))
	 (browse (getopt opts 'browse))
	 (nobrowse (getopt opts 'nowbrowse #f)))
    (when (or (> (length showvec) 0) (getopt opts 'showempty #f))
      (when seen (hashset-add! seen showvalues))
      (xmlblock
	  `(,elt (class ,(getopt opts 'class "slot"))
		 (slotid ,(slotid->xmlval slotid)))
	  (when (testopt opts 'browsefirst)
	    (when (and browse (> (length showvec) 0))
	      (anchor* (if (string? browse) browse (browse concept slotid))
		  ((class "browseall"))
		(getopt opts 'browsetext "browse all"))))
	(span ((class "label") (title (get-doc slotid language))) label)
	" "
	(doseq (v showvec i)
	  (if (> i 0) (xmlout " " (span ((class "sep")) " . ") " "))
	  (if nobrowse
	      (concept->html v language)
	      (concept->anchor v v language)))
	" "
	(unless (testopt opts 'browsefirst)
	  (when (and browse (> (length showvec) 0))
	    (anchor* (if (string? browse) browse (browse concept slotid))
		((class "browseall"))
	      (getopt opts 'browsetext "browse"))))))))

(define (showslot/span concept slotid (opts #[]))
  (showslot "span" concept slotid opts))
(define (showslot/div concept slotid (opts #[]))
  (showslot "div" concept slotid opts))

(module-export! '{showslot showslot/div showslot/span})

;;; SHOWFIELD

(defambda (showfield/span concept slotid (opts #[]))
  (let ((language (get-language)))
    (do-choices slotid
      (let* ((values (if (test opts 'values)
			 (get opts 'values)
			 (get concept slotid)))
	     (label (getopt opts 'label (getid slotid language))))
	(when (or (exists? values) (getopt opts 'showempty #f))
	  (span ((class "field") (slotid (ifexists slotidattr)))
	    (if (testopt opts 'url)
		(anchor* (getopt opts 'url)
		    ((class "label")
		     (title (ifexists (get-doc slotid language))))
		  label)
		(span ((class "label")
		       (title (ifexists (get-doc slotid language))))
		  label))
	    " "
	    (do-choices (f values i)
	      (xmlout (if (> i 0) " . " " ") (concept->anchor f)))))))))

(defambda (showfield/div concept slotid (values) (showvalues) (opts #[]))
  (let ((language (get-language)))
    (do-choices slotid
      (let ((values (default values (get concept slotid)))
	    (showvalues (default showvalues (get concept slotid)))
	    (slotidattr
	     (tryif (and (oid? slotid) (exists symbol? (get slotid '%mnemonic)))
		    (try (symbol->string (pick (get slotid '%mnemonic) symbol?)))))
	    (label (getopt opts 'label (getid slotid language))))
	(when (or (exists? values) (getopt opts 'showempty #f))
	  (div ((class "field") (slotid (ifexists slotidattr)))
	    (if (testopt opts 'url)
		(anchor* (getopt opts 'url)
		    ((class "label")
		     (title (ifexists (get-doc slotid language))))
		  label)
		(span ((class "label")
		       (title (ifexists (get-doc slotid language))))
		  label))
	    " "
	    (do-choices (f values i)
	      (xmlout (if (> i 0) " . " " ") (concept->anchor f)))))))))

(module-export! '{showfield/span showfield/div})

(define (get-slotidval slotid)
  (if (oid? slotid)
      (try (smallest (pick (get slotid) symbol?))
	   (smallest (pick (get slotid) string?))
	   (oid->string slotid))
      slotid))

;;; Concept summaries

(define (conceptsummary concept (language) (languages) (%env) (xmlbody))
  (default! language (get-language))
  (default! languages (get-languages))
  (let* ((sensecat (get concept 'sense-category))
	 (seen (make-hashset))
	 (opts `#[limit 7 hide ,seen seen ,seen]))
    (div (class "conceptsummary")
      (p* ((class "head") (title "Examine this concept"))
	(let ((shown {})
	      (all (get concept (choice language languages))))
	  (anchor concept
	    (img src "/graphics/diamond12.png" alt "+")
	    (output-words concept language (qc) 5)))
	(if (exists? sensecat)
	    (xmlout " " (span (class "pos") sensecat) " ")
	    (if (exists? (get concept 'part-of-speech))
		(xmlout " "
			(span (class "pos") (get concept 'part-of-speech))
			" "))))
      ;; We do immediate data tests when generating a summary, which
      ;; means that some inferred values may be missed.  But this
      ;; makes summary output much faster.
      (p* ((class "fields"))
	(showslot/span concept always opts)
	(showslot/span concept never opts) " "
	(showslot/span concept sometimes opts) " "
	(showslot/span concept somenot opts) " "
	(showslot/span concept partof opts) " "
	(showslot/span concept defterms opts) " "
	(when (%test concept 'country)
	  (span ((class "field"))
	    (span ((class "fieldid")) "country") " "
	    (do-choices (g (%get concept 'country) i)
	      (if (> i 0) (xmlout " . ")) (concept->anchor g)))))
      (when (exists? (get-gloss concept language))
	(P* (class "gloss") (get-single-gloss concept language)))
      (if (exists? (get concept 'source))
	  (P (strong "source ") (get concept 'source)))
      (if (and (bound? xmlbody) (exists? xmlbody) xmlbody)
	  (xmleval xmlbody %env)))))

(define conceptsummary/prefetch!
  (slambda (concepts language)
    (let ((started (elapsed-time)))
      ;; We keep these separate because they're usually in the cache,
      ;;  so there's not bundling advantage.  However, we want to keep them
      ;;  here so that testing doesn't show them being fetched.
      (prefetch-oids! concepts)
      (prefetch-oids!
       (%get concepts '{hypernym @?genls @?partof
				 @?memberof @?implies @?defterms}))
      (prefetch-keys! (for-choices (language (get-languages))
			(cons language (get concepts language))))
      (xmlout))))




