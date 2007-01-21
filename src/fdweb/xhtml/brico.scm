;;; -*- Mode: scheme; Text-encoding: latin-1; -*-

(in-module 'xhtml/brico)

(use-module '{fdweb xhtml xhtml/clickit brico})

;; We use these for languages (at least)
(define sub* markup*fn)
(define sup* markup*fn)

(define (attrib-true? x)
  (and x (overlaps? x {#t "yes" "true" "on" "yeah" "oui" "ja" "si"})))

;; Language related exports
(module-export! '{getlanguages getlanguage get-languages get-language})

;; Dialogs
(module-export! '{languagebox languagesbox languagedropbox languagesdropbox})

;; Displaying words with language subscripts
(module-export! '{showterm wordform output-words just-output-words})

;; Displaying concepts in various ways
(module-export! '{showconcept})

;;;; Getting language information

(define all-languages (?? 'type 'language))
(define default-languages
  (map (lambda (x) (?? 'langid x)) '(en es nl pt fi fr de it)))
(define default-language (?? 'langid 'en))

(define (lookup-langid langid)
  (try (if (string? langid) (?? 'langid (intern langid)) (fail))
       (?? 'langid langid)
       (?? 'langid (string-downcase langid))
       (?? 'langid (subseq langid 0 2))
       (?? 'langid (string-downcase (subseq langid 0 2)))))

(define (remove-duplicates lst)
  (if (null? lst) '()
      (if (member? (car lst) (cdr lst)) (remove-duplicates (cdr lst))
	  (cons (car lst) (remove-duplicates (cdr lst))))))

(define (get-preferred-languages)
  (try (if (exists? (cgiget 'accepted-languages))
	   ;; If the have one or zero languages accepted,
	   ;; add in the defaults just for flash value
	   (let* ((ordered-prefs (sorted (elts (cgiget 'accepted-languages))
					 cdr))
		  (langs (remove-duplicates
			  (map lookup-langid
			       (map car (vector->list (reverse ordered-prefs)))))))
	     (if (< (length (cgiget 'accepted-languages)) 3)
		 (append langs
			 (choice->list (difference (elts default-languages)
						   (elts langs))))
	       langs))
	 default-languages)     
       default-languages))

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
	       (xmlblock OPTION (value l) (get-language-name l))))))
       (if action
	   (xmlelt 'input 'type "SUBMIT" 'name 'action 'value
		   action))))
    (xmlout)))

(define (languagesbox
	 name (onchange #f) (action #f) (selectbox #t) (multiple #t))
  (let* ((var (getsym name))
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
	  (unless (member? language preferred)
	    (span (class "nobreak")
	      (display-checkbox var language (overlaps? language languages) onchange #t)
	      (span (class "language") (get-language-name language)))
	    (xmlout "  ")))
	(when (true-string? selectbox)
	  (selection (name var)
		     (doseq (l (sorted  all-languages get-language-name))
		       (option l (get-language-name l)))))
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
	 (unless (eq? l language)
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


(define (display-checkbox var val selected onclick multi)
  (if onclick
      (xmlempty 'input
		'type (if multi "checkbox" "radio")
		'name (symbol->string var) 'value val
		'onclick onclick
		(if selected "checked" ""))
      (xmlelt 'input
	      'type (if multi "checkbox" "radio")
	      'name (symbol->string var)
	      'value val
	      (if selected "checked" "") )))

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
      (let ((languages (get-languages-for word (qc concept))))
	(span ((class "wordform")
	       (xml:lang (get (pick-one languages) 'iso639/1)))
	  (cond ((not action) (showterm word))
		((string? action)
		 (anchor (fdscripturl action 'word word 'language languages)
		   (showterm word)))
		((applicable? action)
		 (anchor (action word) (showterm word)))
		(else (showterm word)))
	  (display-langids (qc languages))))
      (span (class "wordform")
	(cond ((not action) (showterm word))
	      ((string? action)
	       (anchor (fdscripturl action 'word word) (showterm word)))
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
	       (get languages 'iso639/1)))
	(else
	 (sub* ((class "langids"))
	       (doseq (elt (sorted languages 'iso639/1) i)
		 (if (> i 0) (xmlout ";"))
		 (span ((title (get-langname elt)))
		   (get elt 'iso639/1)))))))

;;; Displaying concepts

(define (get-sorted-words concept language)
  (try (tryif (eq? language @?english) (get concept 'ranked))
       (append (choice->vector (get concept (get norm-map language)))
	       (sorted (get concept language)
		       (lambda (x) (choice-size (?? language x)))))))

(define (output-words c language
		      (languages #{}) (wordlim #f) (shown #f)
		      (searchurl #f))
  "This outputs words to XHTML and returns the words output"
  (let ((shown (or shown {}))
	(count 0))
    (doseq (word (get-sorted-words c language))
      (cond ((overlaps? word shown))
	    ((or (not wordlim) (< count wordlim))
	     (xmlout
	       (if (> count 0) " . ")
	       (wordform word c searchurl)
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
	      (else))))
    (do-choices (word (get c 'names))
      (cond ((overlaps? word shown))
 	    ((or (not wordlim) (< count wordlim))
 	     (xmlout
 	      (if (> count 0) " . ")
 	      (span ((class "wordform")) (showterm word)) " ")
 	     (set+! shown word)
 	     (set! count (1+ count)))
 	    (else)))
    (if wordlim shown)))

(define (just-output-words c language
			   (languages #{}) (wordlim #f) (shown #f)
			   (searchurl #f))
  (output-words c language languages wordlim (qc shown) searchurl)
  (xmlout))


;;; Showing concepts

(define (showconcept c (language #f) (expansion #f) (wordlim 2))
  (if (fail? c) (xmlout)
    (let ((languages (or language (get-languages 'language)))
	  (expval (if expansion (get c expansion) (fail))))
      (anchor* c (title (stdspace (get-gloss c (qc languages))) class "concept")
	       (just-output-words c language (qc languages) wordlim))
      (when (exists? expval)
	(span (style "cursor: help;"
		     title
		     "the concept to the right is more general than the concept to the left")
	  " « "))
      (do-choices (expval (get c expansion) i)
	(if (> i 0) (xmlout " . "))
	(if (frame? expval) (showconcept expval (qc language))
	  (xmlout expval)))
      (xmlout))))

(define (conceptsummary concept (language #f) %env xmlbody)
  (let  ((language (or language  (get-language)))
	 (languages (or language (get-languages)))
	 (seen (make-hashset))
	 (j 0))
    (div (class "conceptsummary")
      (P* (class "head")
	  (let ((shown {})
		(all (get concept (choice language languages 'names))))
	    (anchor concept
		    (set! shown (output-words concept language languages 5)))
	    (let ((more (difference all shown)))
	      (when (exists? more)
		(xmlout " "
			(hideshow
			 (oid2id concept "WDS")
			 (stringout
			     "show " (choice-size more) " more")
			 (stringout "show "(choice-size more) " less"))
			" ")
		(span ((id (oid2id concept "WDS")) (style "display: none;"))
		  (begin
		    (output-words concept language languages #f (qc shown))
		    (xmlout))))))
	  (if (exists? (get concept 'sense-category))
	      (xmlout " "
		      (span (class "pos") (get concept 'sense-category)) " ")
	    (if (exists? (get concept 'part-of-speech))
		(xmlout " " (span (class "pos") (get concept 'part-of-speech))
			" "))))
      (when (test concept isa)
	(P (strong "isa ")
	   (do-choices (g (get concept isa) i)
	     (if (> i 0) (xmlout " . ")) (showconcept g (qc language)))))
      (when (test concept kindof)
	(P (strong "kindof ")
	   (do-choices (g (get concept kindof) i)
	     (if (> i 0) (xmlout " . ")) (showconcept g (qc language)))))
      (when (exists? (get concept part-of))
	(P (strong "partof ")
	   (do-choices (g (get concept part-of) i)
	     (if (> i 0) (xmlout " . ")) (showconcept g (qc language)))))
      (if (exists? (get concept (?? 'type 'gloss 'language language)))
	  (P* (class "gloss")
	      (get concept (?? 'type 'gloss 'language language)))
	  (if (exists? (get concept 'gloss))
	      (P* (class "gloss") (get concept 'gloss))))
      (if (exists? (get concept 'source))
	  (P (strong "source ") (get concept 'source)))
      (xmleval xmlbody %env))))

(module-export! 'conceptsummary)


