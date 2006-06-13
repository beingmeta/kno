;;; -*- Mode: scheme; Text-encoding: latin-1; -*-

(in-module 'xhtml/brico)

(use-module '{fdweb xhtml brico})

(define sub* markup*fn)
(define sup* markup*fn)

;; Language related exports
(module-export!
 '{languagebox languagesbox getlanguages getlanguage get-languages get-language
   get-browse-language output-words})

;; Displaying words with language subscripts
(module-export! '{showterm wordform})

;; Displaying concepts in various ways
(module-export!
 '{
   ;; showconcepts concept-string showlattice showwords
   showconcept  })

;; SPIDERs: Dialogues for browsing concept spaces
; (module-export!
;  '{spider interpret-text-cue get-unique-concepts get-unknown-cues
; 	  get-spider-constraints})

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
	   (let* ((ordered-prefs (sortby cdr (elts (cgiget 'accepted-languages))))
		  (langs (remove-duplicates
			  (map lookup-langid (map car (vector->list (reverse ordered-prefs)))))))
	     (if (< (length (cgiget 'accepted-languages)) 3)
		 (append langs (choice->list (difference (elts default-languages) (elts langs))))
	       langs))
	 default-languages)     
       default-languages))

(define (get-languages (var languages))
  (try (cgiget var) (elts (get-preferred-languages))))
(define getlanguages get-languages)
(define (get-language (var language))
  (try (cgiget var) (car (get-preferred-languages))))
(define getlanguage get-language)

(define (display-checkbox reqdata var val selected onclick multi)
  (if onclick
      (xmltag 'input 'type (if multi 'checkbox 'radio) 'name var 'value val
	      'onclick onclick (if selected " CHECKED" ""))
    (xmltag 'input 'type (if multi 'checkbox 'radio) 'name var 'value val
	    (if selected " CHECKED" ""))))

(define (get-language-name language) (get language '%id))

;; This is to be used in browse URI generation
(define (get-browse-language f)
  (let* ((var 'language)
	 (preferred (get-preferred-languages))
	 (languages (try (cgiget var #t) (car preferred))))
    languages))
;; This is used to figure out the language and displaying an option box
(define (languagebox id %env (onchange #f) (action #f) (selectbox #t)
		     (title #f))
  (let* ((var (getsym id))
	 (preferred (get-preferred-languages))
	 (language (try (cgiget var) (car preferred))))
    (if (and (fail? (cgiget var)) (= (length preferred) 1) (not selectbox))
	(xmlout)
      (xmlout
       (span (class "langbox_title")
	 (if (string? title) (xmlout title ":")
	   (if title (xmleval title %env) (xmlout))))
       (span (class (if (> (length preferred) 3) "langbox" "langbox_rigid"))
	 (dolist (lang preferred)
	   (span (class "nobreak")
	     (display-checkbox (cgi-data) var lang
			       (eq? lang language) onchange #f)
	     (span (class "language") (get-language-name lang)))
	   (xmlout "  "))
	 (unless (member? language preferred)
	   (span (class "nobreak")
	     (display-checkbox (cgi-data) var language #t
			       onchange #f)
	     (span (class "language") (get-language-name language))))
	 (when (true-string? selectbox)
	   (selection (name var)
		      (option {} "")
		      (doseq (l (sortby get-language-name all-languages))
			(option l (get-language-name l)))))
	 (if action (xmltag 'input 'type 'submit 'name 'action 'value
			    action)))))
    language))
(define (languagesbox id (onchange #f) (action #f) (selectbox #t) (multiple #t))
  (let* ((var (getsym id))
	 (preferred (get-preferred-languages))
	 (languages (try (cgiget var) (car preferred))))
    (if (and (fail? (cgiget var)) (= (length preferred) 1) (not selectbox)) (xmlout)
      (span (class (if (> (choice-size languages) 3) "langbox" "langbox_rigid"))
	(dolist (lang preferred)
	  (span (class "nobreak")
	    (display-checkbox
	     (cgi-data) var lang (contains? lang languages) onchange #t)
	    (span (class "language") (get-language-name lang)))
	  (xmlout "  "))
	(do-choices (language languages)
	  (unless (member? language preferred)
	    (span (class "nobreak")
	      (display-checkbox
	       (cgi-data) var language (contains? language languages) onchange #t)
	      (span (class "language") (get-language-name language)))
	    (xmlout "  ")))
	(when (true-string? selectbox)
	  (selection (name var)
		     (doseq (l (sortby get-language-name all-languages))
		       (option l (get-language-name l)))))
	(if action (xmltag 'input 'type 'submit 'name 'action 'value
			   action))))
    languages))

(define (languagedropbox id (languagesarg #f) (onchange #f) (action #f) (selectbox #t))
  (let* ((var (if (symbol? id) id (intern id)))
	 (languages (if (and (exists? languagesarg) languagesarg)
			(if (sequence? languagesarg) languagesarg (sorted languagesarg))
		      (get-preferred-languages)))
	 (language (try (cgiget var) (car languages))))
    (xmlblock SELECT ((name id))
      (do-choices (lang language)
	(xmlblock OPTION ((value lang)) (get-language-name lang)))
       (doseq (l languages)
	 (unless (eq? l language)
	   (xmlblock OPTION ((value l)) (get-language-name l)))))
    language))
(module-export! 'languagedropbox)

(define (languagesdropbox id (languagesarg #f) (onchange #f) (action #f) (selectbox #t))
  (let* ((var (if (symbol? id) id (intern id)))
	 (languages (if (and (exists? languagesarg) languagesarg)
			(if (sequence? languagesarg) languagesarg (sorted languagesarg))
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
(module-export! 'languagesdropbox)

;;;; Displaying words

(define (wordform word (concept #f) (action #f))
  (cond ((not action) (inner-wordform word concept))
	((string? action)
	 (anchor (scripturl action 'word word)
		 (inner-wordform word concept)))
	((applicable? action)
	 (anchor (action word)
		 (inner-wordform word concept)))
	(else (inner-wordform word concept))))

(define (showterm word)
  (if (position #\Space word)
      (xmlout "\"" word "\"")
      (xmlout word)))

(define (inner-wordform word (concept #f))
  (if concept
      (span (class "wordform")
	(showterm word)
	(display-langids word (qc concept)))
      (span (class "wordform") (showterm word))))

;;; Displaying language information for wordforms

(define (get-languages-for word concepts)
  (choice (if (test concepts 'words word) @?english {})
	  (?? 'type 'language
	      'key
	      (for-choices (translation (get concepts
					     'translations))
		(if (equal? word (cadr translation))
		    (car translation)
		    (fail))))))
(define (get-langname lang)
  (pick-one (get lang 'english-names)))

(define (display-langids word frame)
  (let ((languages (get-languages-for word (qc frame))))
    (cond ((empty? languages)
	   (sub* ((class "langids")) "?"))
	  ((= (choice-size languages) 1)
	   (sub* ((class "langids")
		  (title (get-langname languages)))
		 (get languages 'iso639/1)))
	  (else
	   (sub* ((class "langids"))
		 (doseq (elt (sortby 'iso639/1 languages) i)
		   (if (> i 0) (xmlout ";"))
		   (span ((title (get-langname elt)))
		     (get elt 'iso639/1))))))))


;;; Displaying concepts

(define (get-sorted-words concept language)
  (try (if (eq? language @?english) (get concept 'ranked) (fail))
       (sortby (lambda (x) (choice-size (?? language x)))
	       (get concept language))))

(define (output-words c languages wordlim)
  (let ((the-first #t) (multi-lingual (> (choice-size languages) 1))
	(shown {}))
    (if (exists? (get c languages))
	(do-choices (language languages)
	  (let ((words (get-sorted-words c language)))
	    (doseq (word words i)
	      (cond ((contains? word shown))
		    ((or (not wordlim) (= (length words) (1+ wordlim)))
		     (if the-first (set! the-first #f) (xmlout " or "))
		     (if multi-lingual (wordform word c)
			 (span (class "wordform") word))
		     (set+! shown word))
		    ((= i wordlim)
		     (xmlout " " (make-string (- (length words) i) #\.)))
		    ((> i wordlim) (xmlout))
		    (else 
		     (if the-first (set! the-first #f) (xmlout " or "))
		     (if multi-lingual (wordform word c)
			 (span (class "wordform") word))
		     (set+! shown word))))))
      (let* ((words (try (get c 'words) (cdr (get c 'translations))))
	     (two-words (sorted (pick-n words 2))))
	(if (empty? words) (xmlout "?" (get c '%id) "?")
	  (if (= (choice-size words) 1)
	      (xmlout "?" words "?")
	    (if (= (choice-size words) 2)
		(xmlout "?" (elt two-words 0) " | " (elt two-words 1)
			"?")
	      (xmlout "?" (elt two-words 0) " | " (elt two-words 1)
		      " |...?"))))))))

(define (showconcept c (language #f) (expansion #f) (wordlim 2))
  (if (fail? c) (xmlout)
    (let ((languages (or language (get-languages 'language)))
	  (expval (if expansion (get c expansion) (fail))))
      (anchor+ c (title (string-trim (get-gloss c (qc languages))) class "concept")
	       (output-words c (qc languages) wordlim))
      (when (exists? expval)
	(span (style "cursor: help;"
		     title
		     "the concept to the right is more general than the concept to the left")
	  " � "))
      (do-choices (expval (get c expansion) i)
	(if (> i 0) (xmlout " . "))
	(if (frame? expval) (showconcept expval (qc language))
	  (xmlout expval)))
      (xmlout))))

(define (conceptsummary concept (language #f) . body)
  (let  ((language (or language (get-languages 'language))))
    (DIV (class "conceptsummary")
      (P* (class "head")
	  (anchor concept "@" " ")
	  (do-choices (word (get concept language) i)
	    (if (> i 0) (xmlout " . "))
	    (wordform word concept))
	  (if (exists? (get concept 'sense-category))
	      (xmlout " " (span (class "pos") (get concept 'sense-category)) " ")
	    (if (exists? (get concept 'part-of-speech))
		(xmlout " " (span (class "pos") (get concept 'part-of-speech))
			" "))))
      (P (strong "genl ")
	 (do-choices (g (get concept genls) i)
	   (if (> i 0) (xmlout " . ")) (showconcept g (qc language))))
      (when (exists? (get concept 'gloss))
	(P* (class "gloss") (get concept 'gloss)))
      (if (exists? (get concept 'source))
	  (P (strong "source ") (get concept 'source)))
      (unless (empty? (xml-content body)) (unparse-xml (xml-content body))))))

(module-export! 'conceptsummary)


