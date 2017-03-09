;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc. All rights reserved

(in-module 'xhtml/brico)

(use-module '{reflection texttools})
(use-module '{fdweb xhtml xhtml/clickit i18n})
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

(define goodoids
  (macro expr
    "Returns OID values with sensecats, prefetching the oids"
    `(,pick (,fetchoids (,pickoids ,(cadr expr))) 'sensecat)))

(module-export! '{attrib-true? slotid->xmlval goodoids})

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

;; Renders concepts to HTML as anchors or spans
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
  (try (if (exists? (req/get 'accepted-languages))
	   ;; If the have one or zero languages accepted,
	   ;; add in the defaults just for flash value
	   (let* ((ordered-prefs (sorted (elts (req/get 'accepted-languages))
					 cdr))
		  (langs (remove-duplicates
			  (map lookup-langid
			       (map car (->list (reverse ordered-prefs)))))))
	     (if (< (length (req/get 'accepted-languages)) 3)
		 (append langs
			 (choice->list (difference (elts default-languages)
						   (elts langs))))
	       langs))
	 default-languages)     
       default-languages))

(define (get-preferred-language)
  (first (get-preferred-languages)))

(define (get-languages (var 'languages))
  (try (req/get var) (elts (get-preferred-languages))))
(define getlanguages get-languages)
(define (get-language (var 'language))
  (if (req/test (intern (stringout "X_" var)))
      (let ((lang (req/get (intern (stringout "X_" var)))))
	(message "Switching language to " lang)
	(req/set! var lang)
	lang)
      (try (req/get var)
	   (first (get-preferred-languages)))))
(define getlanguage get-language)

(define (get-language-name language (inlang (get-language)))
  (capitalize (getid language inlang)))

;; This is to be used in browse URI generation
;; (define (get-browse-language f)
;;   (let* ((var 'language)
;; 	 (preferred (get-preferred-languages))
;; 	 (languages (try (req/get var #t) (car preferred))))
;;     languages))


;;; Language Dialogs

;; This is used to figure out the language and displaying an option box
(define (languagebox  %env (name 'language)
		      (onchange #f) (action #f) (selectbox #f)
		      (title #f))
  (let* ((var (if (symbol? name) name (string->lisp name)))
	 (language (get-language var))
	 (languages (get-languages)))
    (req/set! var language)
    (unless (overlaps? language languages)
      (req/set! 'languages (choice language languages)))
    (xmlout
      (span (class "langbox_title") (if title (xmleval title %env)))
      (span (class (if (> (choice-size languages) 3) "langbox" "langbox_rigid"))
	(span (class "hotcheck")
	  (display-checkbox var language #t onchange #f)
	  (span (class "language") (get-language-name language)))
	(doseq (lang (sorted (difference languages language)
			     get-language-name))
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
	 (languages (try (req/get var) (car preferred))))
    (if (and (fail? (req/get var)) (= (length preferred) 1) (not selectbox))
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
	 (name "LANGUAGE")
	 (languagesarg #f) (onchange #f) (action #f) (selectbox #t)
	 (highlight #f))
  (let* ((var (if (symbol? name) name (intern name)))
	 (languages (if (and (exists? languagesarg) languagesarg)
			(if (sequence? languagesarg) languagesarg
			    (sorted languagesarg get-language-name))
			(get-preferred-languages)))
	 (language (try (req/get var) (first languages))))
    ;; (xmlout "larg=" languagesarg "; languages=" languages "; language=" language)
    (xmlblock SELECT ((name "LANGUAGE")
		      (class "langbox")
		      (onchange (if onchange onchange)))
      (do-choices (lang language)
	(xmlblock OPTION
	    ((value lang)
	     (class (if (overlaps? lang highlight) "highlight"))
	     (langid (if (overlaps? lang highlight) (get lang 'iso639/1))))
	  (getid lang (get-language))
	  " (" (xmlblock tt () (pick-one (get lang 'iso639/1)))  ")"
	  (if (overlaps? lang highlight) "* ")))
       (doseq (l languages)
	 (span () l)
	 (unless (eq? l language)
	   (xmlblock OPTION
	       ((value l)
		(class (if (overlaps? l highlight) "highlight"))
		(langid (if (overlaps? l highlight) (get l 'iso639/1))))
	     (getid l (get-language))
	     " (" (xmlblock tt () (pick-one (get l 'iso639/1)))  ")"
	     (if (overlaps? l highlight) " *")))))
    (xmlout)))

(define (languagesdropbox
	 id (languagesarg #f) (onchange #f) (action #f) (selectbox #t)
	 (highlight #f))
  (let* ((var (if (symbol? id) id (intern id)))
	 (languages (if (and (exists? languagesarg) languagesarg)
			(if (sequence? languagesarg) languagesarg
			    (sorted languagesarg))
		      (get-preferred-languages)))
	 (language (try (req/get var) (car languages))))
    (xmlblock SELECT ((name id) (size 5)
		      (class "langbox")
		      (onchange (if onchange onchange))
		      "MULTIPLE")
      (do-choices (lang language)
	(xmlblock OPTION ((value lang)
			  (class (if (overlaps? lang highlight) "highlight"))
			  (langid (if (overlaps? lang highlight) (get lang 'iso639/1)))
			  "SELECTED")
	  (get-language-name lang)
	  " (" (xmlblock tt () (pick-one (get lang 'iso639/1)))  ")"
	  (if (overlaps? lang highlight) " *")))
      (doseq (l languages)
	(unless (eq? l language)
	  (xmlblock OPTION
	      ((value l)
	       (class (if (overlaps? l highlight) "highlight"))
	       (langid (if (overlaps? l highlight) (get l 'iso639/1))))
	    (get-language-name l)
	    " (" (xmlblock tt () (pick-one (get l 'iso639/1)))  ")"
	    (if (overlaps? l highlight) " *")))))
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
(define (showterm word (quoted #f))
  (xmlout (xhtml "&ldquo;") word (xhtml "&rdquo;")))

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
  (choice (if (test concepts 'words word) @1/2c1c7"English" {})
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
  (if (and (eq? language english) (test concept 'ranked))
      (let ((ranked (get concept 'ranked)))
	(append ranked
		(sorted (difference (get concept language) (elts ranked)))))
      (let ((norms (get concept (get norm-map language))))
	(append (choice->vector norms)
		(sorted (difference (get concept language) norms))))))

(define (output-words c (language (get-language))
		      (languages #{}) (wordlim #f) (shown #f)
		      (searchurl #f))
  "This outputs words to XHTML and returns the words output"
  (let ((shown (or shown {}))
	(monolingual (empty? (difference languages language)))
	(sorted-words (get-sorted-words c language))
	(count 0))
    (doseq (word sorted-words)
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
		  (xmlblock SUB ((class "langids")) langid)))
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
(define concept-label-fcn get-normterm)

;; Whether to use titles as glosses for concept displays
;; This may be set to false if, for example, richtips are being used
(define title-gloss #t)

(define (get-label-fcn)
  (try (threadget 'conceptlabel)
       (req/get 'conceptlabel)
       concept-label-fcn))

(define (title-gloss?)
  (try (threadget 'titlegloss) title-gloss))

(define (trim-gloss string)
  (if (position #\u00b6 string)
      (subseq string 0 (position #\u00b6 string))
      (if (search "\n\n" string)
	  (subseq string 0 (search "\n\n" string))
	  string)))

(define (concept->html tag (language #f) (var #f) (selected))
  (let* ((oid (if (and (pair? tag) (exists oid? (cdr tag)))
		  (cdr tag)
		  (and (oid? tag) tag)))
	 (language (or language (get-language)))
	 (dterm (tryif oid (dterm-fcn oid language)))
	 (gloss (tryif oid (trim-gloss (get-short-gloss oid language))))
	 (text (if (pair? tag) (car tag)
		   (if (string? tag) tag
		       (pick-one ((get-label-fcn) oid language)))))
	 (selected (default selected (and var (req/test var tag)))))
    (span ((class "concept")
	   (oid (if oid oid))
	   (gloss (ifexists gloss))
	   (dterm (ifexists (pick-one dterm)))
	   (resolved (if oid "yes"))
	   (title (if (not (req/get 'notitle (not title-gloss)))
		      (ifexists gloss)))
	   (tag tag)
	   (text text))
      (when var
	(if selected
	    (input type "CHECKBOX" name var value tag "CHECKED")
	    (input type "CHECKBOX" name var value tag)))
      (span ((class "taghead")) (dterm->html text)))))

(define (concept->anchor tag (url #f) (language #f) (var #f) (selected) (target #t))
  (let* ((oid (if (and (pair? tag) (oid? (cdr tag)))
		  (cdr tag)
		  (and (oid? tag) tag)))
	 (language (or language (get-language)))
	 (dterm (tryif oid (dterm-fcn oid language)))
	 (gloss (trim-gloss (tryif oid (get-short-gloss oid language))))
	 (text (if (pair? tag) (car tag)
		   (if (string? tag) tag
		       (pick-one ((get-label-fcn) oid language)))))
	 (selected (default selected (and var (req/test var tag)))))
    (anchor* (or url oid)
	((class "concept")
	 (oid (if oid oid))
	 (dterm (ifexists (pick-one dterm)))
	 (gloss (ifexists gloss))
	 (title (if (not (req/get 'notitle (not title-gloss)))
		    (ifexists gloss)))
	 (target (if target
		     (if (string? target) target
			 (if (and (req/test 'browsetarget) (req/get 'browsetarget))
			     (req/get 'browsetarget)))))
	 (text text))
      (when var
	(if selected
	    (input type "CHECKBOX" name var value tag "CHECKED")
	    (input type "CHECKBOX" name var value tag)))
      (dterm->html text))))

(define dtermdisplay-config
  (slambda (var (val))
    (cond ((not (bound? val)) dterm-fcn)
	  ((equal? dterm-fcn val))
	  (else (set! dterm-fcn val)))))
(config-def! 'dtermdisplay dtermdisplay-config)

(define conceptlabel-config
  (slambda (var (val))
    (cond ((not (bound? val)) concept-label-fcn)
	  ((equal? concept-label-fcn val))
	  (else (set! concept-label-fcn val)))))
(config-def! 'conceptlabel conceptlabel-config)

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

;;; VALUES selection

(defambda (somevalues values opts)
  (let* ((seen (getopt opts 'seen #f))
	 (hide (getopt opts 'hide seen))
	 (topvalues (getopt opts 'topvalues))
	 (showvalues (if hide
			 (reject (difference values topvalues) hide)
			 values))
	 (limit (getopt opts 'limit #f))
	 (sortfn (getopt opts 'sortby getabsfreq))
	 (sortprim (if (getopt opts 'reverse #f) sorted rsorted))
	 (sortmax (getopt opts 'sortmax #f)))
    (append (sortprim (intersection topvalues values) sortfn)
	    (if (and limit (> (choice-size showvalues) limit))
		(if (and sortmax (> (choice-size showvalues) sortmax))
		    (choice->vector (pick-n showvalues limit))
		    (subseq (rsorted showvalues sortfn) 0 limit))
		(sortprim showvalues sortfn)))))

(defambda (getsome concept slotid opts)
  (let* ((inferlevel (getopt opts 'infer 1))
	 (infermax (getopt opts 'infermax 1))
	 (seen (getopt opts 'seen #f))
	 (values (getopt opts 'value
			 (goodoids (try (get+ concept slotid inferlevel)
					(get+ concept slotid infermax)))))
	 (hide (getopt opts 'hide seen))
	 (topvalues (getopt opts 'topvalues))
	 (showvalues (if hide
			 (reject (difference values topvalues) hide)
			 values))
	 (limit (getopt opts 'limit #f))
	 (sortfn (getopt opts 'sortby getabsfreq))
	 (sortprim (if (getopt opts 'reverse #f) sorted rsorted))
	 (sortmax (getopt opts 'sortmax #f)))
    (append (sortprim (intersection topvalues values) sortfn)
	    (if (and limit (> (choice-size showvalues) limit))
		(if (and sortmax (> (choice-size showvalues) sortmax))
		    (choice->vector (pick-n showvalues limit))
		    (subseq (rsorted showvalues sortfn) 0 limit))
		(sortprim showvalues sortfn)))))

(module-export! '{somevalues getsome})

;;; SHOWSLOT

(define (showslot elt concept slotid (opts #[]))
  (let* ((language (getopt opts 'language (get-language)))
	 (label (getopt opts 'label (translateoid slotid language)))
	 (seen (getopt opts 'seen #f))
	 (anchorify (getopt opts 'anchorify))
	 (browse (getopt opts 'browse))
	 (nobrowse (getopt opts 'nowbrowse #f))
	 (showvec (cachecall getsome concept slotid opts)))
    ;; (xmlout slotid (get (within-module 'i18n translations) slotid))
    ;; (xmlout slotid " " language " " (translateoid slotid language))
    (when (or (> (length showvec) 0) (getopt opts 'showempty #f))
      (when seen (hashset-add! seen (elts showvec)))
      (xmlblock
	  `(,elt (class ,(getopt opts 'class "slot"))
		 (slotid ,(slotid->xmlval slotid)))
	  (if browse
	      (anchor* (if (string? browse) browse (browse concept slotid))
		  ((class "label")
		   (title (string-append "click to browse" ": "
					 (get-doc slotid language))))
		label)
	      (span ((class "label") (title (get-doc slotid language)))
		label))
	(doseq (v showvec i)
	  (if (> i 0) (xmlout " " (span ((class "sep")) " . ") " "))
	  (if nobrowse
	      (concept->html v language)
	      (concept->anchor v v language)))))
    (xmlout " ")))

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
      (try (smallest (pick (get slotid '%id) symbol?))
	   (smallest (pick (get slotid '%mnemonic) symbol?))
	   (smallest (pick (get slotid '%mnemonic) string?))
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
	(showslot/span concept sumterms opts) " "
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
       (%get concepts
	     '{HYPERNYM
	       @1/2ab4d{SUMTERMS} @1/2c272{GENLS} @1/2c274{PART-OF}
	       @1/2c279{MEMBER-OF} @1/2c27e{ISA}}))
      (prefetch-keys! (for-choices (language (get-languages))
			(cons language (get concepts language))))
      (xmlout))))






