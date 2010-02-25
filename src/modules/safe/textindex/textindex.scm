;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'textindex)

;;; Module for simple text analysis, including morphrules and
;;;  reference point extraction
(define id "$Id$")
(define revision "$Revision$")

(use-module '{texttools varconfig logger reflection})

;;; Some terminology
;;;  In the code below,
;;;   SETTINGS are the tables, rules, etc. used for text analysis
;;;   OPTIONS are the arguments to the text analysis which are used
;;;    to generate the settings

(module-export!
 '{text/keystrings
   text/getroots
   text/settings text/reduce
   text/index text/analyze})

;;;; Simple text analysis

(defambda (textanalyze text
		       stopcache rootcache
		       wordrules wordfns stopwords stoprules xrules
		       phrasemap rootstrings
		       rootset rootmaps rootfns morphrules
		       refrules
		       options)
  ;; It's probably fastest to use this as a straight recursive procedure
  ;;  and keep state on the stack
  ;; (%watch "TEXTANALYZE" text options)
  (for-choices text
    ;; Extract features
    (let* ((wordv (words->vector text #t))
	   (stopv (forseq (word wordv)
		    (and (try (get stopcache word) 
			      (stopcheck word stopcache stopwords stoprules))
			 word)))
	   (rootv (forseq (word wordv i)
		    (if (elt stopv i) word
			(try (get rootcache word)
			     (getroot word rootcache
				      rootstrings rootset
				      rootmaps rootfns morphrules)))))
	   (xtracts (text->frames xrules text))
	   (xwords (choice (wordfns rootv text)
			   (get xtracts 'word)
			   (textsubst (gather (qc wordrules) text)
				      (qc wordrules))))
	   (words (filter-choices (word xwords)
		    (not (try (get stopcache word) 
			      (stopcheck word stopcache stopwords
					 stoprules)))))
	   (roots (for-choices (word words)
		    (try (get rootcache word)
			 (getroot word rootcache
				  rootstrings rootset
				  rootmaps rootfns morphrules))))
	   (xrefs (getrefs text refrules stopv))
	   (refs  (filter-choices (word xrefs)
		    (not (try (get stopcache word)
			      (stopcheck word stopcache stopwords
					 stoprules)))))
	   (refroots  (for-choices (ref refs)
			(try (getroot ref rootcache
				      rootstrings rootset rootmaps
				      rootfns morphrules)
			     ref)))
	   (results (choice roots refroots)))
      ;; (%watch "TEXTANALYZE" text rootv stopv)
      ;; (%watch "TEXTANALYZE" xwords words roots xrefs refs refroots)
      (doseq (root rootv i)
	;; (%watch "CHECKROOT" root i (choice-size (get phrasemap root)))
	(unless  (elt stopv i)
	  ;; Reject stop words and don't root capitalized words
	  (if (capitalized? root)
	      (set+! results (elt wordv i))
	      (set+! results root)))
	(do-choices (phrase (get phrasemap root))
	  ;; (%watch "CHECKPHRASE" phrase root)
	  (when (search phrase rootv)
	    ;; Since this is operating over the rootv, we don't
	    ;; need to get the root of the phrase (in English at least)
	    (set+! results (seq->phrase phrase)))))
    (choice results
	    (tryif (overlaps? options 'keepraw)
	      (choice
	       (cons 'refs (qc refs))
	       (cons 'words (qchoice xwords (elts wordv)))))))))

(defambda (stopcheck word cache stopwords stoprules)
  (let ((result (or (get stopwords word)
		    (and (capitalized? word)
			 (get stopwords (downcase word)))
		    (textmatch (qc stoprules) word))))
    (store! cache word result)
    result))

(defambda (getroot word cache
		   rootstrings rootset rootmaps rootfns morphrules)
  (let ((result
	 (choice (pick rootstrings is-prefix? word)
		 (try (get rootmaps word)
		      (choice 
		       (tryif (hashset-test rootset word) word)
		       (try (pick (rootfns word) rootset)
			    (try-choices (rule morphrules)
			      (morphrule word rule rootset))))
		      (tryif (and (capitalized? word) (not (uppercase? word)))
			(try (pick (rootfns (downcase word)) rootset)
			     (try-choices (rule morphrules)
			       (morphrule (downcase word) rule rootset))))))))
    (store! cache word result)
    result))

(defambda (getrefs text refrules (stopv #[]))
  "Get references, typically based on capitalization"
  (for-choices (ref (words->vector
		     (tryif (not (uppercase? text))
		       (gather (qc refrules) text))))
    (seq->phrase
     (if (position (first ref) stopv)
	 (if (position (elt ref -1) stopv)
	     (if (< (length ref) 3) (fail)
		 (subseq ref 1 -1))
	     (if (< (length ref) 2) (fail)
		 (subseq ref 1)))
	 (if (position (elt ref -1) stopv)
	     (subseq ref 0 -1)
	     ref)))))

;;; Exported functions

(define (text/keystrings text (options #[]) (settings))
  (set! settings
	(if (bound? settings)
	    (extend-settings settings options)
	    (text/settings options)))
  ;; (%watch "TEXTANALYZE" settings)
  (textanalyze text
	       (try (get settings 'stopcache) (make-hashtable))
	       (try (get settings 'rootcache) (make-hashtable))
	       (get settings 'wordrules)
	       (get settings 'wordfns)
	       (get settings 'stopwords)
	       (get settings 'stoprules)
	       (get settings 'xrules)
	       (get settings 'phrasemap)
	       (get settings 'rootstrings)
	       (get settings 'rootset)
	       (get settings 'rootmaps)
	       (get settings 'rootfns)
	       (get settings 'morphrules)
	       (get settings 'refrules)
	       (get settings 'options)))

(define (text/settings options)
  (if (or (string? options) (symbol? options))
      (get-language-settings options)
      (if (test options 'type 'indexsettings) options
	  (extend-settings
	   (if (testopt options 'language)
	       (get-language-settings (getopt options 'language))
	       default-settings)
	   options))))

(define (text/index! index f slotid (value) (options #[]))
  (unless (or (not (bound? value)) value) (set! value (get f slotid)))
  (let ((ks (text/keystrings value options)))
    (when (exists? ks)
      (index-frame index f slotid ks)
      (index-frame index f 'has slotid))))

(defambda (text/analyze passages options)
  ;; (%watch "TEXT/ANALYZE" options)
  (let* ((allkeys {})
	 (table (make-hashtable))
	 (stopcache (try (get options 'stopcache) (make-hashtable)))
	 (rootcache (try (get options 'rootcache) (make-hashtable)))
	 (textfns (getopt options 'textfns #f))
	 (settings (text/settings options))
	 (wordrules (get settings 'wordrules))
	 (wordfns (get settings 'wordfns))
	 (stopwords (get settings 'stopwords))
	 (stoprules (get settings 'stoprules))
	 (xrules (get settings 'xrules))
	 (phrasemap (get settings 'phrasemap))
	 (rootstrings (get settings 'rootstrings))
	 (rootset (get settings 'rootset))
	 (rootmaps (get settings 'rootmaps))
	 (rootfns (get settings 'rootfns))
	 (morphrules (get settings 'morphrules))
	 (refrules (get settings 'refrules))
	 (options (get settings 'options)))
    ;; (%watch "TEXT/ANALYZE" options)
    (do-choices (passage passages)
      ;; (%watch "TEXT/ANALYZE" passage)
      (do-choices (text (if textfns
			    (choice ((pick textfns applicable?) passage)
				    (get passage (pick textfns slotid?)))
			    passage))
	;; (%watch "TEXT/ANALYZE" passage text)
	(let ((keys (textanalyze text
				 stopcache rootcache
				 wordrules wordfns
				 stopwords stoprules xrules
				 phrasemap rootstrings rootset
				 rootmaps rootfns morphrules
				 refrules options)))
	  (set+! allkeys keys)
	  (add! table passage keys))))
    (let ((stringset (choice->hashset allkeys)))
      (do-choices (passage (getkeys table))
	(store! table passage
		(choice (pick (get table passage) pair?)
			(text/reduce (pickstrings (get table passage))
				     stringset)))))
    table))

(defambda (text/reduce strings stringset)
  (let* ((caps (pick strings capitalized?))
	 (goodcaps (reject caps downcase stringset)))
    (choice (difference strings caps) goodcaps
	    (downcase (difference caps goodcaps)))))

(define (text/getroots word (settings #[]))
  (let ((settings (text/settings settings)))
    (getroot word (get settings 'rootcache)
	     (get settings 'rootstrings)
	     (get settings 'rootset)
	     (get settings 'rootmaps)
	     (get settings 'rootfns)
	     (get settings 'morphrules))))

;;; Default rules, etc.

(define module-dir (dirname (get-component "en.settings")))

(define-init language-settings (make-hashtable))

;;; Settings

(define (car-length pair) (length (car pair)))

(define (extend-settings settings options)
  (let* ((stops (getopt options 'stops {}))
	 (roots (getopt options 'roots {}))
	 (words (getopt options 'words {}))
	 (rootstrings (pickstrings roots)))
    (frame-create #f
      'type 'indexsettings
      'wordrules (choice (get settings 'wordrules)
			 (reject words applicable?))
      'wordfns (choice (get settings 'wordfns)
		       (pick words applicable?))
      'options (getopt options 'textopts {})
      'stopwords
      (let ((stopwords (make-hashset)))
	(hashset-add!
	 stopwords (hashset-elts (get settings 'stopwords)))
	(hashset-add! stopwords (pickstrings stops))
	(do-choices (hs (pick stops hashset?))
	  (hashset-add! stopwords (hashset-elts hs)))
	stopwords)
      'stoprules
      (choice (pick stops {vector? pair? applicable?})
	      (get settings 'stoprules))
      'rootfns (choice (get settings 'rootfns)
		       (pick roots applicable?))
      'rootmaps (choice (get settings 'rootmaps)
			(choice (pick roots hashtable?) (pick roots index?)))
      'rootstrings (choice (get settings 'rootstrings) rootstrings)
      'rootset
      (let ((rootset (make-hashset)))
	(hashset-add! rootset rootstrings)
	(hashset-add! rootset (hashset-elts (get settings 'rootset)))
	(do-choices (hs (pick roots hashset?))
	  (hashset-add! rootset (hashset-elts hs)))
	(do-choices (map (get settings 'rootmaps))
	  (do-choices (key (getkeys map))
	    (hashset-add! rootset (get map key))))
	rootset)
      'morphrules
      (choice (get settings 'morphrules)
	      (list (pick roots vector?))
	      (list (pick roots textclosure?))
	      (let* ((newrules (pick roots pair?))
		     (well-formed (pick newrules cdr pair?))
		     (suffix-rules (difference newrules well-formed))
		     (string-suffix-rules (pick suffix-rules car string?)))
		(choice well-formed
			(difference suffix-rules string-suffix-rules)
			(tryif (exists? string-suffix-rules)
			  (->list (rsorted string-suffix-rules car-length))))))
      'phrasemap
      (choice (getopt options 'phrasemaps {})
	      (get settings 'phrasemap)
	      (tryif (exists? (getopt options 'phrases {}))
		(let ((phrasemap (make-hashtable))
		      (phrases (choice (getopt options 'phrases {})
				       (pick rootstrings compound?))))
		  (do-choices (phrasev (words->vector phrases))
		    (add! phrasemap (elts phrasev) phrasev))
		  phrasemap)))
      'options (try (getopt options 'textopts {}) (get settings 'options))
      'wordrules (choice (getopt options 'wordrules {})
			 (get settings 'wordrules))
      'refrules
      (choice (getopt options 'refrules {})
	      (get settings 'refrules)))))

;;; Dealing with language settings

(define (get-language-settings language)
  (try (get language-settings language)
       (let ((langname (if (symbol? language)
			   (downcase (symbol->string language))
			   (if (string? language)
			       (downcase language)
			       (fail)))))
	 (try (get language-settings langname)
	      (let ((settings (load-settings (mkpath module-dir langname))))
		(store! language-settings language settings)
		(store! language-settings langname settings)
		settings)))))

(defambda (usefile filename default)
  (if (file-exists? filename)
      (file->dtype filename)
      default))

(define (load-settings base)
  (compile-settings
   (frame-create #f
     'stopwords (usefile (string-append base ".stops")
			 (get default-settings 'stopwords))
     'stoprules (usefile (string-append base ".stoprules")
			 (get default-settings 'stoprules))
     'rootset (usefile (string-append base ".rootset")
		       (deep-copy (get default-settings 'roots)))
     'rootmaps (usefile (string-append base ".rootmap")
			(get default-settings 'rootmaps))
     'morphrules (usefile (string-append base ".morphrules")
			  (get default-settings 'morphrules))
     'refrules (usefile (string-append base ".refs")
			(get default-settings 'refrules))
     'wordrules (usefile (string-append base ".wordrules")
			 (get default-settings 'wordrules))
     'rootstrings (usefile (string-append base ".rootstrings")
			   (get default-settings 'rootstrings))
     'phrasemap (usefile (string-append base ".phrasemap")
			 (deep-copy (get default-settings 'phrasemap))))))

(define (compile-settings settings)
  (let ((rootset (try (get settings 'rootset) (make-hashset)))
	(rootmaps (get settings 'rootmaps))
	(phrasemap (try (get settings 'phrasemap) (make-hashtable))))
    (do-choices (map rootmaps)
      (do-choices (key (getkeys map))
	(hashset-add! rootset (get map key))))
    (hashset-drop! rootset (hashset-elts (get settings 'stopwords)))
    (store! settings 'rootset rootset)
    (store! settings 'phrasemap phrasemap)
    settings))


;;; Default rules

(define consrules
  (for-choices (letter {"p" "t" "b" "m" "r" "d"})
    `(subst ,(string-append letter letter) ,letter)))

(define default-stoprules
  '{(isdigit+) (isalpha) #((isalnum) (isalnum)) (ispunct+)})

(define default-morphrules
  ;; These are the rules for English and should usually be the same
  ;; as in en.morphrules
  `(("'s" . "")
    ("s'" . "s")
    ("’s" . "")
    ("s’" . "s")
    #((ISUPPER) (REST))
    ("ies" . "y")
    ("ees" . "ee")
    ,@(map (lambda (x) (cons (string-append x "es") x))
	   '("ss" "x" "z" "sh" "ge" "ch"))
    ("s" . "")
    ("ing"
     . #((NOT> #({"bb" "dd" "mm" "pp" "rr" "tt"} "ing")) (ISNOTVOWEL)
	 (SUBST #((ISNOTVOWEL) "ing") "")))
    ("ing"
     . #((NOT> #({"b" "d" "m" "p" "r" "t"} "ing")) (ISNOTVOWEL) (SUBST "ing" "e")
	 (EOS)))
    ;; ##20=
    ("ed"
     . #((NOT> #({"bb" "dd" "mm" "pp" "rr" "tt"} "ed")) (ISNOTVOWEL)
	 (SUBST #((ISNOTVOWEL) "ed") "")))
    ("ed"
     . #((NOT> #({"b" "d" "m" "p" "r" "t"} "ed")) (ISNOTVOWEL) (SUBST "ed" "e")
	 (EOS)))
    ("ed" . "e")
    ("ed" . "")
    ("ing" . "")
    ("ly" . #((NOT> "ly") (SUBST "ly" "")))))

;;; Default ref rules

(define name-prefixes
  {"Dr." "Mr." "Mrs." "Ms." "Miss." "Mmme" "Fr." "Rep." "Sen."
   "Prof." "Gen." "Adm." "Col." "Lt." "Gov." "Maj." "Sgt."})

(define name-glue {"de" "van" "von" "St."})
(define name-preps {"to" "of" "from"})

(define default-refrules
  `{#(,name-prefixes (spaces)
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (opt #({,name-glue ,name-preps} (spaces)))
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))
    #(,name-prefixes (spaces)
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))
    #((capword) (spaces)
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (opt #({,name-glue ,name-preps} (spaces)))
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))
    #((capword) (spaces)
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))})

(define solename
  #[PATTERN
    #((islower)  (spaces)
      (label solename (capword))
      (spaces*) {(ispunct) (eol) (islower)})
    LABELS SOLENAME])

;;; This pulls out initial words in their uncapitalized form,
;;;  just in case.
(define uncapped-rule
  `#[PATTERN
     #({(bol) "." "," "\"" "'"} (spaces)
       (label term (capword) ,downcase))
     LABELS TERM])

(define default-wordrules
  `{#((SUBST #(,name-prefixes (spaces)) "")
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (opt #({,name-glue ,name-preps} (spaces)))
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))
    #((SUBST #(,name-prefixes (spaces)) "")
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))
    #((SUBST #((ispunct) (spaces*)) "")
      (subst (capword) downcase))
    #((bol) (subst (capword) downcase))
    #((isalpha+) (subst "-" ""))
    #((subst "-" "") (isalpha+))})

;;; Setup

(define-init default-settings
  `#[stoprules ,default-stoprules
     stopwords ,(file->dtype (get-component "en.stops"))
     morphrules ,default-morphrules
     rootmaps ,(file->dtype (get-component "en.rootmap"))
     rootset ,(file->dtype (get-component "en.rootset"))
     wordrules ,default-wordrules
     refrules ,default-refrules])

(compile-settings default-settings)
