;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'textindex)

;;; Module for simple text analysis, including morphrules and
;;;  reference point extraction
(define id "$Id$")
(define revision "$Revision$")

(use-module '{texttools varconfig logger reflection})

(module-export!
 '{text/keystrings text/settings text/index text/analyze text/reduce})

;;; Default rules, etc.

(define module-dir (dirname (get-component "en.settings")))

(define-init default-settings
  #[stoprules
    {(isdigit+) (isalpha) #((isalnum) (isalnum)) (ispunct+)}
    options stem])

(unless (test default-settings 'stopwords)
  (store! default-settings 'stopwords
	  (file->dtype (get-component "en.stops"))))
(unless (test default-settings 'morphrules)
  (store! default-settings 'morphrules
	  (file->dtype (get-component "en.morphrules"))))
(unless (test default-settings 'rootmaps)
  (store! default-settings 'rootmaps
	  (file->dtype (get-component "en.rootmap"))))

(define-init language-settings (make-hashtable))

;;; Top level functions

(define (text/keystrings text (options default-settings) (settings))
  (set! settings
	(if (bound? settings)
	    (extend-settings settings options)
	    (text/settings options)))
  (textanalyze text
	       (qc (get settings 'wordrules))
	       (qc (get settings 'wordfns))
	       (qc (get settings 'stopwords))
	       (qc (get settings 'stoprules))
	       (qc (get settings 'phrasemap))
	       (qc (get settings 'rootstrings))
	       (qc (get settings 'rootset))
	       (qc (get settings 'rootmaps))
	       (qc (get settings 'rootfns))
	       (qc (get settings 'morphrules))
	       (qc (get settings 'refrules))
	       (qc (get settings 'options))))

(define (text/settings options)
  (if (or (string? options) (symbol? options))
      (get-language-settings options)
      (if (test options 'type 'indexsettings) options
	  (extend-settings
	   (if (test options 'language)
	       (get-language-settings (get options 'language))
	       default-settings)
	   options))))

(define (text/index! index f slotid (value) (options #[]))
  (unless (or (not (bound? value)) value) (set! value (get f slotid)))
  (index-frame index f slotid (text/keystrings value options)))

(defambda (text/analyze passages options)
  (let* ((allkeys {})
	 (table (make-hashtable))
	 (textfns (try (get options 'textfns) #f))
	 (settings (text/settings options))
	 (wordrules (get settings 'wordrules))
	 (wordfns (get settings 'wordfns))
	 (stopwords (get settings 'stopwords))
	 (stoprules (get settings 'stoprules))
	 (phrasemap (get settings 'phrasemap))
	 (rootstrings (get settings 'rootstrings))
	 (rootset (get settings 'rootset))
	 (rootmaps (get settings 'rootmaps))
	 (rootfns (get settings 'rootfns))
	 (morphrules (get settings 'morphrules))
	 (refrules (get settings 'refrules))
	 (options (get settings 'options)))
    (message "Analyzing " (choice-size passages) " passages")
    (do-choices (passage passages)
      (message "Analyzing " (get passage 'id))
      (do-choices (text (if textfns
			    (choice ((pick textfns applicable?) passage)
				    (get passage (pick textfns slotid?)))
			    passage))
	(message "Got text for " (get passage 'id) ": " (write text))
	(let ((keys (textanalyze text wordrules wordfns
				 stopwords stoprules
				 phrasemap rootstrings rootset
				 rootmaps rootfns morphrules
				 refrules options)))
	  (message "Got keys for " (get passage 'id) ": " keys)
	  (set+! allkeys keys)
	  (add! table passage keys))))
    (let ((stringset (choice->hashset allkeys)))
      (do-choices (passage (getkeys table))
	(store! table passage (text/reduce (get table passage) stringset))))
    table))

(defambda (text/reduce strings stringset)
  (choice (reject strings capitalized?)
	  (reject (pick strings capitalized?)
		  downcase stringset)))

;;;; Simple text analysis

(defambda (textanalyze text
		       wordrules wordfns stopwords stoprules
		       phrasemap rootstrings
		       rootset rootmaps rootfns morphrules
		       refrules
		       options)
  (for-choices text
    ;; Extract features
    (let* ((wordv (words->vector text #t))
	   (stopv (forseq (word wordv)
		    (and (or (get stopwords word)
			     (and (capitalized? word)
				  (get stopwords (downcase word)))
			     (textmatch (qc stoprules) word))
			 word)))
	   (rootv (forseq (word wordv i)
		    (if (elt stopv i) word
			(try (tryif (hashset-get rootset word) word)
			     (get rootmaps word)
			     (rootfns word)
			     (try-choices (rule morphrules)
			       (apply morphrule word rule))
			     word))))
	   (results (choice (wordfns rootv text)
			    (getroot
			     (textsubst (gather (qc wordrules) text)
					(qc wordrules))
			     
			     (qc rootstrings) (qc rootset) (qc rootmaps)
			     (qc rootfns) (qc morphrules))
			    (getroot (getrefs text refrules stopv)
				     (qc rootstrings) (qc rootset) (qc rootmaps)
				     (qc rootfns) (qc morphrules)))))
      ;; (%watch text rootv stopv results stopwords rootmaps)
      (doseq (root rootv i)
	(unless (or (elt stopv i) (capitalized? root))
	  ;; Reject stop words and leave capitals to getrefs
	  (set+! results root))
	(do-choices (phrase (get phrasemap root))
	  (tryif (search phrase rootv)
	    (set+! results (seq->phrase phrase)))))
      results)))

(define (stopword? word stopwords stoprules)
  (or (get stopwords word)
      (and (capitalized? word)
	   (get stopwords (downcase word)))
      (textmatch (qc stoprules) word)))

(define (getroot word rootstrings rootset rootmaps rootfns morphrules)
  (choice (pick rootstrings is-prefix? word)
	  (if (hashset-test rootset word) word
	      (try (get rootmaps word)
		   (rootfns word)
		   (try-choices (rule morphrules)
		     (apply morphrule word rule))
		   word))))

(defambda (getrefs text refrules (stopv #[]))
  "Get references, typically based on capitalization"
  (for-choices (ref (stdspace (tryif (not (uppercase? text))
				(gather (qc refrules) text))))
    (choice (tryif (and (position #\Space ref)
			(position (subseq ref 0 (position #\Space ref)) stopv))
	      (subseq ref (1+ (position #\Space ref))))
	    ref)))

;;; Settings

(define (car-length pair) (length (car pair)))

(define (extend-settings settings options)
  (let* ((stops (get options 'stops))
	 (roots (get options 'roots))
	 (words (get options 'words))
	 (rootstrings (pickstrings roots)))
    (frame-create #f
      'type 'indexsettings
      'wordrules (choice (get settings 'wordrules)
			 (reject words applicable?))
      'wordfns (choice (get settings 'wordfns)
		       (pick words applicable?))
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
      (choice (get options 'phrasemaps)
	      (get settings 'phrasemap)
	      (tryif (exists? (get options 'phrases))
		(let ((phrasemap (make-hashtable))
		      (phrases (choice (get options 'phrases)
				       (pick rootstrings compound?))))
		  (do-choices (phrasev (words->vector phrases))
		    (add! phrasemap (elts phrasev) phrasev))
		  phrasemap)))
      'options (try (get options 'textopts) (get settings 'options))
      'wordrules (choice (get options 'wordrules) (get settings 'wordrules))
      'refrules
      (choice (get options 'refrules) (get settings 'refrules)))))

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
     'roots (usefile (string-append base ".roots")
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
    (do-choices (phrase (pick (hashset-elts rootset) compound?))
      (let ((phrasev (words->vector phrase)))
	(add! phrasemap (elts phrasev) phrasev)))
    (store! settings 'rootset rootset)
    (store! settings 'phrasemap phrasemap)
    settings))

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
      (capword))
    (capword)})

(define default-wordrules
  `{#((SUBST #(,name-prefixes (spaces)) "")
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (opt #({,name-glue ,name-preps} (spaces)))
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))
    #((SUBST #(,name-prefixes (spaces)) "")
      (* #({(capword) (+ #((isupper) "."))} (spaces)))
      (capword))})

(store! default-settings 'refrules default-refrules)
(store! default-settings 'wordrules default-wordrules)

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


;;; Setup

(compile-settings default-settings)
