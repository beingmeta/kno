;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'textindex)

;;; Module for simple text analysis, including morphrules and
;;;  reference point extraction
(use-module '{texttools logger reflection})

;;; Some terminology
;;;  In the code below,
;;;   SETTINGS are the tables, rules, etc. used for text analysis
;;;   OPTIONS are the arguments to the text analysis which are used
;;;    to generate the settings

(module-export!
 '{text/keystrings
   text/getroots
   text/settings text/reduce
   text/phrasemap
   text/index text/analyze})

(define-init %loglevel %notify%)

;;;; Simple text analysis

(defambda (textanalyze text
		       wordrule
		       stopcache stopwords stoprules
		       rootcache rootset rootmaps rootfns morphrules
		       phrasemap nameinfo srules xrules xfns
		       options)
  ;; It's probably fastest to use this as a straight recursive procedure
  ;;  and keep state on the stack
  (for-choices text
    ;; Extract features
    (let* ((wordv (->vector (gather->list wordrule text)))
	   (stopv (forseq (word wordv)
		    (and (try (get stopcache word) 
			      (stopcheck word stopcache stopwords stoprules))
			 word)))
	   (rootv (forseq (word wordv i)
		    (if (elt stopv i) word
			(or
			 (try (get rootcache word)
			      (getroot word rootcache
				       rootset rootmaps rootfns morphrules))
			 {}))))
	   (xwords
	    ;; Special case extraction
	    (choice (for-choices (xtract (text->frames xrules text))
		      (for-choices (label (getkeys xtract))
			(cons label (get xtract label))))
		    (for-choices (srule srules)
		      (textsubst (gather srule text) srule))))
	   (phrases (getphrases wordv rootv phrasemap))
	   (stopwords (difference (elts stopv) #f))
	   (names (difference
		   (getnames wordv stopv nameinfo (getopt options 'trackposs))
		   stopwords)))
      (debug%watch "TEXTANALYZE" text wordv stopv rootv)
      (debug%watch "TEXTANALYZE" xwords phrases names)
      (choice (difference (elts rootv) stopwords)
	      xwords phrases names
	      (cons 'names (pickstrings names))
	      (for-choices (xfn xfns)
		(xfn text wordv stopv rootv options))
	      (tryif (overlaps? options 'keepraw)
		(cons 'words (elts wordv)))))))

(defambda (stopcheck word cache stopwords stoprules)
  (let ((result (or (get stopwords word)
		    (and (capitalized? word)
			 (get stopwords (downcase word)))
		    (textmatch (qc stoprules) word))))
    (store! cache word result)
    result))

(defambda (getroot word cache rootset rootmaps rootfns morphrules)
  (let ((result
	 (try (get rootmaps word)
	      (choice 
	       (tryif (hashset-test rootset word) word)
	       (try (pick (rootfns word) rootset)
		    (try-choices (rule morphrules)
		      (morphrule word rule rootset))))
	      (tryif (and (capitalized? word) (not (uppercase? word)))
		(try (pick (rootfns (downcase word)) rootset)
		     (try-choices (rule morphrules)
		       (morphrule (downcase word) rule rootset)))))))
    (store! cache word (try result #f))
    result))

;;; Standard extraction functions

(defambda (getphrases wordv rootv phrasemap)
  (let ((phrases {}))
    (dotimes (i (length wordv))
      (do-choices (phrase (get phrasemap (elt wordv i)))
	(when (match? phrase wordv i)
	  (set+! phrases (seq->phrase phrase))))
      (do-choices (phrase (get phrasemap (elt rootv i)))
	(unless (equal? (elt rootv i) (elt wordv i))
	  (when (match? phrase rootv i)
	    (set+! phrases (seq->phrase phrase))))))
    phrases))

(defambda (getnames wordv stopv nameinfo (trackposs #f))
  (let* ((names {}) (i 0)
	 (len (length wordv)) (margin (1- len))
	 (nameglue (get nameinfo 'glue))
	 (sbreak #t) (namestart #f) (possnamestart #f))
    (while (< i len)
      (cond ((textmatch #({"!" "." "?"} (ispunct+)) (elt wordv i))
	     ;; End of sentence
	     (when possnamestart
	       (set+! names
		      (if trackposs
			  (cons 'possname
				(seq->phrase wordv possnamestart i))
			  (seq->phrase wordv possnamestart i))))
	     (when namestart
	       (set+! names (seq->phrase wordv namestart i)))
	     (set! namestart #f)
	     (set! possnamestart #f)
	     (set! sbreak #t))
	    ((capitalized? (elt wordv i))
	     ;; Start or continue a name
	     (unless namestart
	       (if sbreak
		   (unless (elt stopv i) (set! possnamestart i))
		   (set! namestart i)))
	     (set! sbreak #f))
	    ((overlaps? (elt wordv i) nameglue)
	     ;; Possibly continue a name, saving the subname before
	     ;;  and after the glue word
	     (when possnamestart
	       (set+! names
		      (if trackposs
			  (cons 'possname (seq->phrase wordv possnamestart i))
			  (seq->phrase wordv possnamestart i))))
	     (when namestart
	       (set+! names (seq->phrase wordv namestart i)))
	     (if (and (< i margin) (capitalized? (elt wordv (1+ i))))
		 (set! namestart #f) (set! possnamestart #f)
		 (set! namestart (1+ i))))
	    ;; Numbers and digits don't close the sentence break
	    ((and sbreak (textmatch '(+ {(isdigit) (ispunct)}) (elt wordv i))))
	    ;; End of name
	    (else
	     (when possnamestart
	       (set+! names
		      (if trackposs
			  (cons 'possname (seq->phrase wordv possnamestart i))
			  (seq->phrase wordv possnamestart i))))
	     (when namestart
	       (set+! names (seq->phrase wordv namestart i)))
	     (set! namestart #f)
	     (set! possnamestart #f)
	     (set! sbreak #f)))
      (set! i (1+ i)))
    names))

;;; Exported functions

(define (text/keystrings text (options #[]))
  (info%watch "TEXT/KEYSTRINGS" settings)
  (textanalyze text (try (getopt options 'wordrule
				 (get text-settings 'default-word-rule)))
	       (try (getopt options 'stopcache {}) (make-hashtable))
	       (choice (getopt options 'stopwords {})
		       (get text-settings 'stopwords))
	       (choice (getopt options 'stoprules {})
		       (get text-settings 'stoprules))
	       (try (getopt options 'rootcache {}) (make-hashtable))
	       (choice (getopt options 'rootset {})
		       (get text-settings 'rootset))
	       (choice (getopt options 'rootmaps {})
		       (get text-settings 'rootmap))
	       (choice (getopt options 'rootfns {})
		       (get text-settings 'rootfns))
	       (choice (getopt options 'morphrules {})
		       (get text-settings 'morphrules))
	       (choice (getopt options 'phrasemap {})
		       (get text-settings 'phrasemap))
	       (choice (getopt options 'nameinfo {})
		       (get text-settings 'nameinfo))
	       (choice (getopt options 'substrules {})
		       (get text-settings 'substrules))
	       (choice (getopt options 'xrules {})
		       (get text-settings 'xrules))
	       (choice (getopt options 'xfns {})
		       (get text-settings 'xfns))
	       options))

(define (text/index! index f slotid (value) (options #[]))
  (unless (or (not (bound? value)) value) (set! value (get f slotid)))
  (let ((ks (text/keystrings value options)))
    (when (exists? ks)
      (index-frame index f slotid ks)
      (index-frame index f 'has slotid))))

(defambda (text/analyze passages options)
  ;; (info%watch "TEXT/ANALYZE" options)
  (let* ((allkeys (make-hashset))
	 (text/settings
	  (try (getopt options 'text/settings {})
	       (text/settings (getopt options 'language))
	       (text/settings (getopt options 'lang))
	       text-settings))
	 (options (cons #[trackposs #t] options))
	 (localopts (car options))
	 (table (make-hashtable))
	 (wordrule (getopt options 'wordrule (get text/settings 'wordrule)))
	 (stopcache (try (getopt options 'stopcache) (make-hashtable)))
	 (stopwords (choice (getopt options 'stopwords {})
			    (get text/settings 'stopwords)))
	 (stoprules (choice (getopt options 'stoprules {})
			    (get text/settings 'stoprules)))
	 (rootcache (try (getopt options 'rootcache) (make-hashtable)))
	 (rootset (choice (getopt options 'rootset {})
			  (get text/settings 'rootset)))
	 (rootmaps (choice (getopt options 'rootmaps {})
			   (get text/settings 'rootmap)))
	 (rootfns (choice (getopt options 'rootfns {})
			  (get text/settings 'rootfns)))
	 (morphrules (choice (getopt options 'morphrules {})
			     (get text/settings 'morphrules)))
	 (phrasemap (choice (getopt options 'phrasemap {})
			    (get text/settings 'phrasemap)))
	 (nameinfo (choice (getopt options 'nameinfo {})
			   (get text/settings 'nameinfo)))
	 (srules (choice (getopt options 'substrules {})
			 (get text/settings 'substrules)))
	 (xrules (choice (getopt options 'xrules {})
			 (get text/settings 'xrules)))
	 (xfns (choice (getopt options 'xfns {})
		       (get text/settings 'xfns)))
	 (textfns (getopt options 'textfns)))
    (debug%watch "TEXT/ANALYZE" options)
    (do-choices (passage passages)
      ;; (debug%watch "TEXT/ANALYZE" passage)
      (do-choices (text (if textfns
			    (choice ((pick textfns applicable?) passage)
				    (get passage (pick textfns slotid?)))
			    passage))
	(debug%watch "TEXT/ANALYZE" passage text)
	(let ((keys (textanalyze text wordrule
				 stopcache stopwords stoprules
				 rootcache rootset rootmaps rootfns morphrules
				 phrasemap nameinfo srules xrules xfns
				 options)))
	  (hashset-add! allkeys (pickstrings keys))
	  (add! table passage keys))))
    (do-choices (passage (getkeys table))
      (store! table passage
	      (for-choices (key (get table passage))
		(if (and (pair? key) (eq? (car key) 'possname))
		    (if (hashset-get allkeys (cdr key))
			(choice (cdr key) (cons 'names (cdr key)))
			(fail))
		    key))))
    table))

(define (text/settings language)
  "This SHOULD get the settings for a particular language"
  text-settings)

(define (text/getroots word (options #[]))
  (getroot word
	   (try (getopt options 'rootcache) (make-hashtable))
	   (choice (getopt options 'rootset {})
		   (get text-settings 'rootset))
	   (choice (getopt options 'rootmaps {})
		   (get text-settings 'rootmap))
	   (choice (getopt options 'rootfns {})
		   (get text-settings 'rootfns))
	   (choice (getopt options 'morphrules {})
		   (get text-settings 'morphrules))))

;;; Default rules

(define default-word-rule
  '(GREEDY {(isalnum+)
	    #((isalpha+) "'" (isalpha+))
	    #((isalnum) (+ #("." (isalnum+))) (opt "."))
	    (ispunct+)}))

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
    ("ed" . "")
    ("ing" . "")
    ("ly" . #((NOT> "ly") (SUBST "ly" "")))))

;;; Default ref rules

(define default-name-prefixes
  {"Dr." "Mr." "Mrs." "Ms." "Miss." "Mmme" "Fr." "Rep." "Sen."
   "Prof." "Gen." "Adm." "Col." "Lt." "Gov." "Maj." "Sgt."})

(define default-name-glue {"de" "van" "von" "St." "to" "of" "from"})

;;; Setup

(defambda (text/phrasemap . inputs)
  (let ((phrases (make-hashset))
	(phrasemap (make-hashtable)))
    (dolist (input inputs)
      (do-choices input
	(cond ((string? input)
	       (when (compound? input) (hashset-add! phrases input)))
	      ((hashset? input)
	       (hashset-add! phrases
			     (pick (pickstrings (hashset-elts input))
				   compound?)))
	      ((hashtable? input)
	       (let ((keys (getkeys input)))
		 (hashset-add! phrases (pick (pickstrings keys) compound?))
		 (do-choices (key keys)
		   (hashset-add! phrases
				 (pick (pickstrings (get input key))
				       compound?)))))
	      (else))))
    (do-choices (phrase (hashset-elts phrases))
      (let ((vec (words->vector phrase)))
	(unless (= (length vec) 1)
	  (add! phrasemap (first vec) vec))))
    phrasemap))

(define-init text-settings
  `#[wordrule ,default-word-rule
     stoprules ,default-stoprules
     stopwords ,(file->dtype (get-component "en-stops.dtype"))
     morphrules ,default-morphrules
     rootmaps ,(file->dtype (get-component "en-rootmap.dtype"))
     rootset ,(file->dtype (get-component "en-rootset.dtype"))
     phrasemap ,(text/phrasemap
		 (file->dtype (get-component "en-rootmap.dtype"))
		 (file->dtype (get-component "en-rootset.dtype")))
     nameinfo #[nameglue ,default-name-glue]])
