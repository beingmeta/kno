;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'domutils)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM
(define version "$Id$")
(define revision "$Revision$")

(use-module '{fdweb xhtml texttools ezrecords varconfig})

(module-export!
 '{
   dom/textify
   dom/set! dom/add! dom/append!
   dom/selector dom/match dom/lookup dom/find
   dom/search dom/strip! dom/map
   dom/getmeta dom/getlinks
   ->selector selector-tag selector-class selector-id
   *block-text-tags*})

(define *block-text-tags*
  (string->symbol
   '{"P"
     "LI" "DT" "BLOCKQUOTE"
     "H1" "H2" "H3" "H4" "H5" "H6"}))

;;; DOM editing

;;; The trick here is to update the XML representation so that
;;;  XML unparsing will reflect the changing.  This means updating
;;;  in multiple ways.

(define (varycase s)
  (if (symbol? s)
      (choice (symbol->string s) (downcase (symbol->string s)))))
(define (attrib-basename s)
  (if (or (pair? s) (and (vector? s) (> (length s) 0)))
      (if (and (string? (first s)) (position #\: (first s)))
	  (subseq (first s) (1+ (position #\: (first s))))
	  s)
      (if (string? s)
	  (if (and (string? s) (position #\: s))
	      (subseq s (1+ (position #\: s)))
	      s)
	  s)))
(define (attrib-namespace s)
  (and (string? s) (position #\: s)
       (subseq s 0 (position #\: s))))

(define (dom/set! node attrib val (fdxml #f))
  (let* ((slotid (if (symbol? attrib) attrib
		     (string->lisp attrib)))
	 (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		    attrib))
	 (anames (if (symbol? attrib) (varycase attrib) attrib))
	 (attribs (try (pick (get node '%attribs) first anames)
		       (pick (get node '%attribs) attrib-basename anames)))
	 (raw-attribs
	  (try (pick (get node '%%attribs) first anames)
	       (pick (get node '%%attribs) attrib-basename anames)))
	 (stringval (if (and (not fdxml) (string? val)) val
			(unparse-arg val))))
    (when (or (ambiguous? attribs) (ambiguous? raw-attribs))
      (error "AmbiguousDOMAttribute"
	     "DOM/SET of ambiguous attribute " attrib
	     (choice attribs raw-attribs)))
    (store! node slotid val)
    (add! node '%attribids slotid)
    (if (exists? attribs)
	(begin (drop! node '%attribs attribs)
	       (add! node '%attribs (cons (car attribs) stringval)))
	(add! node '%attribs (cons aname stringval)))
    (if (exists? raw-attribs)
	(begin
	  (drop! node '%%attribs raw-attribs)
	  (add! node '%%attribs
		(vector (first raw-attribs) (second raw-attribs)
			stringval)))
	(when (test node '%%name) ;; Kept raw XML info
	  (add! node '%%attribs
		(vector aname aname stringval))))))

(define (dom/add! node attrib val (fdxml #f))
  (unless (test node (if (symbol? attrib) attrib
			 (string->lisp attrib))
		val)
    (let* ((slotid (if (symbol? attrib) attrib
		       (string->lisp attrib)))
	   (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		      attrib))
	   (anames (if (symbol? attrib) (varycase attrib) attrib))
	   (attribs (try (pick (get node '%attribs) first anames)
			 (pick (get node '%attribs) attrib-basename anames)))
	   (raw-attribs
	    (try (pick (get node '%%attribs) first anames)
		 (pick (get node '%%attribs) attrib-basename anames)))
	   (stringval (if (and (not fdxml) (string? val)) val
			  (unparse-arg val))))
      (when (or (ambiguous? attribs) (ambiguous? raw-attribs))
	(error "AmbiguousDOMAttribute"
	       "DOM/SET of ambiguous attribute " attrib
	       (choice attribs raw-attribs)))
      (store! node slotid val)
      (add! node '%attribids slotid)
      (if (exists? attribs)
	  (begin (drop! node '%attribs attribs)
		 (add! node '%attribs
		       (cons (car attribs)
			     (string-append (cdr attribs) ";" stringval))))
	  (add! node '%attribs (cons aname stringval)))
      (if (exists? raw-attribs)
	  (begin
	    (drop! node '%%attribs raw-attribs)
	    (add! node '%%attribs
		  (vector (first raw-attribs) (second raw-attribs)
			  (string-append (third raw-attribs) ";" stringval))))
	  (when (test node '%%name) ;; Kept raw XML info
	    (add! node '%%attribs
		  (vector aname aname stringval)))))))

(define (dom/append! node . content)
  (let ((current (try (get node '%content) '())))
    (dolist (elt content)
      (if (pair? elt)
	  (set! current (append current elt))
	  (if (and (string? elt) (has-prefix elt "<"))
	      (if (test node '%%name)
		  (xmlparse elt 'keepraw)
		  (xmlparse elt))
	      (set! current (append current (list elt))))))
    (store! node '%content current)))

;;; Selector functions

;;; Currently, we're ignoring attribs (just like the Javascript version)

(defrecord selector tag (class #f) (id #f) (attribs (frame-create #f)))

(define xmlid #((isalpha) (opt (isalnum+)) (* #("_" (opt (isalnum+))))))

(define selector-pattern
  `{#("." (label classname ,xmlid))
    #("#" (label idname ,xmlid))
    #((bol) (label tagname ,xmlid #t))})

(define (dom/selector spec)
  (if (selector? spec) spec
      (if (string? spec)
	  (let ((match (text->frames selector-pattern spec)))
	    (cons-selector (try (get match 'tagname) #f)
			   (try (get match 'classname) #f)
			   (try (get match 'idname) #f)))
	  (if (table? spec)
	      ;; Assume this is an XML node
	      (cons-selector (try (get spec '%name) #f)
			     (try (get spec 'class) #f)
			     (try (get spec 'id) #f))
	      (fail)))))
(define ->selector dom/selector)

(define (dom/lookup table sel (dflt))
  (if (selector? sel)
      (try (tryif (and (selector-tag sel) (selector-class sel)
		       (selector-id sel))
		  (get table (stringout (selector-tag sel)
					"." (selector-class sel)
					"#" (selector-id sel))))
	   (tryif (and (selector-tag sel) (selector-id sel))
		  (get table (stringout (selector-tag sel)
					"#" (selector-id sel))))
	   (tryif (and (selector-tag sel) (selector-class sel))
		  (get table (stringout (selector-tag sel)
					"." (selector-class sel))))
	   (tryif (selector-id sel)
		  (get table (stringout "#" (selector-id sel))))
	   (tryif (selector-class sel)
		  (get table (stringout "#" (selector-class sel))))
	   (tryif (selector-tag sel)
		  (get table (selector-tag sel)))
	   (if (bound? dflt) dflt {}))
      (if (bound? dflt)
	  (dom/lookup table (->selector sel) dflt)
	  (dom/lookup table (->selector sel)))))

(defambda (dom/match elt sel)
  (for-choices elt
    (if (string? elt) #f
	(if (exists? (reject sel selector?))
	    (dom/match elt (->selector sel))
	    (try
	     (try-choices sel
	       (or (and (or (not (selector-tag sel))
			    (test elt '%name (selector-tag sel))
			    (test elt '%%name (selector-tag sel)))
			(or (not (selector-class sel))
			    (test elt 'class (selector-class sel)))
			(or (not (selector-id sel))
			    (test elt 'id (selector-id sel))))
		   {}))
	     #f)))))

;;; Searching

(defambda (dom/find under sel (findall #t))
  "Finds all nodes matching SEL under UNDER, if FINDALL is true, \
   look under matching nodes for other matching nodes."
  (unless (exists? (pickstrings sel)) (set! sel (->selector sel)))
  (cond ((string? under) (fail))
	((ambiguous? under)
	 (for-choices under (dom/find under sel)))
	((pair? under)
	 (for-choices (elt (elts under)) (dom/find elt sel findall)))
	((and (table? under) (dom/match under sel))
	 (choice under (tryif findall
			 (for-choices (elt (elts (get under '%content)))
			   (dom/find elt sel findall)))))
	((table? under)
	 (for-choices (elt (elts (get under '%content)))
	   (dom/find elt sel findall)))
	(else (fail))))

;;; Text searching

(define (search-helper under pattern exitor)
  "Finds all text and nodes containing pattern under UNDER"
  (for-choices (elt (elts (if (pair? under) under
			      (get under '%content))))
    (if (string? elt)
	(if (textsearch pattern elt)
	    (if exitor (exitor (if (pair? under) elt under))
		(fail))
	    (fail))
	(if (table? elt)
	    (search-helper elt pattern exitor)
	    (fail)))))

(define (dom/search under pattern (all #f))
  (if all
      (search-helper under pattern #f)
      (call/cc (lambda (exitor) (search-helper under pattern exitor)))))


;;; Stripping out some elements

(defambda (dom/strip! under sel)
  "Removes all nodes matching SEL under UNDER"
  (if (fail? (reject sel selector?))
      (strip-under under sel)
      (dom/strip! under (->selector sel))))

(defambda (strip-under under sel)
  "Removes all nodes matching SEL under UNDER"
  (cond ((string? under) under)
	((pair? under) (strip-helper under sel))
	((and (table? under) (exists? (get under '%content)))
	 (let ((stripped (strip-helper (get under '%content) sel)))
	   (store! under '%content stripped)
	   under))
	(else under)))

(defambda (strip-helper content sel)
  (if (pair? content)
      (if (or (string? (car content)) (not (slotmap? (car content))))
	  (cons (car content) (strip-helper (cdr content) sel))
	  (if (dom/match (car content) sel)
	      (strip-helper (cdr content) sel)
	      (cons (strip-under (car content) sel)
		    (strip-helper (cdr content) sel))))
      content))

;;; Getting meta fields

(define (dom/getmeta doc field (dflt))
  (try (get (pick (dom/find (dom/find doc "HEAD") "META")
		  'name (choice field
				(tryif (symbol? field)
				  (symbol->string field))))
	    'content)
       (if (bound? dflt) dflt (fail))))

(define (dom/getlinks doc field (dflt))
  (let ((links (pick (dom/find (dom/find doc "HEAD") "LINK")
		     'rel field)))
    (get links 'href)))

;;; Walking the DOM

(define (map0 node fn)
  (if (pair? node)
      (dolist (elt node) (map0 elt fn))
      (begin (fn node)
	     (dolist (elt (get node '%contents))
	       (map0 elt fn)))))

(define (map1 node fn arg)
  (if (pair? node)
      (dolist (elt node) (map1 elt fn (qc arg)))
      (begin (fn node)
	     (dolist (elt (get node '%contents))
	       (map1 elt fn (qc arg))))))

(define (mapn node fn args)
  (if (pair? node)
      (dolist (elt node) (mapn elt fn args))
      (begin (apply fn node args)
	     (dolist (elt (get node '%contents))
	       (mapn elt fn args)))))

(defambda (dom/map node fn (arg) . args)
  (do-choices node
    (do-choices fn
      (if (bound? arg)
	  (if (null? args) (map1 node fn arg)
	      (mapn node fn (cons (qc arg) args)))
	  (map0 node fn)))))

;;; Textify

(define *line-break-tags*
  (choice *block-text-tags* (string->symbol '{"PRE" "BR" "HR"})))
(define *no-text-tags* {})
(varconfig! DOM:TEXTFREE *no-text-tags* ->selector choice)

(define (dom/textify node (embedded #f) (cache #t))
  (if embedded
      (if (string? node) (printout (decode-entities node))
	  (if (pair? node)
	      (dolist (elt node) (dom/textify elt #t cache))
	      (if (table? node)
		  (unless (or (test node 'textfree)
			      (dom/match node *no-text-tags*))
		    (printout
		      (if (overlaps? (get node '%name) *line-break-tags*) "\n")
		      (if (test node '%text)
			  (get node '%text)
			  (when (test node '%content)
			    (let ((s (stdspace
				      (stringout
					(dolist (elt (get node '%content))
					  (if (string? elt)
					      (printout (decode-entities elt))
					      (dom/textify elt #t cache)))))))
			      (when cache (store! node '%text s))
			      (printout s))))))
		  (printout node))))
      (stdspace (stringout (dom/textify node #t cache)))))
