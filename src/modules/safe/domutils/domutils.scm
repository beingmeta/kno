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
   dom/textual?
   dom/oidify dom/oidmap dom/nodeid
   dom/set! dom/add! dom/append!
   dom/selector dom/match dom/lookup dom/find
   dom/getrules dom/add-rule!
   dom/search dom/strip! dom/map
   dom/getmeta dom/getlinks
   dom/split-space dom/split-semi
   ->selector selector-tag selector-class selector-id
   *block-text-tags*})

(define *block-text-tags*
  (string->symbol
   '{"P"
     "LI" "DT" "BLOCKQUOTE"
     "H1" "H2" "H3" "H4" "H5" "H6"}))

;;; Random utilities

(define (dom/split-space string) (elts (segment string " ")))
(define (dom/split-semi string)
  (difference (stdspace (elts (segment string ";"))) ""))

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
	(when (test node '%%xmltag) ;; Kept raw XML info
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
	  (when (test node '%%xmltag)
	    ;; Kept raw XML info, so use it
	    (add! node '%%attribs
		  (vector aname aname stringval)))))))

(define (dom/append! node . content)
  (let ((current (try (get node '%content) '())))
    (dolist (elt content)
      (if (pair? elt)
	  (set! current (append current elt))
	  (if (and (string? elt) (has-prefix elt "<"))
	      (if (test node '%%xmltag)
		  (xmlparse elt 'keepraw)
		  (xmlparse elt))
	      (set! current (append current (list elt))))))
    (store! node '%content current)))

;;; Selector functions

;;; Currently, we're ignoring attribs (just like the Javascript version)

(defrecord selector tag (class #f) (id #f) (attribs {}))

(define xmlid #((isalpha) (opt (isalnum+)) (* #("_" (opt (isalnum+))))))

(define selector-pattern
  `{#("." (label classname ,xmlid))
    #("#" (label idname ,xmlid))
    #((bol) (label tagname ,xmlid #t))
    (GREEDY
     #("[" (label name ,xmlid #t)
       (opt
	#((label cmp {"=" "=~"})
	  (label value (not> "]"))))
       "]"))})
(module-export! 'selector-pattern)

(define (combine-attribs attrib)
  (cons (get attrib 'name)
	(if (test attrib 'cmp)
	    (if (test attrib 'cmp "=~")
		`(IS (PHRASE ,(get attrib 'value)))
		(get attrib 'value))
	    '())))

(define (dom/selector spec)
  (if (selector? spec) spec
      (if (string? spec)
	  (let ((match (text->frames selector-pattern spec)))
	    (cons-selector (try (get match 'tagname) #f)
			   (try (get match 'classname) #f)
			   (try (get match 'idname) #f)
			   (combine-attribs (pick match 'name))))
	  (if (table? spec)
	      ;; Assume this is an XML node
	      (cons-selector (try (get spec '%xmltag) #f)
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

(defambda (filter-dom-matches elt matches)
  (for-choices (match matches)
    (if (and (pair? match) (selector? (car match)))
	(tryif (dom/match elt (car match)) (cdr match))
	match)))

(define (dom/getrules table elt (dflt))
  (if (pair? table)
      (try (dom/getrules (car table) elt)
	   (dom/getrules (cdr table) elt)
	   (tryif (bound? dflt) dflt))
      (let ((tag (get elt '%xmltag))
	    (class (elts (segment (get elt 'class))))
	    (id (get elt 'id)))
	(try (filter-dom-matches elt (get table (string-append (symbol->string tag) "." class "#" id)))
	     (filter-dom-matches elt (get table (string-append (symbol->string tag) "#" id)))
	     (filter-dom-matches elt (get table (string-append "#" id)))
	     (filter-dom-matches elt (get table (string-append (symbol->string tag) "." class)))
	     (filter-dom-matches elt (get table (string-append "." class)))
	     (filter-dom-matches elt (get table tag))
	     (tryif (bound? dflt) dflt)))))

(define (dom/add-rule! table selector value)
  (let* ((selector (->selector selector))
	 (tag (or (selector-tag selector) {}))
	 (class (or (selector-class selector) {}))
	 (id (or (selector-id selector) {})))
    (add! table (try (string-append (symbol->string tag) "." class "#" id)
		     (string-append "." class "#" id)
		     (string-append (symbol->string tag) "#" id)
		     (string-append (symbol->string tag) "." class)
		     (string-append "#" id)
		     (string-append "." class)
		     tag)
	  (cons selector value))))

(defambda (dom/match elt sel)
  (for-choices elt
    (if (string? elt) #f
	(if (exists? (reject sel selector?))
	    (dom/match elt (->selector sel))
	    (try
	     (try-choices sel
	       (or (and (or (not (selector-tag sel))
			    (test elt '%xmltag (selector-tag sel))
			    (test elt '%%xmltag (selector-tag sel)))
			(or (not (selector-class sel))
			    (test elt 'class (selector-class sel)))
			(or (not (selector-id sel))
			    (test elt 'id (selector-id sel)))
			(not (try (try-choices (attrib (selector-attribs sel))
				    (if (string? (cdr attrib))
					(tryif (not (test elt (car attrib) (cdr attrib))) #t)
					(tryif (not (textsearch (cdr attrib) (get elt (car attrib))))
					  #t)))
				  #f)))
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
      (if (or (string? (car content))
	      (not (slotmap? (car content))))
	  (cons (car content) (strip-helper (cdr content) sel))
	  (if (dom/match (car content) sel)
	      (strip-helper (cdr content) sel)
	      (cons (strip-under (car content) sel)
		    (strip-helper (cdr content) sel))))
      content))

;;; Getting meta fields

(define (dom/getmeta doc field (xform) (dflt))
  (let ((elts (pick (dom/find (dom/find doc "HEAD") "META")
		    'name (choice field
				  (tryif (symbol? field)
				    (symbol->string field))))))
    (try (if (and (bound? xform) xform)
	     (xform (get elts 'content))
	     (get elts 'content))
	 (if (bound? dflt) dflt (fail)))))

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

(define (dom/textify node (embedded #f) (cache #t) (skip #f))
  (if embedded
      (if (string? node)
	  (printout (if (position #\< node)
			(decode-entities (strip-markup node))
			(decode-entities node)))
	  (if (pair? node)
	      (dolist (elt node) (dom/textify elt #t cache skip))
	      (if (table? node)
		  (unless (or (test node 'textfree)
			      (dom/match node *no-text-tags*)
			      (and skip (dom/match node skip)))
		    (printout
		      (if (overlaps? (get node '%xmltag) *line-break-tags*)
			  "\n")
		      (if (test node '%text)
			  (get node '%text)
			  (when (test node '%content)
			    (dom/textify (get node '%content) #t #f skip)))))
		  (printout node))))
      (if (test node '%text)
	  (get node '%text)
	  (if (and cache (not (pair? node)) (not (string? node)))
	      (let ((s (stringout (dom/textify node #t #f skip))))
		(store! node '%text s)
		s)
	      (stringout (dom/textify node #t #f skip))))))

(define (dom/textual? node (lim 1))
  (and (test node '%content)
       (do ((content (get node '%content) (cdr content))
	    (len 0 (+ (if (string? (car content)) (isalphalen (car content)) 0)
		      len)))
	   ((or (null? content) (> len lim))
	    (> len lim)))))

;;; OIDify

(define dompool #f)

(define (dom/oidify node (pool dompool))
  (if (not pool) node
      (if (or (oid? node) (immediate? node) (number? node) (string? node))
	  node
	  (if (slotmap? node)
	      (try (get node '%oid)
		   (let ((oid (frame-create pool))
			 (slotids (getkeys node)))
		     (store! oid '%oid oid)
		     (store! node '%oid oid)
		     (when (test node '%content)
		       (store! oid '%content
			       (forseq (elt (get node '%content)) (dom/oidify elt pool))))
		     (do-choices (slotid (difference slotids '%content))
		       (store! oid slotid (%get node slotid)))
		     (unless (test oid '%id) (store! oid '%id (dom/nodeid oid)))
		     oid))
	      (if (pair? node)
		  (if (proper-list? node)
		      (forseq (elt node) (dom/oidify elt pool))
		      (cons (qc (dom/oidify (car node) pool))
			    (qc (dom/oidify (cdr node) pool))))
		  (if (vector? node)
		      (forseq (elt node) (dom/oidify elt pool))
		      node))))))

;; This returns a hashtable containing the OID value mappings for an
;;  OIDified DOM tree.  As a bit of a kludge, as long as this is
;;  around, cleaning the OID map won't do anything.
(define (dom/oidmap node (table (make-hashtable)))
  (if (oid? node)
      (if (test table node) table
	  (begin (store! table node (oid-value node))
		 (dom/oidmap (oid-value node) table)))
      (if (or (immediate? node) (number? node) (string? node))
	  table
	  (if (slotmap? node)
	      (let ((slotids (getkeys node)))
		(do-choices (slotid (getkeys node))
		  (set! table (dom/oidmap (%get node slotid) table)))
		table)
	      (if (pair? node)
		  (dom/oidify (cdr node) (dom/oidmap (car node) table))
		  (begin
		    (when (vector? node)
		      (doseq (elt node) (set! table (dom/oidmap elt table))))
		    table))))))

(define (dom/nodeid oid)
  (stringout
    (try (get oid '%%xmltag) (get oid '%xmltag))
    (when (test oid 'class)
      ;; (->string (get oid 'class))
      (printout "." (string-subst (get oid 'class) " " ".")))
    (when (test oid 'id) (printout "#" (get oid 'id)))
    (when (test oid 'name) (printout "[NAME=" (get oid 'name) "]"))
    (do-choices (attrib (difference (get oid '%attribids) '{name class id}))
      (printout "[" attrib "]"))))

(varconfig! DOMPOOL dompool use-pool)
