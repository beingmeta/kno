;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'domutils)

(use-module '{fdweb xhtml texttools reflection ezrecords
	      varconfig logger})

(define %loglevel %notice%)

(module-export!
 '{
   dom/textify
   dom/textual? dom/structural?
   dom/hasclass? dom/addclass! dom/dropclass!
   dom/oidify dom/oidmap dom/nodeid
   dom/get dom/set-tag! dom/set! dom/add! dom/drop!
   dom/append! dom/prepend!
   dom/remove-child! dom/remove!
   dom/replace-child! dom/replace!
   dom/selector dom/match dom/lookup dom/find dom/find->list
   dom/getrules dom/add-rule!
   dom/search dom/search/first dom/strip! dom/map dom/combine!
   dom/gethead dom/getschemas dom/getmeta dom/getlinks
   dom/split-space dom/split-semi dom/split-comma
   ->selector selector-tag selector-class selector-id
   *block-text-tags* dom/block? dom/terminal?
   dom->id id->dom})

(define *block-text-tags*
  (string->symbol
   '{"P" "LI" "DT" "DD" "BLOCKQUOTE"
     "DIV" "SECTION"
     "UL" "DL" "OL"
     "H1" "H2" "H3" "H4" "H5" "H6" "H7"}))
(define stdschemas
  '{(sbooks . "http://sbooks.net/")
    (sbook . "http://sbooks.net/")
    (olib . "http://openlibrary.org/")
    (ia . "http://archive.org/")
    (fdjt . "http://fdjt.org/")
    (fd . "http://framerd.org/")
    (kno . "http://knodules.org/")
    (bm . "http://beingmeta.com/")
    (beingmeta . "http://beingmeta.com/")})
(do-choices (schema stdschemas)
  (set+! stdschemas (cons (cdr schema) (car schema))))
(config-def! 'stdschemas
	     (lambda (name (val))
	       (if (bound? val)
		   (if (or (and (pair? val)
				(symbol? (car val))
				(string? (cdr val)))
			   (and (pair? val)
				(symbol? (cdr val))
				(string? (car val))))
		       (set+! stdschemas
			      (choice val (cons (cdr val) (car val))))
		       (error "Bad DOM schema spec" "stdschemas:config"
			      val))
		   stdschemas))
	     "Define standard schemas for DOM META and LINK elements")

(define *terminal-block-text-tags*
  (string->symbol
   '{"P" "LI" "DT" "DD"
     "H1" "H2" "H3" "H4" "H5" "H6" "H7"}))

(define (dom/block? node)
  (overlaps? (get node '%xmltag) *block-text-tags*))

(define (dom/terminal? node)
  (overlaps? (get node '%xmltag) *terminal-block-text-tags*))

;;; Random utilities

(define (dom/split-space string) (elts (segment string " ")))
(define (dom/split-semi string)
  (difference (stdspace (elts (segment string ";"))) ""))
(define (dom/split-comma string)
  (difference (stdspace (elts (segment string ","))) ""))

;;; DOM editing

;;; The trick here is to update the XML representation so that
;;;  XML unparsing will reflect the changing.  This means updating
;;;  in multiple ways.

(define (varycase s)
  (if (symbol? s)
      (choice (symbol->string s) (downcase (symbol->string s)))))
(define (lowername x) (downcase (first x)))
(define (lowerqname x) (downcase (second x)))
(define (unqname attrib)
  (let ((qname (first attrib)))
    (if (position #\: qname)
	(slice qname (1+ (position #\: qname)))
	qname)))
(define (lunqname attrib)
  (let ((qname (first attrib)))
    (downcase
     (if (position #\: qname)
	 (slice qname (1+ (position #\: qname)))
	 qname))))

(define (attrib-basename s)
  (if (or (pair? s) (and (vector? s) (> (length s) 0)))
      (if (string? (first s))
	  (if (position #\: (first s))
	      (subseq (first s) (1+ (position #\: (first s))))
	      (if (position #\} (first s))
		  (subseq (first s) (1+ (position #\} (first s))))
		  (first s)))
	  s)))
(define (attrib-namespace s)
  (and (string? s) (position #\: s)
       (subseq s 0 (position #\: s))))
(define (attrib-name s)
  (and (string? s) (position #\: s)
       (subseq s (1+ (position #\: s)))))

(define (dom/set-tag! node name (index))
  (default! index (try (get (get node '%doc) 'index) #f))
  (when index
    (drop! index (cons '{%xmltag %qname} (get node '{%xmltag %qname}))
	   node)
    (add! index (cons '{%xmltag %qname} name) node))
  (store! node '{%xmltag %qname} name)
  (store! node '%id (dom/nodeid node)))

(define (dom/set! node attrib val (fdxml #f) (index #f))
  (drop! node '%markup)
  (let* ((slotid (if (symbol? attrib) attrib (string->lisp attrib)))
	 (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		    attrib))
	 (unq (tryif (position #\: aname)
		(slice aname (1+ (position #\: aname)))))
	 (allattribs (get node '%attribs))
	 (attribs (try (pick allattribs first aname)
		       (pick allattribs second aname)
		       (pick allattribs lowername aname)
		       (pick allattribs unqname (try unq aname))
		       (pick allattribs lunqname (downcase (try unq aname)))))
	 (stringval (if (and (not fdxml) (string? val)) val
			(unparse-arg val)))
	 (qattrib (string->lisp (first (singleton attribs)))))
    (when index
      (drop! index (cons (choice attrib slotid qattrib) (get node attrib)) node)
      (add! index (cons (choice attrib slotid qattrib) val) node))
    (when (ambiguous? attribs)
      (error "AmbiguousDOMAttribute"
	     "DOM/SET of ambiguous attribute " attrib
	     attribs))
    (store! node slotid val)
    (unless (test node '%attribids (try qattrib slotid))
      (add! node '%attribids (try qattrib slotid))
      (when index (add! index (cons '%attribids (try qattrib slotid)) node)))
    (if (exists? attribs)
	(begin (drop! node '%attribs attribs)
	  (add! node '%attribs
		(vector (elt attribs 0) (elt attribs 1) stringval))
	  (store! node (choice (string->lisp aname) slotid qattrib) val)
	  (when (attrib-name aname)
	    (store! node (string->lisp (attrib-name aname)) val))
	  (add! node '%attribs
		(vector (elt attribs 0) (elt attribs 1) stringval)))
	(add! node '%attribs (vector aname #f stringval)))))

(define (dom/drop! node attrib (value) (sep #f) (index #f))
  (drop! node '%markup)
  (let* ((slotid (if (symbol? attrib) attrib (string->lisp attrib)))
	 (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		    attrib))
	 (anames (if (symbol? attrib) (varycase attrib) attrib))
	 (attribs (try (pick (get node '%attribs) first anames)
		       (pick (get node '%attribs) attrib-basename anames))))
    (when (ambiguous? attribs)
      (error "AmbiguousDOMAttribute"
	     "DOM/SET of ambiguous attribute " attrib
	     attribs))
    (if (bound? value)
	(let* ((val (try (cadr attribs) (get node slotid)))
	       (vals (if sep (segment val sep) (segment val)))
	       (newvals (remove value vals)))
	  (unless (equal? vals newvals)
	    (dom/set! node (try (car attribs) slotid)
		      (stringout
			(if sep
			    (doseq (v newvals i)
			      (printout (if (> i 0) sep) v))
			    (doseq (v newvals i)
			      (printout (if (> i 0) " ") v)))))))
	(begin
	  (drop! node slotid)
	  (drop! node '%attribids slotid)
	  (when (exists? attribs) (drop! node '%attribs attribs))
	  (when index (drop! index (cons attrib value) node))))))

(define (dom/get node attrib)
  (if (symbol? attrib) (get node attrib)
      (third (pick (get node '%attribs) {first second} attrib))))

(define (dom/hasclass? node classname (attrib 'class))
  (or (test node attrib classname)
      (textsearch (if (string? classname) `(word ,classname)
		      `(IC (WORD ,(->string classname))))
		  (try (pickstrings (get node attrib)) ""))))
(define (dom/addclass! node classname)
  (if (test node 'class)
      (let ((classes (segment (get node 'class) " ")))
	(unless (position classname classes)
	  (dom/set! node 'class
		    (stringout (doseq (class classes) (printout class " "))
		      (printout classname)))))
      (dom/set! node 'class classname)))
(define (dom/dropclass! node classname)
  (when (test node 'class)
    (let ((classes (segment (get node 'class) " ")))
      (when (position classname classes)
	(if (= (length classes) 1)
	    (dom/drop! node 'class)
	    (dom/set! node 'class
		      (stringout
			(doseq (class (remove classname classes) i)
			  (printout (if (> i 0) " ") class)))))))))

(define (dom/add! node attrib value (sep ";") (index #f))
  (drop! node '%markup)
  (let ((current (dom/get node attrib)))
    (if (fail? current)
	(dom/set! node attrib value)
	(let ((values (segment current sep)))
	  (unless (position value values)
	    (dom/set! node attrib (string-append value sep current))
	    (when index (add! index (cons attrib value) node)))))))

(define (dom/remove-child! node elt (recur #t))
  (drop! node '%markup)
  (and (table? node) (test node '%content)
       (let* ((content (get node '%content))
	      (newcontent (remove elt content)))
	 (cond ((equal? content newcontent)
		(if recur
		    (some? (lambda (x) (and (table? x) (dom/remove! x elt)))
			   content)
		    #f))
	       (else (store! node '%content newcontent)
		     #t)))))
(define (dom/replace-child! node cur new (recur #t))
  (drop! node '%markup)
  (and (table? node) (test node '%content)
       (let* ((content (get node '%content))
	      (newcontent
	       (->list (forseq (celt (->vector content))
			 (if (equal? cur celt) new celt)))))
	 (cond ((equal? content newcontent)
		(if recur
		    (some? (lambda (x)
			     (and (table? x) (dom/replace-child! x cur new #t)))
			   content)
		    #f))
	       (else (store! node '%content newcontent)
		     #t)))))

(define (dom/remove! elt (node))
  (default! node (get elt '%parent))
  (when (and (exists? node) (table? node) (test node '%content))
    (drop! node '%markup)
    (let* ((content (get node '%content))
	   (newcontent (remove elt content)))
      (store! node '%content newcontent))))
(define (dom/replace! cur new (node))
  (default! node (get cur '%parent))
  (when (and (exists? node) (table? node) (test node '%content))
    (drop! node '%markup)
    (let* ((content (get node '%content))
	   (newcontent
	    (->list (forseq (celt (->vector content))
		      (if (equal? cur celt) new celt)))))
      (store! node '%content newcontent))))

(define (dom/append! node . content)
  (drop! node '%markup)
  (let ((current (try (get node '%content) '())))
    (dolist (elt content)
      (when (exists? elt)
	(set! current
	      (append current
		      (if (pair? elt) elt
			  (if (and (string? elt) (has-prefix elt "<"))
			      (if (test node '%%xmltag)
				  (xmlparse elt 'keepraw)
				  (xmlparse elt))
			      (list elt)))))))
    (store! node '%content current)))

(define (dom/prepend! node . content)
  (let ((current (try (get node '%content) '())))
    (dolist (elt (reverse content))
      (when (exists? elt)
	(set! current
	      (append (if (pair? elt) elt
			  (if (and (string? elt) (has-prefix elt "<"))
			      (if (test node '%%xmltag)
				  (xmlparse elt 'keepraw)
				  (xmlparse elt))
			      (list elt)))
		      current))))
    (store! node '%content current)))

;;; Textmap functions

(define (dom/reverse-text! node)
  (if (string? node)
      (reverse (decode-entities node))
      (if (test node '%content)
	  (begin (store! node '%content
			 (->list
			  (forseq (elt (->vector (get node '%content)))
			    (if (string? elt) (reverse elt)
				(dom/reverse-text! elt)))))
	    node)
	  node)))

(define (dom/transform-text! node fn)
  (if (string? node)
      (fn (decode-entities node))
      (if (test node '%content)
	  (begin (store! node '%content
			 (->list
			  (forseq (elt (->vector (get node '%content)))
			    (if (string? elt) (fn elt)
				(dom/transform-text! elt fn)))))
	    node)
	  node)))

(module-export! '{dom/reverse-text! dom/transform-text!})

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
	  (for-choices (spec (if (position #\, spec)
				 (elts (segment spec ","))
				 spec))
	    (let ((match (text->frames selector-pattern spec)))
	      (cons-selector (try (get match 'tagname) #f)
			     (try (difference (get match 'classname) "*") #f)
			     (try (get match 'idname) #f)
			     (combine-attribs (pick match 'name)))))
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
	     (filter-dom-matches elt (get table (symbol->string tag)))
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
			    (test elt '%rawtag (selector-tag sel)))
			(or (not (selector-class sel))
			    (test elt 'class (selector-class sel))
			    (and (test elt 'class)
				 (if (test elt 'classname)
				     (contains? (selector-class sel)
						(get elt 'classname))
				     (if (test elt '%class)
					 (contains? (selector-class sel)
						    (get elt '%class))
					 (let ((classes (elts (segment (get elt 'class)))))
					   (store! elt '%class classes)
					   (contains? (selector-class sel) classes))))))
			(or (not (selector-id sel))
			    (test elt 'id (selector-id sel)))
			(or (fail? (selector-attribs sel))
			    (try
			     (try-choices (attrib (selector-attribs sel))
			       (and (if (null? (cdr attrib))
					(test elt (car attrib))
					(if (string? (cdr attrib))
					    (test elt (car attrib) (cdr attrib))
					    (textsearch (cdr attrib)
							(get elt (car attrib)))))
				    {}))
			     #t)))))
	     #f)))))

;;; Finding using the index

(defambda (dom-index-find index sel (under #f))
  (cond (under
	 (for-choices under
	   (intersection (dom-index-find index sel)
			 (find-frames index 'parents under))))
	((ambiguous? sel) (for-choices sel (dom-index-find index sel)))
	((exists? (selector-attribs sel))
	 (filter-choices (cand (dom-find-core index sel))
	   (dom/match cand sel)))
	(else (dom-find-core index sel))))

(define (dom-find-core index sel)
  (cond ((and (selector-tag sel) (selector-id sel)
	      (singleton? (selector-class sel))
	      (selector-class sel))
	 (find-frames index
	   '%xmltag (selector-tag sel)
	   'class (selector-class sel)
	   'id (selector-id sel)))
	((and (selector-tag sel) (singleton? (selector-class sel))
	      (selector-class sel))
	 (find-frames index
	   '%xmltag (selector-tag sel)
	   'class (selector-class sel)))
	((and (selector-tag sel) (selector-id sel)
	      (not (selector-class sel)))
	 (find-frames index
	   '%xmltag (selector-tag sel)
	   'id (selector-id sel)))
	((and (selector-id sel)
	      (not (selector-tag sel))
	      (not (selector-class sel)))
	 (find-frames index 'id (selector-id sel)))
	((and (selector-id sel)
	      (singleton? (selector-class sel))
	      (selector-class sel)
	      (not (selector-tag sel)))
	 (find-frames index
	   'class (selector-class sel)
	   'id (selector-id sel)))
	((ambiguous? (selector-class sel))
	 (apply intersection
		(cond ((and (selector-tag sel) (selector-id sel))
		       (find-frames index '%xmltag (selector-tag sel)
				    'id (selector-id sel)))
		      ((selector-tag sel) (find-frames index '%xmltag (selector-tag sel)))
		      ((selector-id sel) (find-frames index 'id (selector-id sel)))
		      (else (fail)))
		(map (lambda (class) (find-frames index 'class class))
		     (choice->list (selector-class sel)))))
	((selector-id sel) (find-frames index 'id (selector-id sel)))
	((selector-class sel)
	 (find-frames index 'class (selector-class sel)))
	((selector-tag sel)
	 (find-frames index '%xmltag (selector-tag sel)))
	((and (not (selector-tag sel)) (not (selector-class sel))
	      (not (selector-id sel))
	      (exists? (selector-attribs sel)))
	 (let ((cands #f) (selected {}))
	   (do-choices (attrib (selector-attribs sel))
	     (unless (fail? cands)
	       (set! selected
		     (if (null? (cdr attrib))
			 (find-frames index '{has %attribids} (car attrib))
			 (find-frames index (car attrib) (cdr attrib))))
	       (if cands (set! cands (intersection cands selected))
		   (set! cands selected))))
	   cands))
	(else (fail))))

;;; Searching

(defambda (dom/find under sel (findall #t))
  "Finds all nodes matching SEL under UNDER, if FINDALL is true, \
   look under matching nodes for other matching nodes."
  (when (exists? (pickstrings sel)) (set! sel (->selector sel)))
  (cond ((string? under) (fail))
	((ambiguous? under)
	 (for-choices under (dom/find under sel)))
	((exists?  (get under 'index))
	 (for-choices (index (get under 'index)) (dom-index-find index sel)))
	((exists? (get (get under '%doc) 'index))
	 (let ((index (try (get under 'index) (get (get under '%doc) 'index))))
	   (for-choices index (dom-index-find index sel under))))
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

(defambda (dom/find->list under sel (findall #t))
  "Finds all nodes matching SEL under UNDER, if FINDALL is true, \
   look under matching nodes for other matching nodes."
  (unless (exists? (pickstrings sel)) (set! sel (->selector sel)))
  (cond ((string? under) '())
	((ambiguous? under)
	 (let ((nodes '()))
	   (do-choices under
	     (set! nodes (append nodes (dom/find->list under sel))))
	   nodes))
	((pair? under)
	 (let ((nodes '()))
	   (dolist (under under)
	     (set! nodes (append nodes (dom/find->list under sel))))
	   nodes))
	((and (table? under) (dom/match under sel))
	 (list under))
	((and (table? under) (test under '%content)
	      (exists? (get under '%content))
	      (pair? (get under '%content)))
	 (let ((nodes '()))
	   (dolist (under  (get under '%content))
	     (set! nodes (append nodes (dom/find->list under sel))))
	   nodes))
	(else '())))

;;; Text searching

(define (search-helper under path pattern foreach)
  "Finds all text and nodes containing pattern under UNDER"
  (for-choices (elt (elts (if (pair? under) under
			      (get under '%content))))
    (if (string? elt)
	(if (textsearch pattern elt)
	    (if foreach
		(if (foreach elt under path)
		    (if (pair? under) (cons elt path)
			(cons* elt under path))
		    {})
		(if (pair? under) (cons elt path)
		    (cons* elt under path)))
	    (fail))
	(if (table? elt)
	    (search-helper elt (cons under path) pattern foreach)
	    (fail)))))

(define (dom/search under pattern (foreach #f))
  (search-helper under '() pattern #f))

(define (dom/search/first under pattern (test #f) (return 'node))
  (unless (overlaps? return '{string node path}) (error "Invalid return specifier"))
  (call/cc (lambda (exitor)
	     (search-helper under pattern '()
			    (lambda (string node path)
			      (if (and test (not (test string node path))) #f
				  (if (eq? return 'node) (exitor node)
				      (if (eq? return 'string) (exitor string)
					  (if (eq? return 'path) (exitor (cons node path))
					      (exitor (cons* string node path)))))))))))


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

(define (dom/gethead doc)
  (try (get doc 'head)
       (tryif (test doc '%xmltag 'head) doc)
       (let ((found (dom/find doc "HEAD")))
	 (when (exists? found)
	   (store! doc 'head found))
	 found)))

(define (dom/getschemas doc)
  (if (test doc '%schemas) (get doc '%schemas)
      (begin
	(when (exists? (get doc '%xschemas))
	  (add! doc '%schemas (get doc '%xschemas)))
	(do-choices (link (dom/find doc "LINK"))
	  (when (has-prefix (get link 'rel) "schema.")
	    (let* ((rel (get link 'rel))
		   (href (get link 'href))
		   (prefix (string->symbol (subseq rel 7))))
	      (add! doc '%schemas (cons prefix rel))
	      (add! doc '%schemas (cons rel prefix)))))
	(get doc '%schemas))))

(define (getnames doc field)
  (if (position #\. field)
      (let* ((sep (position #\. field))
	     (schemas (dom/getschemas doc))
	     (prefixstring (subseq field 0 sep))
	     (prefix (string->symbol prefixstring))
	     (uri (try (get schemas prefixstring)
		       (get schemas prefix)
		       (get stdschemas prefixstring)
		       (get stdschemas prefix)))
	     (prefixes (get schemas uri)))
	(if (fail? prefixes) field
	    (glom prefixes "." (subseq field (1+ sep)))))
      field))

(define (dom/getmeta doc field (xform) (dflt))
  (let* ((head (dom/gethead doc))
	 (meta (dom/find head "META"))
	 (names
	  (if (symbol? field)
	      (getnames doc (symbol->string field))
	      (getnames doc field)))
	 (elts (if (symbol? field)
		   (pick meta lname (downcase names))
		   (try (pick meta 'name names)
			(pick meta 'name field)))))
    (try (if (and (bound? xform) xform)
	     (if (applicable? xform)
		 (xform (get elts 'content))
		 (if (eq? xform #t) (parse-arg (get elts 'content))
		     (textsubst (get elts 'content) (qc xform))))
	     (if (bound? xform) elts ;; Must be #f
		 (get elts 'content)))
	 (if (bound? dflt) dflt (fail)))))

(define (dom/getlinks doc field (dflt))
  (let* ((head (dom/gethead doc))
	 (links (dom/find head "LINK"))
	 (names
	  (if (symbol? field)
	      (getnames doc (symbol->string field))
	      (getnames doc field)))
	 (elts (if (symbol? field)
		   (pick links lname (downcase names))
		   (try (pick links 'rel names)
			(pick links 'rel field)))))
    (get elts 'href)))

(define (lname x (name))
  (default! name (try (get x 'rel) (get x 'name)))
  (if (fail? name) {}
      (if (symbol? name) (downcase (symbol->string name))
	  (if (string? name) (downcase name)
	      (downcase (stringout name))))))

;;; Walking the DOM

(define (map0 node fn)
  (if (pair? node)
      (doseq (elt (->vector node)) (map0 elt fn))
      (begin (fn node)
	(doseq (elt (->vector (try (get node '%content) '())))
	  (when (table? elt) (map0 elt fn))))))

(define (map1 node fn arg)
  (if (pair? node)
      (doseq (elt (->vector node)) (map1 elt fn (qc arg)))
      (begin (fn node)
	(dolist (elt (try (get node '%content) '()))
	  (when (table? elt) (map1 elt fn (qc arg)))))))

(define (mapn node fn args)
  (if (pair? node)
      (doseq (elt node) (mapn elt fn args))
      (begin (apply fn node args)
	(doseq (elt (->vector (try (get node '%content) '())))
	  (when (table? elt) (mapn elt fn args))))))

(defambda (dom/map node fn (arg) . args)
  (do-choices node
    (do-choices fn
      (if (bound? arg)
	  (if (null? args) (map1 node fn arg)
	      (mapn node fn (cons (qc arg) args)))
	  (map0 node fn)))))

;;; Combining

(define (dom/combine! target elements (extract "BODY"))
  (dolist (elt elements)
    (dom/append! target (dom/find->list elt extract))))

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
			      (test node '%xmltag '%comment)
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

(define (dom/textual? node (lim #f))
  (and (test node '%content)
       (if lim
	   (do ((content (get node '%content) (cdr content))
		(len 0 (+ (if (string? (car content))
			      (if (has-prefix (car content) "<!--")
				  0 (isalphalen (car content)))
			      0)
			  len)))
	       ((or (null? content) (> len lim))
		(> len lim)))
	   (some? (lambda (x)
		    (and (string? x)
			 (not (empty-string? x))
			 (not (has-prefix x "<!--"))))
		  (get node '%content)))))

(define inline-tags '{a em strong i b span cite})

(define (dom/structural? node)
  "Returns true if a node only contains other blocks"
  (and (test node '%content)
       (every? (lambda (x) (if (string? x) (empty-string? x)
			       (not (and (test x '%xmltag inline-tags)
					 (dom/textual? x)))))
	       (get node '%content))))

;;; OIDify

(define dompool #f)

(define (oidify/copy node pool (doc #f) (parent #f))
  (if (slotmap? node)
      (try (get node '%oid)
	   (let* ((oid (frame-create pool))
		  (slotids (getkeys node))
		  (cur #f) (prev #f))
	     (when parent (store! oid '%parent parent))
	     (store! oid '%oid oid)
	     (store! node '%oid oid)
	     (when doc (store! oid '%doc doc))
	     (when (test node '%content)
	       (store! oid '%content
		       (->list
			(forseq (elt (->vector (get node '%content)))
			  (when (slotmap? elt)
			    (set! cur (oidify/copy elt pool doc oid))
			    (when (oid? cur)
			      (when prev (store! prev 'next cur) (store! cur 'prev prev))
			      (set! prev cur)))
			  (if (slotmap? elt) cur elt)))))
	     (do-choices (slotid (difference slotids '%content))
	       (store! oid slotid (%get node slotid)))
	     (unless (test oid '%id)
	       (store! oid '%id (dom/nodeid oid)))
	     (store! oid '%children
		     (pickoids (elts (get oid '%content))))
	     oid))
      node))

(define (oidify/inplace node pool (doc #f) (parent #f))
  (if (slotmap? node)
      (try (get node '%oid)
	   (let ((oid (allocate-oids pool)) (cur #f) (prev #f)
		 (content (try (->vector (get node '%content)) #f)))
	     ;; (logdebug "Allocated " oid " for " (get node 'id))
	     (store! node '%oid oid)
	     (set-oid-value! oid node #t)
	     (when parent (store! node '%parent parent))
	     (when doc (store! node '%doc doc))
	     (when content
	       (store! node '%content
		       (->list
			(forseq (elt content)
			  (if (slotmap? elt)
			      (begin
				(set! cur (oidify/inplace elt pool doc oid))
				(when prev
				  (store! prev 'next cur)
				  (store! cur 'prev prev))
				(set! prev cur)
				cur)
			      elt)))))
	     (unless (test node '%id)
	       (store! oid '%id (dom/nodeid oid)))
	     (store! oid '%children
		     (pickoids (elts (get node '%content))))
	     oid))
      node))

(define (dom/oidify node (pool dompool) (inplace #t) (doc #f) (parent #f) )
  (if (eq? pool #t) (set! pool dompool))
  (if (not pool) node
      (if inplace
	  (oidify/inplace node pool doc parent)
	  (oidify/copy node pool doc parent))))

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
    (if (and (singleton? (get oid '%attribids))
	     (vector? (get oid '%attribids)))
	(doseq (attrib (get oid '%attribids))
	  (unless (overlaps? attrib '{name class id})
	    (printout "[" attrib "]")))
	(do-choices (attrib (difference (get oid '%attribids) '{name class id}))
	  (printout "[" attrib "]")))))

(varconfig! DOMPOOL dompool use-pool)

;;;; DOM IDs

(define (dom->id elt)
  (if (or (oid? elt) (string? elt)) elt
      (try (get elt 'oid) (get elt 'id))))
(define (id->dom id index)
  (if (oid? id) id (find-frames index 'id id)))



