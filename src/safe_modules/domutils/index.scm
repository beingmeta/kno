;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Utilites for indexing XML content, especially XHTML
(in-module 'domutils/index)

(define %used_modules '{varconfig})

(use-module '{reflection domutils varconfig logger})

(module-export! '{dom/index! dom/indexer})

(define-init %loglevel %notice%)

(define default-dom-slots
  '{%xmltag %qname %attribids %rawtag %namespace
    id class name href src rel})

(define default-indexrules (make-hashtable))
(define (indexrule-config var (val))
  (if (bound? val)
      (if (pair? val)
	  (add! default-indexrules (car val) (cdr val))
	  (set+! default-dom-slots val))
      default-indexrules))
(config-def! 'DOM:INDEXRULES indexrule-config)

(define default-analyzers {})
(varconfig! dom:analyzers default-analyzers #f choice)

(config! 'dom:indexrules (cons 'class dom/split-space))
(config! 'dom:analyzers
	 (lambda (x settings)
	   (cons 'classname (dom/split-space (get x 'class)))))

(define (dom/index! index doc (settings #[]))
  (let ((indexslots (getopt settings 'indexslots default-dom-slots))
	(cacheslots (getopt settings 'cacheslots))
	(indexrules (getopt settings 'indexrules default-indexrules))
	(analyzers (getopt settings 'analyzers default-analyzers))
	(idmap (getopt settings 'idmap)))
    (when (overlaps? indexslots '{dom/index/defaults})
      (set+! indexslots default-dom-slots))
    (info%watch "DOM/INDEX!" index doc indexslots indexrules analyzers)
    (dom/indexer index doc {} {}
		 indexslots cacheslots indexrules analyzers idmap
		 settings doc)))
	
(defambda (dom/indexer index xml parent parents
		       indexslots cacheslots indexrules
		       analyzers idmap settings doc)
  (if (or (pair? xml) (vector? xml))
      (doseq (elt xml)
	(dom/indexer index elt parent parents
		     indexslots cacheslots
		     indexrules analyzers idmap
		     settings doc))
      (when (table? xml)
	(unless (test xml 'noindex)
	  (let* ((content (get xml '%content))
		 (indexval (try (get xml '%oid)
				(if idmap (get xml 'id) xml)))
		 (eltinfo (dom/lookup indexrules xml))
		 (slots (choice
			 (if (or (not indexslots)
				 (overlaps? indexslots 'dom/index/all))
			     (getkeys xml)
			     (intersection
			      (choice (pick indexslots symbol?)
				      (car (pick indexslots pair?))
				      (pick eltinfo symbol?)
				      (car (pick eltinfo pair?)))
			      (getkeys xml)))
			 (get xml '%attribids)))
		 (rules (choice
			 (pick (choice (pick indexslots pair?)
				       (pick eltinfo pair?))
			       slots)
			 (pick indexrules hashtable?)))
		 (terminal #t))
	    (debug%watch "DOM/INDEXER" (dom/sig xml) index xml parent
			 indexval indexslots slot rules eltinfo)
	    (when idmap (add! idmap (get xml 'id) xml))
	    (add! index (cons 'has {(getkeys xml) (get xml '%attribids)}) indexval)
	    (add! index (cons 'parent parent) indexval)
	    (add! index (cons 'parents parents) indexval)
	    (when (exists? indexval)
	      (add! index (cons '%doc doc) indexval)
	      (if (fail? content)
		  (add! index (cons '%type 'void) indexval)
		  (if (null? content)
		      (add! index (cons '%type 'empty) indexval)
		      (if (every? string? content)
			  (add! index (cons '%type 'terminal) indexval))))
	      (do-choices (slotid (reject slots rules))
		(add! index (cons slotid (get xml slotid)) indexval))
	      (when (overlaps? slots '%attribids)
		(do-choices (attrib (get xml '%attribs))
		  (when (or (vector? attrib) (pair? attrib))
		    (add! index (cons '%attribid (first attrib)) indexval))))
	      (do-choices (slotid (pick slots rules))
		(add! index
		      (cons slotid ((get rules slotid) (get xml slotid)))
		      indexval))
	      (do-choices (analyzer (choice (pick eltinfo procedure?)
					    analyzers))
		(do-choices (slot.val (analyzer xml settings))
		  (when (overlaps? (car slot.val) cacheslots)
		    (add! xml (car slot.val) (cdr slot.val)))
		  (add! index (cons (car slot.val) (cdr slot.val))
			indexval))))
	    (when (exists? content)
	      (doseq (elt content)
		(unless (string? elt)
		  (dom/indexer index elt
			       (if (oid? xml) xml (get xml 'id))
			       (choice (if (oid? xml) xml (get xml 'id))
				       parents)
			       indexslots cacheslots
			       indexrules analyzers idmap
			       settings doc)))))))))







