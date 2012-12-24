;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'domutils/index)

;;; Utilites for indexing XML content, especially XHTML
(define version "$Id$")
(define revision "$Revision: 4957 $")

(use-module '{reflection
	      fdweb xhtml texttools domutils
	      morph varconfig logger})

(module-export! '{dom/index! dom/indexer})

(define %loglevel %notice!)
(define default-dom-slots '{id class name href src rel})

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
  (let ((indexslots (try (get settings 'indexslots) default-dom-slots))
	(cacheslots (get settings 'cacheslots))
	(indexrules (try (get settings 'indexrules) default-indexrules))
	(analyzers (try (get settings 'analyzers) default-analyzers))
	(idmap (try (get settings 'idmap) #f)))
    (dom/indexer index doc {} {}
		 indexslots cacheslots indexrules analyzers idmap
		 settings doc)))
	
(defambda (dom/indexer index xml parent parents
		       indexslots cacheslots indexrules
		       analyzers idmap settings doc)
  (if (pair? xml)
      (dolist (elt xml)
	(dom/indexer index elt parent parents
		     indexslots cacheslots
		     indexrules analyzers idmap
		     settings doc))
      (when  (table? xml)
	(unless (test xml 'noindex)
	  (let* ((content (get xml '%content))
		 (indexval (try (get xml '%oid)
				(if idmap (get xml 'id) xml)))
		 (eltinfo (dom/lookup indexrules xml))
		 (slots (choice
			 (intersection
			  (choice (pick indexslots symbol?)
				  (car (pick indexslots pair?))
				  (pick eltinfo symbol?)
				  (car (pick eltinfo pair?)))
			  (getkeys xml))
			 (get xml '%attribids)))
		 (rules (choice
			 (pick (choice (pick indexslots pair?)
				       (pick eltinfo pair?))
			       slots)
			 (pick indexrules hashtable?))))
	    ;;(%WATCH "DOMINDEXER" indexval indexslots eltinfo slots rules)
	    (when idmap (add! idmap (get xml 'id) xml))
	    (add! index (cons 'has {(getkeys xml) (get xml '%attribids)}) indexval)
	    (add! index (cons 'parent parent) indexval)
	    (add! index (cons 'parents parents) indexval)
	    (when (exists? indexval)
	      (add! index (cons '%doc doc) indexval)
	      (do-choices (slotid slots)
		(add! index
		      (if (test rules slotid)
			  (cons slotid
				((get rules slotid)
				 (get xml slotid)))
			  (cons slotid (get xml slotid)))
		      indexval))
	      (do-choices (analyzer (choice (pick eltinfo procedure?)
					    analyzers))
		(do-choices (slot.val (analyzer xml settings))
		  (when (overlaps? (car slot.val) cacheslots)
		    (add! xml (car slot.val) (cdr slot.val)))
		  (add! index (cons (car slot.val) (cdr slot.val))
			indexval))))
	    (when (exists? content)
	      (dolist (elt content)
		(dom/indexer index elt
			     (if (oid? xml) xml (get xml 'id))
			     (choice (if (oid? xml) xml (get xml 'id))
				     parents)
			     indexslots cacheslots
			     indexrules analyzers idmap
			     settings doc))))))))








