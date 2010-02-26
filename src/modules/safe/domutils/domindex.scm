;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'domutils/index)

;;; Utilites for indexing XML content, especially XHTML
(define version "$Id$")
(define revision "$Revision$")

(use-module '{reflection
	      fdweb xhtml texttools domutils
	      morph varconfig logger})

(module-export! '{dom/index! dom/indexer})

(define %loglevel %notice!)
(define default-dom-slots '{id class name})

(define default-indexrules (make-hashtable))
(define (indexrule-config var (val))
  (if (bound? val)
      (if (pair? val)
	  (add! default-indexrules (car val) (cdr val))
	  (set+! default-dom-slots val))
      default-indexrules))
(config-def! 'DOM:INDEXRULES indexrule-config)

(define default-analyzers {})
(varconfig! dom:analyzer default-analyzers #f choice)

(config! 'dom:indexrules (cons 'class dom/split-space))

(define (dom/index! index doc (settings #[]))
  (let ((indexslots (try (get settings 'indexslots) default-dom-slots))
	(cacheslots (get settings 'cacheslots))
	(indexrules (try (get settings 'indexrules) default-indexrules))
	(analyzers (try (get settings 'analyzers) default-analyzers))
	(idmap (try (get settings 'idmap) #f)))
    (dom/indexer index doc
		 indexslots cacheslots indexrules analyzers idmap
		 settings doc)))
	
(defambda (dom/indexer index xml
		       indexslots cacheslots indexrules
		       analyzers idmap settings doc)
  (if (pair? xml)
      (dolist (elt xml)
	(dom/indexer index elt 
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
		 (rules (pick (choice (pick indexslots pair?)
				      (pick eltinfo pair?))
			      slots)))
	    ;; (%WATCH "DOMINDEXER" indexval slots rules)
	    (when idmap (add! idmap (get xml 'id) xml))
	    (add! index (cons 'has slots) indexval)
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
	      (do-choices (analyzer (choice analyzers
					    (pick eltinfo procedure?)))
		(do-choices (slot.val (analyzer xml settings))
		  (when (overlaps? (car slot.val) cacheslots)
		    (add! xml (car slot.val) (cdr slot.val)))
		  (add! index (cons (car slot.val) (cdr slot.val))
			indexval))))
	    (when (exists? content)
	      (dolist (elt content)
		(dom/indexer index elt
			     indexslots cacheslots
			     indexrules analyzers idmap
			     settings doc))))))))








