;;; -*- Mode: Scheme; -*-

(in-module 'domutils/index)

;;; Utilites for indexing XML content, especially XHTML
(define version "$Id:$")
(define revision "$Revision:$")

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

(define (dom/index! index doc (settings #[]))
  (let ((indexslots (try (get settings 'indexslots) default-dom-slots))
	(cacheslots (get settings 'cacheslots))
	(indexrules (try (get settings 'indexrules) default-indexrules))
	(analyzers (try (get settings 'analyzers) default-analyzers))
	(useids (try (get settings 'useids) #t)))
    (dom/indexer index doc
		 indexslots cacheslots indexrules analyzers useids
		 settings doc)))
	
(defambda (dom/indexer index xml
		       indexslots cacheslots indexrules
		       analyzers useids settings doc)
  (if (pair? xml)
      (dolist (elt xml)
	(dom/indexer index elt 
		     indexslots cacheslots
		     indexrules analyzers useids
		     settings doc))
      (when  (table? xml)
	(unless (test xml 'noindex)
	  (let* ((content (get xml '%content))
		 (indexval (if useids (get xml 'id) xml))
		 (eltinfo (dom/lookup indexrules xml))
		 (slots (intersection
			 (choice (pick indexslots symbol?)
				 (car (pick indexslots pair?))
				 (pick eltinfo symbol?)
				 (car (pick eltinfo pair?)))
			 (getkeys xml)))
		 (rules (pick (choice (pick indexslots pair?)
				      (pick eltinfo pair?))
			      slots)))
	    ;; (%WATCH "DOMINDEXER" indexval slots rules)
	    (when (test settings 'nodemap)
	      (add! (get settings 'nodemap) (get xml 'id) xml))
	    (add! index (cons 'has slots) indexval)
	    (when (exists? indexval)
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
			     indexrules analyzers useids
			     settings doc))))))))


