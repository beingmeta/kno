;;; -*- Mode: Scheme; -*-

(in-module 'domutils/index)

(use-module '{fdweb xhtml texttools domutils varconfig})

(module-export! 'dom/index!)

(define default-dom-slots '{id class name})
(define default-text-elements
  (->selector (->string '{p h1 h2 h3 h4 h5 h6 h7 h8 h9 li blockquote div})))

(define default-rootfn #f)
(varconfig! dom:rootfn default-rootfn)

(defambda (dom/index! index xml (settings #[])
		      (slotids)
		      (rootfn)
		      (phrasemap)
		      (useids)
		      (textelts))
  (default! slotids (try (get settings 'slotids) default-dom-slots))
  (default! rootfn (try (get settings 'rootfn) default-rootfn))
  (default! phrasemap (try (get settings 'phrasemap) #f))
  (when (overlaps? slotids 'text)
    (if (fail? (get settings 'textelts))
	(store! settings 'textelts default-text-elements)
	(let* ((tofix
		(choice (pick (get settings 'textelets) string?)
			(pick (get settings 'textelets) symbol?)))
	       (selectors
		(->selector (choice (pickstrings tofix)
				    (symbol->string (pick tofix symbol?))))))
	  (add! settings 'textelts selectors)
	  (drop! settings 'textelts tofix))))
  (default! textelts (get settings 'textelts))
  (default! useids
    (or (index? index)
	(and (test settings 'useids)
	     (get settings 'useids))))
  (if (pair? xml)
      (dolist (elt xml)
	(dom/index! index elt settings slotids
		    rootfn phrasemap useids))
      (when  (table? xml)
	(let ((content (get xml '%content))
	      (indexval (if useids (get xml 'id) xml)))
	  (when (and (exists? indexval)
		     (exists dom/match xml textelts))
	    (do-choices (slotid (difference slotids 'text))
	      (add! index (cons slotid (get xml slotid)) indexval))
	    (when (overlaps? slotids 'text)
	      (let* ((text (dom/textify xml))
		     (wordv (words->vector text))
		     (rootv (and (and rootfn (map rootfn wordv)))))
		(store! xml 'words wordv)
		(add! index (cons 'words (elts wordv)) indexval)
		(do-choices (phrase (tryif phrasemap
					   (get phrasemap (elts wordv))))
		  (when (search phrase wordv)
		    (add! index (cons 'words (seq->phrase phrase)) elt)))
		(when rootv
		  (store! xml 'roots rootv)
		  (add! index (cons 'roots (elts wordv)) indexval)
		  (do-choices (phrase (tryif phrasemap
					     (get phrasemap (elts rootv))))
		    (when (search phrase rootv)
		      (add! index (cons 'words (seq->phrase phrase)) elt)))))))
	  (when (exists? content)
	    (dolist (elt content)
	      (dom/index! index elt settings slotids
			  rootfn phrasemap useids)))))))

