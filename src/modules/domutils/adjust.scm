;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'domutils/adjust)

(use-module '{reflection fdweb xhtml texttools domutils varconfig logger})

(module-export! '{dom/adjust! dom/edit! dom/parsedit})

(define (dom/adjust! doc selector (action #f))
  (cond ((and (procedure? selector) (not action))
	 (dom/map! doc selector))
	((procedure? action)
	 (let ((nodes (dom/find doc selector)))
	   (do-choices (node nodes) (action node))))
	((string? action) (dom/edit! (dom/find doc selector) action))
	(action (error "Invalid DOM/ADJUST action" action))
	((and (pair? selector) (or (procedure? (cdr selector)) (string? (cdr selector))))
	 (dom/adjust! doc (car selector) (cdr selector)))
	((pair? selector)
	 (dolist (rule selector) (dom/adjust! doc rule)))
	(else (error "Invalid DOM/ADJUST selector" action))))

(define edit-action-rules
  {#((bol) (label tagname #((isalpha) (* (isalnum)))))
   #("." (label addclass #((isalpha) (* {(isalnum) "-" "_"}))))
   #(".=" (label setclass #((isalpha) (* {(isalnum) "-" "_"}))))
   #(".-" (label dropclass #((isalpha) (* {(isalnum) "-" "_"}))))
   #(".+" (label addclass #((isalpha) (* {(isalnum) "-" "_"}))))
   #("#" (label setid #((isalpha) (+ {(isalnum) "-" "_"}))))
   #("[" (label setattrib #((isalpha) (+ {(isalnum) "-" "_"})))
     "=\"" (label val (not> "\"")) "\"]")
   #("[" (label setattrib #((isalpha) (+ {(isalnum) "-" "_"})))
     "='" (label val (not> "'")) "\']")
   #("[" (label setattrib #((isalpha) (+ {(isalnum) "-" "_"})))
     "=+\"" (opt (label punct {(ispunct) (isspace)}))
     (label addval (not> "\"")) "\"]")
   #("[" (label setattrib #((isalpha) (+ {(isalnum) "-" "_"})))
     "=+'" (opt (label punct {(ispunct) (isspace)}))
     (label addval (not> "'")) "\']")
   #("[-" (label dropattrib #((isalpha) (+ {(isalnum) "-" "_"}))))})

(define (dom/parsedit string)
  (text->frames (qc edit-action-rules) string))

(defambda (dom/edit! node action)
  (do-choices action
    (if (string? action)
	(let* ((parsed (text->frames (qc edit-action-rules) action))
	       (setclass (get parsed 'setclass))
	       (newtagname (string->symbol (get parsed 'tagname)))
	       (dropclass (get parsed 'dropclass))
	       (addclass (choice setclass (get parsed 'addclass)))
	       (setid (get parsed 'setid))
	       (dropattrib (get parsed 'dropattrib))
	       (setattrib (reject (pick parsed 'setattrib) 'setattrib dropattrib)))
	  (do-choices node
	    (if (exists? setclass)
		(dom/set! node 'class
			  (stringout
			    (do-choices (classname (difference (choice setclass addclass)
							       dropclass)
						   i)
			      (printout (if (> i 0) " ") classname))))
		(let ((classes (try (segment (get node 'class)) '())))
		  (do-choices (add addclass)
		    (unless (position add classes)
		      (set! classes (append classes (list add)))))
		  (do-choices (drop dropclass)
		    (when (position drop classes)
		      (set! classes (remove drop classes))))
		  (dom/set! node 'class
			    (stringout (doseq (name classes i)
					 (printout (if (> i 0) " ") name)))))))
	  (when (exists? newtagname) (store! node '%xmltag newtagname))
	  (when (exists? setid) (dom/set! node 'id setid))
	  (do-choices (drop dropattrib) (dom/drop! node drop))
	  (do-choices (edit setattrib)
	    (if (test edit 'addval)
		(dom/add! node (get edit 'setattrib) (get edit 'addval)
			  (try (get edit 'sep) " "))
		(dom/set! node (get edit 'setattrib) (get edit 'val))))))))



