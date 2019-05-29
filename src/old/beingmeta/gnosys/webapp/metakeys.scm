(in-module 'gnosys/webapp/metakeys)

(use-module 'texttools)
(use-module '{brico gnosys gnosys/urldb gnosys/metakeys
		    gnosys/webapp/userinfo})
(use-module '{fdweb xhtml xhtml/clickit xhtml/brico})

(module-export! '{metakey->html metakey->anchor})

;;; This is the new (hopefully reduced) gnosys/metakeys/web

(define (metakey->html tag (var #f) (selected #t))
  (let* ((oid (if (and (pair? tag) (exists oid? (cdr tag)))
		  (cdr tag)
		  (and (oid? tag) tag)))
	 (language (get-language))
	 (dterm (tryif oid (cached-dterm oid language)))
	 (gloss (tryif oid (pick-one (smallest (get-gloss oid language) length))))
	 (text (if (pair? tag) (car tag)
		   (if (string? tag) tag
		       (get-norm oid language)))))
    (span ((class "metakey")
	   (oid (if oid oid))
	   (dterm (ifexists dterm))
	   (gloss (ifexists gloss))
	   (resolved (if oid "yes"))
	   (title "")
	   (tag tag)
	   (text text))
      (when var
	(if selected
	    (input type "CHECKBOX" name var value tag "CHECKED")
	    (input type "CHECKBOX" name var value tag)))
      (span ((class "taghead")) text))))

(define (metakey->anchor url tag (var #f) (selected #t))
  (let* ((oid (if (and (pair? tag) (oid? (cdr tag)))
		  (cdr tag)
		  (and (oid? tag) tag)))
	 (language (get-language))
	 (dterm (tryif oid (cached-dterm oid language)))
	 (gloss (pick-one
		 (tryif oid (smallest (get-gloss oid language) length))))
	 (text (if (pair? tag) (car tag)
		   (if (string? tag) tag
		       (get-norm oid language)))))
    (anchor* url
	((class "metakey")
	 (oid (if oid oid))
	 (dterm (ifexists dterm))
	 (gloss (ifexists gloss))
	 (title "")
	 (text text))
      (when var
	(if selected
	    (input type "CHECKBOX" name var value tag "CHECKED")
	    (input type "CHECKBOX" name var value tag)))
      text)))





