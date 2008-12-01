(in-module 'xhtml/concept)

(use-module '{fdweb texttools xhtml xhtml/brico})
(use-module '{brico brico/lookup brico/dterms brico/wikipedia})

(define (uri-kludge s)
  (if (has-prefix s "http") s
      (if (and (has-prefix s "[")  (has-suffix s "]"))
	  (subseq s 1 -1)
	  s)))

(define (output-contextspan body weblink oid language)
  (let* ((contextid
	  (and oid
	       (stringout "CONTEXT"
		 (oid-hi oid) "/" (oid-lo oid) "." (random 65536))))
	 (wikiref
	  (try (tryif (eq? language @?en) (get brico2enwiki oid))
	       (get brico2wiki (cons language oid)))))
    (span ((class "cxtspan")
	   (title (stringout "(CLICK FOR CONTEXT) "
		    (try (get-short-gloss oid language) "")))
	   (onclick "fdb_showhide_onclick(event);")
	   (clicktotoggle contextid))
      (span ((class "term")
	     (oid (if oid oid))
	     (dterm (if oid (get-dterm oid language))))
	(xmleval body))
      (when (or oid weblink)
	(span ((class "context") (id contextid))
	  (when weblink
	    (anchor weblink
	      (img src "/graphics/outlink16x8.png" alt "link")))
	  (when oid
	    (anchor* (scripturl "http://www.bricobase.net/index.fdcgi"
		       'concept oid)
		((title "info at BRICOBASE"))
	      (img src "/graphics/diamond16.png" alt "brico")))
	  (when (exists? wikiref)
	    (anchor* (stringout "http://"
		       (get language 'iso639/1) ".wikipedia.org/wiki/"
		       (string-subst wikiref " " "_"))
		((title "info at Wikipedia"))
	      (img src "/graphics/wikipedia.png" alt "wikipedia")))
	  (when (test oid 'website)
	    (let* ((website (pick-one (get oid 'website)))
		   (uri (if (pair? website) (car website) website))
		   (text (if (pair? website) (cdr website) website)))
	      (anchor* (uri-kludge uri)
		  ((class "home_button") (title text))
		(img SRC "/graphics/Home-Icon16.png" BORDER 0 ALT "Website"))))
	  (when (or (test oid 'lat) (test oid 'lat/long))
	    (let* ((lat/long (pick-one (get oid 'lat/long)))
		   (lat (try (first lat/long) (get oid 'lat)))
		   (long (try (second lat/long) (get oid 'long)))
		   (z (try (tryif (and (exists? lat/long)
				       (> (length lat/long) 2))
				  (third lat/long))
			   8)))
	      (anchor* (scripturl "http://maps.google.com/maps"
			 "q" (stringout lat ","long)
			 "z" 8)
		  ((class "button"))
		(img src "/graphics/earth24.png"
		     border 0 alt "map" width 24 height 24)))))))))

(define (concept (oid #f) (term #f) (weblink #f) (xmlbody #f))
  (let* ((oid (if oid
		  (if (string? oid) (parse-arg oid) oid)
		  (or (and term (brico/ref term))
		      (singleton
		       (brico/ref (stdspace (stringout (xmleval xmlbody))))))))
	 (language (get-language))
	 (body xmlbody))
    (output-contextspan body weblink oid language)))

(module-export! '{concept output-contextspan})

