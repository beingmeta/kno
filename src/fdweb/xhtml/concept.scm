;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc. All rights reserved

(in-module 'xhtml/concept)

;;; This provides for augmented HTML display with clickable items
;;;  that reveals display/exit options.

(define version "$Id$")

(use-module '{fdweb texttools xhtml xhtml/brico xhtml/beingmeta})
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
	      (img src (mkpath bmstatic "graphics/outlink16x8.png") alt "link")))
	  (when oid
	    (anchor* (scripturl "http://www.bricobase.net/index.fdcgi"
		       'concept oid)
		((title "info at BRICOBASE")
		 (target "_blank"))
	      (img src (mkpath bmstatic "graphics/diamond16.png") alt "brico")))
	  (when (exists? wikiref)
	    (anchor* (stringout "http://"
		       (get language 'iso639/1) ".wikipedia.org/wiki/"
		       (string-subst wikiref " " "_"))
		((title "info at Wikipedia")
		 (target "_blank"))
	      (img src (mkpath bmstatic "graphics/wikipedia.png") alt "wikipedia")))
	  (when (test oid 'website)
	    (let* ((website (pick-one (get oid 'website)))
		   (uri (if (pair? website) (car website) website))
		   (text (if (pair? website) (cdr website) website)))
	      (anchor* (uri-kludge uri)
		  ((class "home_button") (title text))
		(img SRC (mkpath bmstatic "graphics/Home-Icon16.png") BORDER 0 ALT "Website"))))
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
		  ((title "see at Google maps")
		   (target "_blank"))
		(img src (mkpath bmstatic "graphics/earth16.png") alt "map")))))))))

(define (concept (oid #f) (term #f) (weblink #f) (xmlbody #f))
  (let* ((oid (if oid
		  (if (string? oid) (parse-arg oid) oid)
		  (or (and term (brico/ref term))
		      (singleton
		       (brico/ref (stdspace (stringout (xmleval xmlbody))))))))
	 (language (get-language)))
    (if (exists? oid)
	(output-contextspan xmlbody weblink oid language)
	(xmleval xmlbody))))

(module-export! '{concept output-contextspan})

