;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'domutils/localize)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM
(define version "$Id$")
(define revision "$Revision: 5083 $")

(use-module '{fdweb texttools domutils aws/s3
	      savecontent gpath logger mimetable})

(define-init %loglevel %notice!)
;;(set! %loglevel %debug%)

(module-export! '{dom/localize!})
(module-export! '{dom/getmanifest dom/textmanifest dom/datamanifest})
(module-export! '{dom/getcssurls})

(define (chkdir filename)
  (if (file-directory? (dirname filename))
      filename
      (begin (system "mkdir -p " (dirname filename))
	     filename)))

(define absurlstart #((isalpha) (isalpha) (isalpha+) ":")) ;; (choice  "/")

(define (addversion string num)
  (if (textsearch #("." (isalpha+) (eos)) string)
      (textsubst string `#((SUBST "." ,(glom "-" num ".")) (isalpha+) (eos)))
      (glom string "-" num)))

;; Converts a URL into a local reference, copying its data if needed
;; REF is the local references (from the document)
;; URLMAP is a table of REFs which have already been localized
;; BASE is the URL of the input document which is used to actually fetch things
;; SAVETO is the where downloaded data should be stored locally
;; READ is the relative path to use for local references
;; AMALGAMATE is a list of URL prefixes which are being amalgamated
;;   into the output file, so that fragment identifiers should just
;;   be the corresponding fragments
;; LOCALHOSTS is a bunch of prefixes for things to not bother converting
(define (localref ref urlmap base saveto read
		  amalgamate localhosts
		  (ctype) (xform #f))
  (default! ctype (path->mimetype (gp/basename ref)))
  (try ;; relative references are untouched
       (tryif (or (empty-string? ref) (has-prefix ref "#")) ref)
       ;; If it's got a fragment identifer, make a localref without the
       ;;  fragment and put the fragment back.  We don't bother checking
       ;;; fragment ID uniqueness, though we probably should (it would
       ;;; be pretty hard to fix automatically)
       (tryif (position #\# ref)
	 (let ((hashpos (position #\# ref)))
	   (string-append (localref (subseq ref 0 hashpos)
				    urlmap base saveto read
				    (qc amalgamate) (qc localhosts))
			  (subseq ref hashpos))))
       ;; if we're gluing a bunch of files together (amalgamating them),
       ;;  the ref will just be moved to the current file by stripping
       ;;  off the URL part
       (tryif (overlaps? amalgamate (gp/mkpath base ref)) "")
       ;; Check the cache
       (get urlmap ref)
       ;; don't bother localizing these references
       (tryif (string-starts-with? ref localhosts) ref)
       ;; No easy outs, fetch the content and store it
       (let* ((absref
	       (if (string-starts-with? ref absurlstart) ref
		   (if (string? base)
		       (if (has-prefix ref "./")
			   (mkuripath (if (has-suffix base "/") base
					  (dirname base))
				      (slice ref 2))
			   (if (has-prefix ref "../")
			       (mkuripath (if (has-suffix base "/")
					      (dirname base)
					      (dirname (dirname base)))
					  (slice ref 3))
			       (mkuripath (if (has-suffix base "/") base
					      (dirname base))
					  ref)))
		       (if (has-prefix ref "./")
			   (gp/path (gp/location base) (slice ref 2))
			   (if (has-prefix ref "../")
			       (gp/path (gp/location (gp/location base))
					(subseq ref 3))
			       (gp/path base ref))))))
	      (name (basename (uribase ref)))
	      (suffix (filesuffix name))
	      (lref (try (get urlmap (vector ref))
			 (mkpath read name)))
	      (savepath (gp/mkpath saveto name)))
	 (debug%watch "LOCALIZE" ref base absref saveto read
		      (get urlmap absref))
	 (when (and (not (get urlmap (vector ref)))
		    (exists? (get urlmap lref)))
	   ;; Name conflict
	   (set! name (glom (packet->base16 (md5 absref)) suffix))
	   (set! lref (mkpath read name))
	   (set! savepath (gp/mkpath saveto name)))
	 (debug%watch "URLMAP" ref lref absref)
	 (store! urlmap ref lref)
	 (store! urlmap lref ref)
	 (store! urlmap (vector ref) lref)
	 (when (string? absref) (store! urlmap absref lref))
	 (let* ((sourcetime (gp/modified absref))
		(existing (gp/exists? savepath))
		(savetime (and existing (gp/modified savepath)))
		(changed (or (not savetime) (not sourcetime)
			     (time>? sourcetime savetime)))
		(fetched (and changed (gp/fetch+ absref))))
	   (cond ((and changed fetched)
		  ;; Updated
		  (if xform
		      (gp/write! saveto name
			(xform (get fetched 'content)) ctype)
		      (gp/write! saveto name (get fetched 'content) ctype))
		  (store! urlmap (list absref)
			  (get fetched 'modified))
		  (loginfo "Updated " ref " for " lref
			   " from " (write absref)))
		 ((not changed)
		  ;; No update needed
		  (loginfo "Content " ref " is up to date for " lref
			   " from " (write absref)))
		 ((and existing (not fetched))
		  ;; Update failed
		  (logwarn "Couldn't update content from " absref))
		 ((not fetched)
		  ;; Initial download failed, keep absref
		  (logwarn "Couldn't download content from " absref)
		  (set! lref absref)))
	   ;; Save the mapping in both directions (we assume that
	   ;;  lrefs and absrefs are disjoint, so we can use the
	   ;;  same table)
	   (store! urlmap absref lref)
	   (store! urlmap lref absref)
	   lref))))

(define (fix-crlfs string)
  (string-subst (string-subst string "\r\n" "\n")
		"\r" "\n"))

(define (dom/localize! dom base saveto read
		       (urlmap (make-hashtable))
		       (amalgamate #f) (localhosts #f)
		       (doanchors #f))
  (loginfo "Localizing references from " (write base)
	   " to " (write read) ", copying content to " 
	   (if (singleton? saveto) (write saveto)
	       (do-choices saveto (printout "\n\t" (write saveto)))))
  (let ((amalgamate (or amalgamate {}))
	(localhosts (or localhosts {}))
	(head (dom/find dom "HEAD" #f))
	(files {}))
    (dolist (node (dom/find->list dom "img"))
      (let ((ref (localref (get node 'src)
			   urlmap base (qc saveto) read
			   (qc amalgamate) (qc localhosts))))
	(logdebug "Localized " (write (get node 'src))
		  " to " (write ref) " for " node)
	(when (and (exists? ref) ref)
	  (dom/set! node 'src ref)
	  (set+! files ref))))
    (do-choices (node (pick (dom/find head "link") 'rel "stylesheet"))
      (let* ((ctype (try (get node 'type) "text"))
	     (xformurlfn
	      (lambda (url)
		(localref url urlmap (gp/mkpath base (get node 'href))
			  (qc saveto) (mkpath ".." read)
			  (qc amalgamate) (qc localhosts))))
	     (xformrule
	      `(IC (GREEDY #("url" (spaces*)
			     {#("(" (spaces*)
				(subst (not> ")") ,xformurlfn)
				(spaces*) ")")
			      #("('" (spaces*)
				(subst (not> "')") ,xformurlfn)
				(spaces*) "')")
			      #("(\"" (spaces*)
				(subst (not> "\")") ,xformurlfn)
				(spaces*) "\")")}))))
	     (xformcss (lambda (css)
			 (if (string? css)
			     (if (textsearch '(IC #("url" (spaces*) "(")) css)
				 (textsubst (fix-crlfs css) xformrule)
				 (fix-crlfs css))
			     css)))
	     (ref (localref (get node 'href)
			    urlmap base (qc saveto) read
			    (qc amalgamate) (qc localhosts)
			    (try (get node 'type) "text/css")
			    (and (exists has-prefix (get node 'type) "text/css")
				 xformcss))))
	(logdebug "Localized " (write (get node 'href))
		  " to " (write ref) " for " node)
	(when (and (exists? ref) ref)
	  (dom/set! node 'href ref)
	  (set+! files ref))))
    (do-choices (node (pick (dom/find dom "script") 'src))
      (let ((ref (localref (get node 'src)
			   urlmap base (qc saveto) read
			   (qc amalgamate) (qc localhosts))))
	(when (and (exists? ref) ref)
	  (dom/set! node 'src ref)
	  (set+! files ref))))
    (dolist (node (dom/find->list dom "a"))
      (let* ((href (try (get node 'href) #f))
	     (ref (and href
		       (not (string-starts-with?
			     href #((isalpha) (isalpha) (isalpha+) ":")))
		       (or (not doanchors) (textsearch doanchors href))
		       (localref href urlmap base (qc saveto) read
				 (qc amalgamate) (qc localhosts)))))
	(when (and (exists? ref) ref)
	  (dom/set! node 'href ref)
	  (set+! files ref))))
    (let ((xresources '()))
      (do-choices (resource (pick (pickstrings (get urlmap (getkeys urlmap)))
				  has-prefix (choice read (glom "../" read))))
	(set! xresources
	      (cons* `#[%XMLTAG LINK %ATTRIBIDS {REL HREF}
			REL "x-resource"
			HREF ,(if (has-prefix resource "../")
				  (slice resource 3)
				  resource)]
		     "\n"
		     xresources)))
      (store! head '%content
	      (append (get head '%content) xresources)))))

;;;; Manifests

(define (dom/getmanifest doc)
  (choice (for-choices (link (dom/find doc "LINK"))
	    (tryif (or (test link 'rel "stylesheet")
		       (test link 'rel "knowlet")
		       (test link 'rel "knodule"))
	      (get link 'href)))
	  (get (dom/find doc "SCRIPT") 'src)
	  (get (dom/find doc "IMG") 'src)))

(defambda (dom/textmanifest node (staticrefs {}) (dynamicrefs {}))
  "Generates a text manifest for a document, with additional static or dynamic refs"
  (let* ((urls (dom/getmanifest node))
	 (static (reject urls string-contains? "?"))
	 (dynamic (pick urls string-contains? "?"))
	 (given (choice staticrefs dynamicrefs)))
    (stringout "CACHE MANIFEST\n"
	       (do-choices (url (choice staticrefs (difference static given)))
		 (lineout url))
	       (when (or (exists? dynamic) (exists? dynamicrefs))
		 (lineout "NETWORK:")
		 (do-choices (url (choice dynamicrefs (difference dynamic given)))
		   (lineout url))))))

(defambda (dom/datamanifest node (staticrefs {}) (dynamicrefs {}))
  "Generates a data: URI containing a manifest.  Unfortunately, this \
   doesn't seem to work in the browsers I've tested, for all it's \
   cleverness."
  (let* ((manifest (dom/textmanifest node staticrefs dynamicrefs))
	 (packet (string->packet manifest))
	 (base64 (packet->base64 packet)))
    (string-append
     "data:text/cache-manifest;charset=\"utf-8\";base64,"
     base64)))

;;;; Getting CSS urls

(define css-url-pattern
  '(IC (GREEDY
	#("url" (spaces*)
	  {#("(" (spaces*) (label url (not> ")")) (spaces*) ")")
	   #("('" (spaces*) (label url (not> "')")) (spaces*) "')")
	   #("(\"" (spaces*) (label url (not> "\")")) (spaces*) "\")")}))))

(define (dom/getcssurls text/css)
  (get (text->frames css-url-pattern text/css) 'url))

