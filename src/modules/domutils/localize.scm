;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'domutils/localize)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM
(define version "$Id$")
(define revision "$Revision: 5083 $")

(use-module '{fdweb texttools domutils aws/s3
	      savecontent gpath logger mimetable})

(define-init %loglevel %notice!)
;;(set! %loglevel %info%)
;;(set! %loglevel %debug%)

(module-export! '{dom/localize!})
(module-export! '{dom/getmanifest dom/textmanifest dom/datamanifest})
(module-export! '{dom/getcssurls})

(define (chkdir filename)
  (if (file-directory? (dirname filename))
      filename
      (begin (system "mkdir -p " (dirname filename))
	filename)))

(define (addversion string num)
  (if (textsearch #("." (isalpha+) (eos)) string)
      (textsubst string `#((SUBST "." ,(glom "-" num ".")) (isalpha+) (eos)))
      (glom string "-" num)))

(define absurlstart #((isalpha) (isalpha) (isalpha+) ":")) ;; (choice  "/")

(define (getabsref ref (base #f))
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

(define (fix-crlfs string)
  (string-subst (string-subst string "\r\n" "\n")
		"\r" "\n"))

(define (needsync? savepath absref)
  (let ((current (gp/modified savepath))
	(remote  (gp/modified absref)))
    (or (not current) (not remote) (time>? current remote))))

(define gp->s gpath->string)

(define (sync! ref savepath absref options ctype urlmap
	       (checksync) (exists))
  (default! urlmap (getopt options 'urlmap))
  (default! checksync
    (getopt options 'checksync
	    (not (getopt options 'updateall (config 'updateall)))))
  (default! exists (gp/exists? savepath))
  (logdebug |LOCALIZE/sync| "Syncing " ref " with " savepath " from " absref)
  (cond ((and checksync exists (needsync? savepath absref))
	 (logdebug "Content " ref " is up to date in " (gp->s savepath)
		   " from " (gp->s absref))
	 #t)
	(else
	 (let ((fetched
		(onerror (gp/fetch+ absref)
		  (lambda (ex)
		    (logwarn |LOCALIZE/sync| "Error fetching " absref ":\n\t" ex)
		    #f)))
	       (xform (getopt options 'xform)))
	   (cond ((and exists (not fetched))
		  (logwarn |LOCALIZE/sync| "Couldn't update content for " ref
			   " from " (gp->s absref) ", using current " (gp->s savepath)))
		 ((or (not fetched)
		      (fail? (get fetched 'content))
		      (not (get fetched 'content)))
		  (logwarn |LOCALIZE/sync| "Couldn't download content from " (gp->s absref)
			   " for " ref))
		 (xform (gp/save! savepath (xform (get fetched 'content)) ctype))
		 (else (gp/save! savepath (get fetched 'content) ctype)))
	   (when (and fetched (test fetched 'content)
		      (or (string? (get fetched 'content))
			  (packet? (get fetched 'content))))
	     (lognotice "Copied " (length (get fetched 'content))
			" from\n\t" (gp->s absref)
			"\n  to\t " (gp->s savepath)
			"\n  for\t" ref)
	     (store! urlmap (list absref) (get fetched 'modified)))
	   (or fetched exists)))))

;;;; LOCALREF

;; Converts a URL into a local reference, copying its data if needed
;; REF is the local references (from the document)
;; URLMAP is a table of REFs which have already been localized
;; BASE is the URL of the input document which is used to actually fetch things
;; SAVETO is the where downloaded data should be stored locally
;; READ is the relative path to use for local references
(define (localref ref urlmap base saveto read options (ctype) (xform))
  (default! ctype (getopt options 'mimetype (path->mimetype (gp/basename ref))))
  (default! xform (getopt options 'xform #f))
  (when (position #\% ref) (set! ref (uridecode ref)))
  (logdebug |LOCALIZE/ref| ref "\n\tfrom " base "\n\tto " saveto "\n\tfor " read)
  (try ;; relative references are untouched
   (tryif (or (empty-string? ref) (has-prefix ref "#")
	      (has-prefix ref {"javascript:" "chrome-extension:"}))
     ref)
   ;; If it's got a fragment identifer, make a localref without the
   ;;  fragment and put the fragment back.  We don't bother checking
   ;; fragment ID uniqueness, though we probably should.
   (tryif (position #\# ref)
     (if (has-prefix (get urlmap ref) "#")
	 (get urlmap ref)
	 (let* ((hashpos (position #\# ref))
		(baseuri (subseq ref 0 hashpos))
		(hashid (subseq ref hashpos))
		(lref (localref baseuri urlmap base saveto read options))
		(useref hashid))
	   (if (has-prefix lref "#")
	       (begin
		 ;; To be correct, we'll need to clean up these
		 ;;  references later, which we can catch by examining
		 ;;  the urlmap; in particular an urlmap key which
		 ;;  starts with #hash but isn't the suffix of its
		 ;;  value indicates a renaming.  We can then get the
		 ;;  section id and the local reference in order to
		 ;;  find the node we need to rename.  This is not yet
		 ;;  implemented.
		 (when (and (test urlmap useref) (not (test urlmap useref ref)))
		   (set! useref (glom lref "-" hashid)))
		 (store! urlmap (vector useref) ref)
		 (store! urlmap ref useref)
		 useref)
	       ;; Not clear what the right to do is when the baseuri
	       ;; isn't amalgamated into the same namespace, so we
	       ;; leave the ref as it is
	       ref))))
   ;; if we're gluing a bunch of files together (amalgamating them),
   ;;  the ref will just be moved to the current file by stripping
   ;;  off the URL part
   (tryif (overlaps? (getopt options 'amalgamate)
		     (gp/mkpath base ref))
     (try (get urlmap (gp/mkpath base ref))
	  (get urlmap ref)
	  ""))
   ;; Check the cache
   (get urlmap ref)
   ;; don't bother localizing these references
   (tryif (exists string-starts-with? ref
		  (getopt options 'localhosts {}))
     ref)
   ;; No easy outs, fetch the content and store it
   (let* ((absref (getabsref ref base))
	  (name (gp/basename ref))
	  (suffix (filesuffix name))
	  (lref (try (get urlmap ref)
		     (get urlmap absref)
		     (mkpath read name)))
	  (savepath (gp/mkpath saveto name)))
     ;; We store inverse links as #(ref) keys, so this is where we detect
     ;;  conflicts
     (when (and (fail? (get urlmap (vector lref)))
		(exists? (get urlmap lref)))
       ;; Filename conflict
       (set! name (glom (packet->base16 (md5 absref)) suffix))
       (set! lref (mkpath read name))
       (set! savepath (gp/mkpath saveto name)))
     (store! urlmap ref lref)
     (store! urlmap (vector lref) ref)
     (when (string? absref) (store! urlmap absref lref))
     (debug%watch "LOCALREF" lref ref base absref saveto read
		  (get urlmap absref))
     (if (sync! ref savepath absref options ctype urlmap)
	 (begin
	   ;; Save the mapping in both directions (we assume that
	   ;;  lrefs and absrefs are disjoint, so we can use the
	   ;;  same table)
	   (store! urlmap absref lref)
	   (when (string? absref) (store! urlmap lref absref))
	   (logdebug |LOCALIZE/ref| "LOCALREF " ref "==>" lref
		     ",\n\tsynced from " base "\n\tto " saveto)
	   lref)
	 (begin 
	   (logwarn |LOCALIZE/sync|
		    "Couldn't sync " ref "==>" lref
		    ",\n\tsynced from " base "\n\tto " saveto)
	   ref)))))

(define (dom/localize! dom base saveto read (options #f)
		       (urlmap) (doanchors) (dolinks))
  (default! urlmap (getopt options 'urlmap (make-hashtable)))
  (default! doanchors (getopt options 'doanchors #f))
  (default! dolinks (getopt options 'synclinks {}))
  (loginfo "Localizing references for "
	   (try (gpath->string (get dom 'source)) "source")
	   "\n\tfrom " (write (gp->s base))
	   "\n\tto " (write read) ", copying content to "
	   (if (singleton? saveto) (write (gp->s saveto))
	       (do-choices saveto
		 (printout "\n\t\t" (write (gp->s saveto))))))
  (debug%watch "DOM/LOCALIZE!" base saveto read options doanchors)
  (let ((head (dom/find dom "HEAD" #f)) (files {}))
    (dolist (node (dom/find->list dom "[src]"))
      (logdebug "Localizing " (dom/sig node)
		"\n\tfrom " base "\n\tto " saveto
		"\n\t" node)
      (let ((ref (localref (get node 'src) urlmap
			   base (qc saveto) read options)))
	(loginfo "Localized " (write (get node 'src))
		 " to " (write ref) " for " (dom/sig node #t))
	(when (and (exists? ref) ref)
	  (dom/set! node 'src ref)
	  (set+! files ref))))
    ;; Convert url() references in stylesheets
    (do-choices (node (pick (pick (dom/find head "link") 'rel "stylesheet")
			    'href))
      (logdebug "Localizing stylesheet " (get node 'href)
		"\n\tfrom " base "\n\tto " saveto
		"\n\t" node)
      (let* ((ctype (try (get node 'type) "text"))
	     (href (get node 'href))
	     (xformurlfn
	      (lambda (url)
		(localref url urlmap
			  (if (position #\/ href)
			      (gp/mkpath base (dirname href))
			      base)
			  (qc saveto) (mkpath ".." read)
			  options)))
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
			    (if (exists has-prefix (get node 'type) "text/css")
				(cons `#[mimetype
					 ,(try (get node 'type) "text/css")
					 xform ,xformcss]
				      options)
				options))))
	(loginfo "Localized " (write href) " to " (write ref)
		 " for " (dom/sig node #t))
	(when (and (exists? ref) ref)
	  (dom/set! node 'href ref)
	  (set+! files ref))))
    (dolist (node (dom/find->list dom "[href]"))
      (when (or (test node '%xmltag 'a)
		(test node 'rel {dolinks "stylesheet" "knodule"}))
	(logdebug "Localizing " (get node 'href) "\n\tfrom " base
		  "\n\tto " saveto "\n\tfor " node)
	(let* ((href (get node 'href))
	       (ref (and (not (string-starts-with?
			       href #((isalpha) (isalpha) (isalpha+) ":")))
			 (or (not doanchors) (textsearch doanchors href))
			 (localref href urlmap base (qc saveto) read options))))
	  (loginfo "Localized " (write href) " to " (write ref)
		   " for " (dom/sig node #t))
	  (when (and (exists? ref) ref)
	    (dom/set! node 'href ref)
	    (set+! files ref)))))
    (dolist (node (dom/find->list dom "[href]"))
      (unless (test node 'rel {dolinks "x-resource" "stylesheet" "knodule"})
	(let* ((href (get node 'href))
	       (hashpos (position #\# href)))
	  (cond ((test urlmap href)
		 (dom/set! node 'href (get urlmap href)))
		((and hashpos (test urlmap (slice href 0 hashpos)))
		 (get urlmap (slice href 0 hashpos))
		 (dom/set! node 'href (slice href hashpos)))))))
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

(define url-prefix-pat #((maxlen (isalpha+) 10) ":"))

(define (dom/getmanifest doc)
  (choice (for-choices (link (dom/find doc "LINK"))
	    (tryif (or (test link 'rel "stylesheet")
		       (test link 'rel "knowlet")
		       (test link 'rel "knodule")
		       (and (test link 'href)
			    (fail? (textmatcher url-prefix-pat
						(get link 'href)))))
	      (get link 'href)))
	  (get (dom/find doc "SCRIPT") 'src)
	  (get (dom/find doc "IMG") 'src)
	  (get (dom/find doc "IMAGE") 'href)))

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



