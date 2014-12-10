;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

(in-module 'domutils/localize)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM

(use-module '{fdweb texttools domutils aws/s3
	      savecontent gpath logger mimetable})

(define-init %loglevel %notice%)
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
  (cond ((string-starts-with? ref absurlstart) ref)
	((not base) ref)
	((string? base)
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
			    ref))))
	((gp/location? base) (gp/mkpath base ref))
	(else (gp/mkpath (gp/location base) ref))))

(define (fix-crlfs string)
  (string-subst (string-subst string "\r\n" "\n")
		"\r" "\n"))

(define (needsync? savepath absref (basetime #f))
  (let ((current (or basetime (gp/modified savepath)))
	(remote (gp/modified absref)))
    (or (not current) (not remote)
	(time>? remote current))))

(define gp->s gpath->string)

(define (sync! ref savepath absref options urlmap
	       (checksync) (exists))
  (default! urlmap (getopt options 'urlmap))
  (default! checksync
    (getopt options 'checksync
	    (getopt options 'basetime
		    (not (getopt options 'updateall (config 'updateall))))))
  (default! exists (gp/exists? savepath))
  (logdebug |LOCALIZE/sync|
    "Syncing " ref " with " (gp->s savepath) " from " (gp->s absref)
    (if exists " exists" " missing")
    (if checksync " checksync" " dontcheck"))
  (cond ((equal? (gp->s savepath) (gp->s absref))
	 (logwarn |DOMUTILS/LOCALIZE/sync!| "Identical paths: " savepath absref)
	 #t)
	((and checksync exists
	      (not (needsync? savepath absref (getopt options 'basetime))))
	 (logdebug |DOMUTILS/LOCALIZE/sync!|
		   "Content " ref " is up to date in " (gp->s savepath)
		   " from " (gp->s absref))
	 #t)
	(else
	 (logdebug |DOMUTILS/LOCALIZE/sync!|
	   "Updating out-of-date " ref " at " (gp->s savepath)
	   " from " (gp->s absref))
	 (let* ((fetched
		 (onerror (gp/fetch+ absref)
		   (lambda (ex)
		     (logwarn |LOCALIZE/sync|
			      "Error fetching " absref ":\n\t" ex)
		     (clear-errors!)
		     #f)))
		(ftype (and fetched (get fetched 'ctype)))
		(content (and fetched (get fetched 'content)))
		(xform (getopt options 'xform)))
	   (cond ((and exists (not fetched))
		  (logwarn |LOCALIZE/sync|
			   "Couldn't update content for " ref
			   " from " (gp->s absref) ", "
			   "using current " (gp->s savepath)))
		 ((or (not fetched)
		      (fail? (get fetched 'content))
		      (not (get fetched 'content)))
		  (logwarn |LOCALIZE/sync|
			   "Couldn't read content from " (gp->s absref)
			   " for " ref))
		 (else (onerror (gp/save! savepath
				  (if xform (xform (get fetched 'content))
				      (get fetched 'content))
				  ftype)
			 (lambda (ex)
			   (logwarn |LOCALIZE/sync/save|
				    "Couldn't update content for " ref
				    " from " (gp->s absref) ", "
				    "using current " (gp->s savepath))))))
	   (when (and fetched (test fetched 'content)
		      (or (string? content) (packet? content)))
	     (loginfo "Copied " (length content) " "
		      (if (packet? content) "bytes" "characters")
		      " of " (or ftype "stuff")
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
  (try ;; non http/ftp references are untouched
   (tryif (or (empty-string? ref) (has-prefix ref "#")
	      (has-prefix ref {"javascript:" "chrome-extension:"})
	      (and (string-starts-with? ref #((isalpha+) ":"))
		   (not (has-prefix ref {"http:" "https:" "ftp:"}))))
     ;; (begin (logdebug |LOCALIZE/ref| "Ignoring " ref) )
     ref)
   (tryif (test urlmap ref)
     (begin (logdebug |LOCALIZE/ref| "Cached " ref " ==> " (get urlmap ref))
       (get urlmap ref)))
   (begin (logdebug |LOCALIZE/ref| ref
		    "\n\tfrom " base "\n\tto " saveto "\n\tfor " read)
     (fail))
   (tryif (position #\# ref)
     (let* ((baseuri (uribase ref))
	    (hashid (urifrag ref))
	    (lref (try (get urlmap baseuri)
		       (if (overlaps? baseuri (getopt options 'amalagamate))
			   baseuri
			   (localref baseuri urlmap base
				     saveto read options)))))
       (logdebug |LOCALIZE/ref| ref
		 "\n\tfrom " base "\n\tto " saveto "\n\tfor " read)
       (debug%watch baseuri hashid lref)
       (glom lref "#" hashid)))
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
     (when (string? absref)
       (store! urlmap absref lref))
     (debug%watch "LOCALREF" lref ref base absref saveto read
		  (get urlmap absref))
     (if (sync! ref savepath absref options urlmap)
	 (begin
	   ;; Save the mapping in both directions (we assume that
	   ;;  lrefs and absrefs are disjoint, so we can use the
	   ;;  same table)
	   (store! urlmap absref lref)
	   (store! urlmap (vector lref) absref)
	   (when (string? absref) (store! urlmap lref absref))
	   (logdebug |LOCALIZE/ref| "LOCALREF " ref " ==> " lref
		     ",\n\tsynced from " base "\n\tto " saveto)
	   lref)
	 (begin 
	   (logwarn |LOCALIZE/sync|
		    "Couldn't sync " ref "==>" lref
		    ",\n\tsynced from " base "\n\tto " saveto)
	   ref)))))

(define image-suffixes
  (choice {".png" ".gif" ".jpg" ".jpeg" ".svg"}
	  (upcase {".png" ".gif" ".jpg" ".jpeg" ".svg"})))

(define (dom/localize! dom base saveto read (options #f)
		       (urlmap) (doanchors) (dolinks) (stylerules))
  (default! urlmap (getopt options 'urlmap (make-hashtable)))
  (default! doanchors (getopt options 'doanchors #f))
  (default! dolinks (getopt options 'synclinks {}))
  (default! stylerules (getopt options 'stylerules {}))
  (loginfo "Localizing references for "
	   (try (gpath->string (get dom 'source)) "source")
	   "\n\tfrom " (write (gp->s base))
	   "\n\tto " (write read) ", copying content to "
	   (if (singleton? saveto) (write (gp->s saveto))
	       (do-choices saveto
		 (printout "\n\t\t" (write (gp->s saveto))))))
  (debug%watch "DOM/LOCALIZE!" base saveto read doanchors)
  (let ((head (dom/find dom "HEAD" #f))
	(saveslot (getopt options 'saveslot)))
    (loginfo |Localize| "Localizing [src] elements")
    (dolist (node (dom/select->list dom "[src]"))
      (loginfo |Localize| "Localizing " (dom/sig node)
	       "\n\tfrom " (gpath->string base) "\n\tto " (gpath->string saveto))
      (let* ((cached (get urlmap (get node 'src)))
	     (ref (try cached
		       (begin
			 (logdebug "Localizing " (write (get node 'src))
				   " for " (dom/sig node #t))
			 (localref (get node 'src) urlmap
				   base (qc saveto) read options)))))
	(when (and (exists? ref) ref)
	  (if (exists? cached)
	      (loginfo |Localize|
		       "Localized (cached) " (write (get node 'src))
		       "\n\tto " (write ref) " for " (dom/sig node #t) " saved in"
		       (do-choices saveto (printout "\n\t\t" (gpath->string saveto))))
	      (loginfo |Localize|
		       "Localized " (write (get node 'src))
		       "\n\tto " (write ref) " for " (dom/sig node #t) " saved in"
		       (do-choices saveto (printout "\n\t\t" (gpath->string saveto)))))
	  (when saveslot (dom/set! node saveslot (get node 'src)))
	  (unless (test node 'data-origin) (dom/set! node 'data-origin (get node 'src)))
	  (dom/set! node 'src ref))))
    ;; Convert url() references in stylesheets
    (loginfo |Localize| "Localizing stylesheet links")
    (do-choices (node (pick (pick (dom/find head "link") 'rel "stylesheet")
			    'href))
      (let* ((ctype (try (get node 'type) "text"))
	     (href (get node 'href))
	     (ref (get urlmap href)))
	(when (or (fail? ref) (not ref))
	  (loginfo |Localize|
		   "Localizing stylesheet " (get node 'href)
		   "\n\tfrom " base "\n\tto " saveto
		   (when (exists? stylerules)
		     (printout "\n\twith CSS rules " stylerules)))
	  (let* ((usebase (if (position #\/ href)
			      (gp/mkpath base (dirname href))
			      base))
		 (fix-csstext
		  (if (exists? stylerules)
		      (lambda (csstext)
			(textsubst (fix-crlfs csstext) (qc stylerules)))
		      fix-crlfs))
		 (xformcss (lambda (css)
			     (if (string? css)
				 (if (textsearch '(IC #("url" (spaces*) "(")) css)
				     (convert-url-refs
				      (fix-csstext css) urlmap usebase
				      (qc saveto) read
				      options)
				     (fix-csstext css))
				 css)))
		 (options (if (exists has-prefix (get node 'type) "text/css")
			      (cons `#[basetime ,(getopt options 'consed)
				       ;; mimetype ,(try (get node 'type) "text/css")
				       xform ,xformcss]
				    options)
			      options)))
	    (set! ref (localref (get node 'href) urlmap base
				(qc saveto) read options))
	    (loginfo |Localize|
		     "Localized stylesheet " (write href) " to " (write ref)
		     " for " (dom/sig node #t))))
	(when (and (exists? ref) ref)
	  (when saveslot (dom/set! node saveslot href))
	  (dom/set! node 'href ref))))
    (loginfo |Localize| "Localizing anchorish [href] elements")
    (dolist (node (dom/select->list dom "[href]"))
      (when (and (or (and doanchors (test node '%xmltag 'a))
		     (test node 'rel {dolinks "knodule"}))
		 (not (string-starts-with?
		       (get node 'href)
		       #((isalpha) (isalpha) (isalpha+) ":")))
		 (or (and doanchors (immediate? doanchors))
		     (and doanchors
			  (textsearch doanchors (get node 'href)))))
	(logdebug |Localize|
		  "Localizing " (get node 'href) "\n\tfrom " base
		  "\n\tto " saveto)
	(let* ((href (get node 'href))
	       (hashpos (position #\# href))
	       (baseuri (slice href 0 hashpos))
	       (rootref (localref baseuri urlmap base (qc saveto)
				  read options))
	       (hashid (if hashpos (slice href hashpos) #f))
	       (ref (if (not hashid) rootref
			(if (has-prefix rootref "#")
			    (hashmerge rootref hashid urlmap)
			    (glom rootref hashid)))))
	  (debug%watch href baseuri rootref hashid ref)
	  (loginfo |Localize|
		   "Localized " (write href) " to " (write ref)
		   " for " (dom/sig node #t))
	  (when (and (exists? ref) ref)
	    (when (and saveslot (not (equal? ref href)))
	      (dom/set! node saveslot href))
	    (dom/set! node 'href ref)))))
    (loginfo |Localize| "Localizing other [href] elements")
    (dolist (node (dom/select->list dom "[href]"))
      (unless (or (test node 'rel {dolinks "x-resource" "stylesheet" "knodule"})
		  (test node '%xmltag 'a))
	(let* ((href (get node 'href))
	       (hashpos (position #\# href)))
	  (cond ((and (test urlmap href) (get urlmap href))
		 (when saveslot (dom/set! node saveslot href))
		 (dom/set! node 'href (get urlmap href)))
		((and hashpos (test urlmap (slice href 0 hashpos))
		      (get urlmap (slice href 0 hashpos)))
		 (when saveslot (dom/set! node saveslot href))
		 (dom/set! node 'href (slice href hashpos)))
		((has-suffix href image-suffixes)
		 (when saveslot (dom/set! node saveslot href))
		 (dom/set! node 'href
			   (or (localref href urlmap base (qc saveto)
					 read options)
			       href)))))))
    (let ((xresources '()) (files {}))
      (do-choices (resource (pick (pickstrings (get urlmap (getkeys urlmap)))
				  has-prefix read))
	(set+! files resource)
	(set! xresources
	      (cons* `#[%XMLTAG LINK %ATTRIBIDS {REL HREF}
			REL "x-resource"
			HREF ,resource]
		     "\n"
		     xresources)))
      (if (pair? options)
	  (add! (car options) 'xresources files)
	  (add! options 'xresources files))
      (add! dom 'xresources files)
      (store! head '%content
	      (append (get head '%content) xresources)))))

(define (hashmerge root hash urlmap)
  (cond ((fail? (get urlmap hash))
	 (store! urlmap (glom root hash) hash)
	 hash)
	((test urlmap (glom root hash) hash)
	 hash)
	(else (glom root (slice hash 1)))))

(define (convert-url-refs text urlmap base saveto read options)
  (let* ((xformurlfn
	  (lambda (url)
	    (let* ((useurl
		    (if (or (and (has-prefix url "'") (has-suffix url "'"))
			    (and (has-prefix url "\"") (has-suffix url "\"")))
			(slice url 1 -1)
			url))
		   (lref (localref useurl urlmap base saveto read options))
		   (useref (if (has-prefix lref (glom read "/"))
			       (slice lref (length (glom read "/")))
			       lref)))
	      (if (equal? url useurl) useref
		  (glom "'" useref "'")))))
	 (xformrule
	  `(IC (GREEDY #("url" (spaces*)
			 #("(" (subst (not> ")") ,xformurlfn) ")"))))))
    (textsubst text xformrule)))

;;;; Manifests

(define url-prefix-pat #((maxlen (isalpha+) 10) ":"))

(define (dom/getmanifest doc (skiprels #f))
  (choice (for-choices (link (dom/find doc "LINK"))
	    (tryif (and
		    (test link 'href)
		    (or (not skiprels)
			(not (test link 'rel))
			(not (if (applicable? skiprels)
				 (skiprels (get link 'rel))
				 (textmatch skiprels (get link 'rel)))))
		    (or (test link 'rel "stylesheet")
			(test link 'rel "knowlet")
			(test link 'rel "knodule")
			(fail? (textmatcher url-prefix-pat (get link 'href)))))
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



