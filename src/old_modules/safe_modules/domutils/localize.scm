;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'domutils/localize)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM

(use-module '{fdweb texttools regex reflection domutils aws/s3
	      varconfig savecontent gpath logger mimetable})

(define-init %loglevel %notice%)
;;(set! %loglevel %info%)
;;(set! %loglevel %debug%)

(module-export! '{dom/localize!})
(module-export! '{dom/getmanifest dom/textmanifest dom/datamanifest})
(module-export! '{dom/getcssurls})

(define synclinks {"knodule" "x-resource" "stylesheet"})
(varconfig! domutils:synclinks synclinks)

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

(define (sync! ref savepath absref options urlmap
	       (checksync) (exists))
  (default! urlmap (getopt options 'urlmap))
  (default! checksync
    (getopt options 'checksync
	    (getopt options 'basetime
		    (not (getopt options 'updateall (config 'updateall))))))
  (default! exists (gp/exists? savepath))
  (logdebug |LOCALIZE/sync|
    "Syncing " ref " with " (gp/string savepath) " from " (gp/string absref)
    (if exists " exists" " missing")
    (if checksync " checksync" " dontcheck"))
  (cond ((equal? (gp/string savepath) (gp/string absref))
	 (logwarn |DOMUTILS/LOCALIZE/sync!| "Identical paths: "
		  savepath " == " absref)
	 #t)
	((and checksync exists
	      (not (needsync? savepath absref (getopt options 'basetime))))
	 (logdebug |DOMUTILS/LOCALIZE/sync!|
		   "Content " ref " is up to date in " (gp/string savepath)
		   " from " (gp/string absref))
	 #t)
	(else
	 (logdebug |DOMUTILS/LOCALIZE/sync!|
	   "Updating out-of-date " ref " at " (gp/string savepath)
	   " from " (gp/string absref))
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
			   " from " (gp/string absref) ", "
			   "using current " (gp/string savepath)))
		 ((or (not fetched)
		      (fail? (get fetched 'content))
		      (not (get fetched 'content)))
		  (logwarn |LOCALIZE/sync|
			   "Couldn't read content from " (gp/string absref)
			   " for " ref))
		 (else (onerror (gp/save! savepath
				  (if xform (xform (get fetched 'content))
				      (get fetched 'content))
				  ftype)
			 (lambda (ex)
			   (logwarn |LOCALIZE/sync/save|
				    "Couldn't update content for " ref
				    " from " (gp/string absref) ", "
				    "using current " (gp/string savepath))))))
	   (when (and fetched (test fetched 'content)
		      (or (string? content) (packet? content)))
	     (loginfo "Copied " (length content) " "
		      (if (packet? content) "bytes" "characters")
		      " of " (or ftype "stuff")
		      " from\n\t" (gp/string absref)
		      "\n  to\t " (gp/string savepath)
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
(define (localref ref urlmap base saveto resources options (ctype) (xform) (absref))
  (default! ctype (getopt options 'mimetype
			  (path->mimetype (gp/basename ref))))
  (default! xform (getopt options 'xform #f))
  (when (position #\% ref) (set! ref (uridecode ref)))
  (default! absref (getabsref ref base))
  (try ;; non http/ftp references are untouched
   (tryif (or (empty-string? ref) (has-prefix ref "#")
	      (has-prefix ref {"javascript:" "chrome-extension:"})
	      (and (string-starts-with? ref #((isalpha+) ":"))
		   (not (has-prefix ref {"http:" "https:" "ftp:"}))))
     ;; (begin (logdebug |LOCALIZE/ref| "Ignoring " ref) )
     ref)
   (tryif (test urlmap absref)
     (begin (logdebug |LOCALIZE/ref| "Cached " absref " ==> " (get urlmap absref))
       (get urlmap absref)))
   (begin (logdebug |LOCALIZE/ref| 
	    ref " " absref "\n\tfrom " base "\n\tto " saveto "\n\tfor " resources)
     {})
   (tryif (position #\# ref)
     (let* ((baseuri (uribase ref))
	    (hashid (urifrag ref))
	    (lref (try (get urlmap (getabsref baseuri base))
		       (if (overlaps? baseuri (getopt options 'amalagamate))
			   baseuri
			   (localref baseuri urlmap base
				     saveto resources options)))))
       (logdebug |LOCALIZE/ref|
	 ref " " absref "\n\tfrom " base "\n\tto " saveto "\n\tfor " resources)
       (debug%watch baseuri hashid lref)
       (glom lref "#" hashid)))
   ;; Check the cache
   (get urlmap absref)
   ;; don't bother localizing these references
   (tryif (exists string-starts-with? ref
		  (getopt options 'localhosts {}))
     ref)
   ;; No easy outs, fetch the content and store it
   (let* ((name (gp/basename ref))
	  (suffix (path-suffix name))
	  (lref (try (get urlmap absref)
		     (mkpath resources name)))
	  (savepath (gp/mkpath saveto name)))
     ;; We store inverse links as #(ref) keys, so this is where we detect
     ;;  conflicts
     (when (and (not (test urlmap lref absref))
		(test urlmap (vector lref)))
       ;; Filename conflict
       (set! name (glom (if (string? absref)
			    (packet->base16 (md5 absref))
			    (if (and (pair? absref) (string? (cdr absref)))
				(packet->base16 (md5 (cdr absref)))
				(md5 (gp/string absref))))
		    suffix))
       (set! lref (mkpath resources name))
       (set! savepath (gp/mkpath saveto name)))
     (store! urlmap absref lref)
     (store! urlmap (vector lref) absref)
     (debug%watch "LOCALREF" lref ref base absref saveto resources
		  (get urlmap absref))
     (if (sync! ref savepath absref options urlmap)
	 (begin
	   ;; Save the mapping in both directions (we assume that
	   ;;  lrefs and absrefs are disjoint, so we can use the
	   ;;  same table)
	   (store! urlmap absref lref)
	   (store! urlmap (vector lref) absref)
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
(define extprefix #((isalpha) (isalpha) (isalpha+) ":"))

(define (dom/localize! dom base saveto read (options #f)
		       (urlmap) (doanchors) (syncrels) (stylerules))
  (default! urlmap (getopt options 'urlmap (make-hashtable)))
  (default! doanchors (getopt options 'doanchors #f))
  (default! syncrels (getopt options 'synclinks (config 'synclinks synclinks)))
  (default! stylerules (getopt options 'stylerules {}))
  (loginfo |DOM/LOCALIZE!|
    "Localizing references for "
    (try (gp/string (get dom 'source)) "source")
    "\n\tfrom " (write (gp/string base))
    "\n\tto " (write read) ", copying content to "
    (if (singleton? saveto) (write (gp/string saveto))
	(do-choices saveto
	  (printout "\n\t\t" (write (gp/string saveto))))))
  (debug%watch "DOM/LOCALIZE!" 
    "DOM" (dom/nodeid dom) base saveto read doanchors)
  (let ((head (dom/find dom "HEAD" #f))
	(saveslot (getopt options 'saveslot)))
    (loginfo |Localize| "Localizing [src] elements")
    (dolist (node (dom/select->list dom "[src]"))
      (loginfo |Localize| "Localizing " (dom/sig node)
	       "\n\tfrom " (gp/string base) "\n\tto "
	       (gp/string saveto))
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
		       (do-choices saveto (printout "\n\t\t" (gp/string saveto))))
	      (loginfo |Localize|
		       "Localized " (write (get node 'src))
		       "\n\tto " (write ref) " for " (dom/sig node #t) " saved in"
		       (do-choices saveto (printout "\n\t\t" (gp/string saveto)))))
	  (when saveslot (dom/set! node saveslot (get node 'src)))
	  (unless (test node 'data-origin) (dom/set! node 'data-origin (get node 'src)))
	  (dom/set! node 'src ref)
	  (logdebug |Localize| "New converted node: \n"  (pprint node)))))
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
		     (printout "\n\twith CSS rules " 
		       (pprint stylerules))))
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
	    (unless (equal? href ref)
	      (loginfo |Localize|
		"Localized stylesheet " (write href) " to " (write ref)
		" for " (dom/sig node #t)))))
	(when (and (exists? ref) ref (not (equal? href ref)))
	  (when saveslot (dom/set! node saveslot href))
	  (dom/set! node 'href ref)
	  (logdebug |Localize/css|
	    "New converted node: \n" (pprint node)))))
    (let ((hrefs (dom/select->list dom "[href]")) 
	  (ignored '())
	  (anchors '())
	  (others '()))
      (dolist (node hrefs)
	(if (or (and doanchors (test node '%xmltag 'a))
		(and (test node '%xmltag 'link)
		     (test node 'rel syncrels))
		(not (test node '%xmltag '{a link})))
	    (if (not (or (string-starts-with? (get node 'href) extprefix)
			 ;; We handle these separately
			 (test node 'rel {"x-resource" "stylesheet"})))
		(if (or (immediate? doanchors)
			(exists textsearch (reject doanchors regex?) (get node 'href))
			(exists regex/search (pick doanchors regex?) (get node 'href)))
		    (set! anchors (cons node anchors))
		    (set! ignored (cons node ignored)))
		(set! ignored (cons node ignored)))
	    (set! ignored (cons node ignored))))
      (unless (null? hrefs)
	(loginfo |Localize| "Split " (length hrefs) " [HREF]s into "
		 (length anchors) " anchors and " (length others) " other elements"
		 (unless (null? ignored) (printout " (" (length ignored) " ignored)"))))
      (unless (null? anchors)
	(loginfo |Localize|
	  "Localizing " (length anchors) " anchor [href] elements"))
      (dolist (node (reverse anchors))
	(loginfo |Localize|
	  "Localizing (anchor) " (get node 'href) "\n\tfrom " base "\n\tto " saveto)
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
	    (dom/set! node 'href ref)))
	(logdebug |Localize/anchor| "New converted node: \n"  (pprint node)))
      (unless (null? others)
	(loginfo |Localize|
	  "Localizing " (length others) " non-anchor [href] elements"))
      (dolist (node (reverse others))
	(loginfo |Localize|
	  "Localizing (" (get node '%xmltag) 
	  (when (test node 'rel) (printout "/" (get node 'rel))) ") " 
	  (get node 'href) "\n\tfrom " base "\n\tto " saveto)
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
			       href))))
	  (logdebug |Localize/href| "New converted node: \n"  (pprint node))))
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
		(append (get head '%content) xresources))))))

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
(define srcset-url-pat
  #((subst {"," (bos)} "") (subst (spaces*) "")
    (not> {(eos) "," (spaces)})))

(define (dom/getmanifest doc (refroot #f)
			 (syncrels (config 'synclinks synclinks))
			 (syncrefs #t))
  (let* ((links (if (overlaps? syncrels #t)
		    (pick (dom/find doc "LINK") 'href)
		    (pick (pick (dom/find doc "LINK") 'href) 'rel syncrels)))
	 (csslinks (pick links 'rel "stylesheet" 'type "text/css")))
    (choice (get links 'href)
	    (get (dom/find doc "SCRIPT") 'src)
	    (get (dom/find doc "IMG") 'src)
	    (gathersubst srcset-url-pat (get (dom/find doc "IMG") 'srcset))
	    ;; From SVG
	    (get (dom/find doc "IMAGE") 'href)
	    ;; From HTML5
	    (gathersubst srcset-url-pat
			 (get (dom/find doc "SOURCE") 'srcset))
	    (tryif refroot
	      (for-choices (ref csslinks)
		(let* ((fullpath (gp/mkpath refroot ref))
		       (content (and (gp/exists? fullpath)
				     (gp/fetch fullpath))))
		  (tryif content (dom/getcssurls content))))))))

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



