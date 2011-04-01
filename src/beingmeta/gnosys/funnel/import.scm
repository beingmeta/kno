(in-module 'gnosys/funnel/import)

(use-module '{fdweb texttools brico/indexing morph/en})
(use-module '{logger})

(module-export! '{import-index-entries book-pool book-index})

(define %loglevel %info!)

(define book-pool (use-pool (config 'BOOKSRC)))
(define book-index (use-index (config 'BOOKSRC)))

(define created {})

(define section-order
  #("C" "A" "S" "W" "G" "E" "P" "M" "R" "MLA" "APA" "CMS" "B"))

(define *termfixrules* '())

(defslambda (config-termfixrules var (val))
  (cond ((bound? val)
	 (if (and (exists? val) val (not (null? val)))
	     (unless (position val *termfixrules*)
	       (set! *termfixrules* (append *termfixrules* (list val))))
	     (set! *termfixrules* '())))
	(else *termfixrules*)))
(config-def! 'termfixrules config-termfixrules)

(define *termextractrules* {})

(define (config-termextractrules var (val))
  (cond ((bound? val)
	 (if (and (exists? val) val (not (null? val)))
	     (set+! *termextractrules* val)
	     (set! *termextractrules* {})))
	(else *termextractrules*)))
(config-def! 'termextractrules config-termextractrules)

(define *keep-capitalized* {})

(define (config-termkeepcaps var (val))
  (cond ((bound? val)
	 (if (and (exists? val) val (not (null? val)))
	     (set+! *keep-capitalized* val)
	     (set! *keep-capitalized* {})))
	(else *keep-capitalized*)))
(config-def! 'termkeepcaps config-termkeepcaps)

;; Converting their syntax into XHTML
(define *htmlize-rules* {})
(define (config-htmlize var (val))
  (cond ((bound? val)
	 (if (and (exists? val) val (not (null? val)))
	     (set+! *htmlize-rules* val)
	     (set! *htmlize-rules* {})))
	(else *htmlize-rules*)))
(config-def! 'htmlize config-htmlize)

;;; Utility functions

;; Changes 'quotation, defining' into 'defining quotation'
(define (commaflip string)
  (let ((pos (position #\, string (or (position #\) string) 0)
		       (or (position #\' string) #f))))
    (if (and pos (not (eqv? (elt string 0) #\')))
	(if (and (> (length (words->vector (subseq string (1+ pos)))) 0)
		 (overlaps? (first (words->vector (subseq string (1+ pos))))
			    prepositions))
	    (stdspace (string-append (subseq string 0 pos) " "
				     (subseq string (1+ pos))))
	    (stdspace (string-append (subseq string (1+ pos)) " "
				     (subseq string 0 pos))))
	string)))

(define (fix-misquotes string)
  (if (has-prefix string "'")
      (if (position #\' string)
	  string
	  (append string "'"))
      (if (has-suffix string "'")
	  (if (position #\' string)
	      string
	      (append "'" string))
	  (if (textsearch #("'" (spaces*) "'") string)
	      (string-append "'" string "'")))))

;;;; Basic data structures

;;; KEYs represent nodes in the index hierarchies
;;; REFs indicate references into the text

;;; Note that these functions are not threadsafe, so don't use them
;;;  in two threads at the same time.
(define (intern-key key (id #f))
  (try (tryif id (find-frames book-index 'indexpath key 'id id))
       (tryif id (find-frames book-index 'id id))
       (find-frames book-index 'indexpath key)
       (let ((f (frame-create book-pool
		  '%id key 'indexpath key 'id (tryif id id)
		  'type 'indexentry '%created (timestamp))))
	 (message "Creating new entry"
		  (if id (printout " #" id))
		  " for " key)
	 (set+! created f)
	 (unless (null? (cdr key))
	   (add! f 'super (intern-key (cdr key))))
	 (index-frame book-index f '{indexpath super type id})
	 (index-frame book-index f 'super* (get (get f 'super) 'super))
	 (when (null? (cdr key))
	   (index-frame book-index f 'super #f))
	 f)))

(define ref-pattern
  '(GREEDY #((opt #((label section (isalpha+)) ":" (spaces*)))
	     (opt "(") (label start (isdigit+))
	     (opt #("-" (label end (isdigit+))))
	     (opt ")"))))

(define (fixdash text) (string-subst text "\u2013" "-"))
(define (unfixdash text) (string-subst text "-" "\u2013"))

(define (strip-punct s)
  (let ((end (textsearch #((+ {"." "," ";"}) (eol)) s)))
    (if end (subseq s 0 end) s)))

(define (intern-ref link-data text html)
  (let* ((text (strip-punct text))
	 (match (text->frame ref-pattern (fixdash text)))
	 (section (get match 'section))
	 (start (string->number (get match 'start)))
	 (end (try (string->number (get match 'end)) start)))
    (when (and (exists? start) (< end start))
      (set! end (+ end (* 100 (quotient start 100)))))
    (try (if (exists? start)
	     (find-frames book-index
	       'type 'indexref 'link-data link-data 'start start 'end end)
	     (find-frames book-index
	       'type 'indexref 'link-data link-data))
	 (let ((colon (position #\: text))
	       (new (frame-create book-pool
		      'type 'indexref 'link-data link-data
		      '%id (stringout text "/" link-data)
		      'ref text 'html (strip-punct html)
		      'start start 'end end 'section section)))
	   (message "Creating new indexref: " new)
	   (index-frame book-index new '{type ref link-data})
	   (if (exists? section)
	       (store! new 'sectnum (position section section-order))
	       (index-frame book-index new 'section #f))
	   (store! new 'end end)
	   (when (exists? start)
	     (dotimes (i (- (1+ end) start))
	       (let ((pageref (intern-page (try section #f) (+ start i))))
		 (add! new 'pages pageref)
		 (add!  pageref 'inref new))))
	   (index-frame book-index new
	     '{sectnum section start end pages})
	   new))))

(define (intern-page section pagenum)
  (try (find-frames 'page pagenum 'section section)
       (let ((f (frame-create book-pool
		  '%id (if section (stringout section ":" pagenum)
			   (stringout "Page" pagenum))
		  'type 'pageref 'page pagenum 'section section)))
	 (message "Created new pageref: " f)
	 (index-frame book-index f '{type page section})
	 f)))

(define (setup-pages ref)
  (let* ((section (try (get ref 'section) #f))
	 (start (get ref 'start))
	 (end (try (get ref 'end) start)))
    (dotimes (i (- (1+ end) start))
      (let ((pageref (intern-page (try section #f) (+ start i))))
	(add! ref 'pages pageref)
	(add!  pageref 'inref ref)))))

(define (index-page-info new text)
  (let* ((match (text->frame ref-pattern (fixdash text)))
	 (section (get match 'section))
	 (start (string->number (get match 'start)))
	 (endval (try (string->number (get match 'end)) start))
	 (end (if (>= endval start) endval
		  (+ endval (* 100 (quotient start 100))))))
    (store! new 'section section)
    (store! new 'start start)
    (store! new 'end end)
    (unless (exists? section)
      (index-frame book-index new 'section #f))
    (dotimes (i (- (1+ end) start))
      (add! new 'pages (+ start i)))
    (index-frame book-index new '{section start end pages})))

(define (fix-page-info ref)
  (let* ((start (get ref 'start))
	 (end (try (string->number (get (text->frame ref-pattern (fixdash (get ref 'ref))) 'end))
		   (get ref 'end)
		   start))
	 (section (get ref 'section)))
    (when (exists? end)
      (when (< end start)
	(set! end (+ end (* 100 (quotient start 100))))
	(store! ref 'end end))
      (dotimes (i (- (1+ end) start))
	(add! ref 'pages (intern-page (try section #f) (+ start i)))))))

;;; Converting the index text in various ways

;; For stripping trailing punctuation and removing markup
;;  from entries
(define (normalize-entry e (rules *termfixrules*))
  (do ((rules rules (cdr rules))
       (s (decode-entities
	   (cond ((has-suffix e ",") (subseq e 0 -1))
		 ((has-suffix e ".") (subseq e 0 -1))
		 (else e)))
	  (textsubst s (qc (car rules)))))
      ((null? rules) s)))

(define (htmlize-markup string)
  (decode-entities (textsubst string (qc *htmlize-rules*))))

(define frag-pattern
  (qc #("'" (not> "'") "'")
      #((isalpha) (* {(isalpha) "'" #("&" (not> ";") ";")})
	{(isalpha) "-"})))

;;; Fragments and phrases

(define (getfrags string)
  (choice (gather frag-pattern string)
	  (gather '(isalpha+) string)
	  (elts (words->vector string))))

(define (getphrases term)
  (let* ((wvec (words->vector term))
	 (frags (vector->frags wvec 3 #f)))
    (reject (seq->phrase frags) stop-words)))

;;; This is a separate function because we might want to call it after additional processing

(define (index-phrases entry (index book-index))
  (index-string index entry 'phrases (getphrases (get entry 'term)))
  (index-string index entry 'phrases (porter-stem (getphrases (get entry 'term))))
  (index-string index entry 'phrases* (getphrases (get entry 'terms)))
  (index-string index entry 'phrases* (porter-stem (getphrases (get entry 'terms)))))

(module-export! '{getphrases index-phrases fixindexcase})

;;; Importing index refs

(define (save-indexentry-data f)
  ;; Save the old information, just in case
  ;; (add! f '%oldlinks (get f 'links))
  ;; (add! f '%oldrefs (get f 'refs))
  (do-choices (ref (pick (get f 'links) oid?))
    ;; (add! ref '%oldentries f)
    (drop! ref 'entries f))
  (drop! f '{links refs allrefs see see_also crossrefs}))

(define (fixindexcase string)
  (if (capitalized? string)
      (if (or (exists? (textmatcher #((isupper) (isupper+) {(eol) (ispunct) (spaces)}) string))
	      (exists? (textmatcher (vector (qc *keep-capitalized*)  '{(eol) (ispunct) (spaces)}) string)))
	  string
	  (string-append (downcase (subseq string 0 1)) (subseq string 1)))
      string))

;;; This is the main loop

;;; This starts with a root entry and imports it into the knowledge base;
;;; Root entries are generally defined for each letter as extracted (above)
;;;  into ixletters.
(define (import-index-entries entry (root '()))
  (if (test entry 'index.term)
      (let* ((rawterm (xmlcontent entry 'index.term))
	     (base (if (null? root)
		       (normalize-entry (fixindexcase rawterm))
		       (normalize-entry rawterm)))
	     (matches (text->frames *termextractrules* rawterm))
	     (path (cons (commaflip base) root))
	     (terms (elts path)))
	(%debug "Importing index entry " (write rawterm) " within " root)
	(let ((f (intern-key path (get entry 'id))))
	  (%debug "Imported " (write rawterm) " within " root " to " f)
	  (unless (identical? path (get f 'indexpath))
	    (logwarn "Changed indexpath of " (get entry 'id)
		     " from " (get f 'indexpath)
		     " to " path)
	    (store! f 'indexpath path)
	    (index-frame book-index f 'indexpath path)
	    (store! f '%id path))
	  (do-choices (slotid (getkeys matches))
	    (add! f slotid (normalize-entry (get matches slotid))))
	  (store! f '%updated (timestamp))
	  (store! f 'term (commaflip base))
	  (store! f 'terms terms)
	  (store! f 'id (get entry 'id))
	  (store! f 'rawterm rawterm)
	  (store! f 'frags
		  (choice (gather frag-pattern (elts path))
			  (elts (words->vector (elts path)))))
	  (index-frame book-index f '{rawterm id literals})
	  (do-choices (slotid '{term terms frags})
	    (index-string book-index f slotid))
	  (save-indexentry-data f)
	  ;; Now go over the links
	  (do-choices (link (get entry '{index.link index.link_dctm}))
	    (let* ((linkstring (xmlcontent link))
		   (linktext (strip-punct (strip-markup linkstring)))
		   (linkmarkup (strip-punct (htmlize-markup linkstring)))
		   (linkdata (tryif (table? link) (get link 'link-data)))
		   (roid (tryif (table? link) (get link 'roid))))
	      (add! f 'links (vector linktext linkmarkup (get entry 'id)))
	      (unless (equal? linktext "")
		(let ((ref (intern-ref linkdata linktext linkmarkup)))
		  ;; These should be redundant w/rt linkmarkup
		  ;; (store! ref 'html linkmarkup)
		  ;; (store! ref 'linkdata linkdata)
		  (when (exists? roid)
		    (store! ref 'roid (get link 'roid))
		    (index-frame book-index ref 'roid (get link 'roid))
		    (index-frame book-index ref 'has 'roid))
		  (add! f 'refs ref)
		  (add! ref 'entries f)))))
	  ;; Now go over the refs, which are intra-index links
	  (do-choices (ref (get entry 'index.ref))
	    (let ((reftype (get ref 'type))
		  (entries (get ref '{index.ref.entry index.ref.entry_dctm})))
	      (do-choices (entry entries)
		(let* ((linkstring (xmlcontent entry))
		       (linktext (strip-punct (strip-markup linkstring)))
		       (linkmarkup (strip-punct (htmlize-markup linkstring)))
		       (linkdata (get entry 'link-data))
		       (refvec
			(cons reftype
			      (vector
			       (try (find-frames book-index 'id linkdata) #f)
			       linktext linkmarkup linkdata))))
		  (add! f 'crossrefs refvec)
		  (add! f (choice 'indexrefs (string->lisp reftype))
			(find-frames book-index 'id linkdata))
		  (index-frame book-index f 'has '{refs allrefs})
		  (index-frame book-index f 'has (string->lisp reftype))))))
	  (index-frame book-index f 'refs)
	  (index-frame book-index (get f 'refs) 'entries f))
	(import-index-entries (get entry 'index.entry) path)
	(import-index-entries (get entry 'index.entry) path))
      (import-index-entries (get entry 'index.entry) root)))

(define reprocess-start #f)

(define (already-processed? f)
  (or (and reprocess-start (time-later? reprocess-start)) 
      (begin (unless reprocess-start (set! reprocess-start (timestamp)))
	     #f)))
(define (reprocess-entry f (index book-index))
  (if (already-processed? f)
      (get f 'terms)
      (let* ((rawterm (get f 'rawterm))
	     (term (commaflip (if (test f 'super)
				  (normalize-entry rawterm)
				  (normalize-entry (fixindexcase rawterm)))))
	     (super-terms (reprocess-entry (get f 'super) index))
	     (terms (choice term super-terms)))
	(if (test f 'super)
	    (%debug "Reprocessing index entry " (write rawterm) " within " (get f 'super))
	    (%debug "Reprocessing root index entry " (write rawterm)))
	(do ((scan f (get scan 'super))
	     (path '()
		   (cons (normalize-entry (get scan 'rawterm)) path)))
	    ((fail? scan)
	     (store! f '%id (reverse path))))
	(store! f '%updated (timestamp))
	(store! f 'term term)
	(store! f 'terms terms)
	(store! f 'frags
		(choice (gather frag-pattern terms)
			(elts (words->vector terms))))
	(index-frame index f '{type rawterm term terms frags id super})
	(index-frame index f 'has (getkeys f))
	(%debug "Done reprocessing index entry" f)
	terms)))

(define (reindex-entry index entry)
  (if (empty? (getkeys entry))
      (index-frame index entry 'status 'deleted)
      (begin
	(index-frame index entry 'has (getkeys entry))
	(cond ((test entry 'type 'indexentry)
	       (index-frame index entry
		 '{type indexpath super 
			literals frags refs rawtext rawterm indexpath
			indexid see see_also indexrefs id})
	       (index-frame index entry 'frags
			    (porter-stem (get entry 'frags)))
	       (index-string index entry '{term terms frags})
	       (index-phrases entry index)
	       (when (fail? (get entry 'super))
		 (index-frame index entry 'super #f))
	       (index-frame index entry 'super* (get* entry 'super))
	       (index-frame index entry 'refs)
	       (index-frame index (get entry 'refs) 'entries entry))
	      ((test entry 'type 'indexref)
	       (index-frame index entry
		 '{type entries ref start end pages sect section sectnum
			id roid link-data}))
	      ((test entry 'type 'pageref)
	       (index-frame index entry '{type section page}))
	      (else (logwarn "Weird entry: " entry))))))


(module-export! '{reprocess-entry reindex-entry})
