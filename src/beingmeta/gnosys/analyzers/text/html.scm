(in-module 'gnosys/analyzers/text/html)
(use-module 'texttools)
(use-module '{gnosys gnosys/docdb gnosys/urldb gnosys/analyze})
(use-module '{gnosys/metakeys gnosys/metakeys/disambiguate})

(module-export! '{analyzer htmlanalyzer})

;;; Extracting anchors and images

(define anchor-pattern
  '(IC #((label HEAD #("<a" (isspace) (not> ">") ">"))
	 (label CONTENT (not> "</a"))
	 "</a" (spaces*) ">")))
(define img-pattern
  '(IC #("<img" (isspace) (not> ">") ">")))
(define img-src-pattern
  '(IC  #((isspace) "src="
	  {#("'" (label src (not> "'")) "'")
	   #("\"" (label src (not> "\"")) "\"")}
	  {(isspace) ">"})))
(define img-alt-pattern
  '(IC  #((isspace) "alt="
	  {#("'" (label alt (not> "'")) "'")
	   #("\"" (label alt (not> "\"")) "\"")}
	  {(isspace) ">"})))

(define (rawtext string)
  (stringout (doseq (word (getwords string) i)
	       (printout (if (> i 0) " ") word))))

(define (extract-html-links doc text (index #f))
  (let ((anchor-fragments (text->frames anchor-pattern text))
	(img-fragments (gather img-pattern text))
	(base (try (get doc 'base) (get doc 'loc) #f)))
    (do-choices (anchor anchor-fragments)
      (let* ((href
	      (get (text->frames href-pattern (get anchor 'head)) 'href))
	     (text (get anchor 'content))
	     (realtext (stdspace
			(if (textsearch '(IC "<img") text)
			    (try (get (text->frames img-alt-pattern text) 'alt) text)
			    (rawtext text))))
	     (target (url2frame href base)))
	(add! doc @?www/anchors (list target (try realtext text) text))
	(when index (index-frame index doc @?www/anchors target))))
    (do-choices (img img-fragments)
      (let* ((src (get (text->frames img-src-pattern img) 'src))
	     (alt (get (text->frames img-alt-pattern img) 'alt))
	     (target (url2frame src base)))
	(add! doc @?www/images (list target (try alt "")))
	(when index (index-frame index doc @?www/images target))))))

;;; Extracting markup fragments

(define (make-fragment-pattern tag)
  (vector "<" `(ic ,tag) `(not> ">") ">"
	  `(label content (not> "<"))
	  "</" `(ic ,tag) ">"))

(define markup-fragment
  (make-fragment-pattern {"strong" "em" "b" "i" "defn" "q" "tt" "var"}))

(define (make-quote-fragment start (end #f))
  (let ((end (or end start)))
    `#(,start (label content (not> ,end)) ,end)))

(define quote-fragment
  (choice (make-quote-fragment {"\"" "'"})
	  (make-quote-fragment {"``" "''"})))

(define (extract-textlets doc text (index #f))
  (let ((bites (get (text->frames markup-fragment text) 'content)))
    (add! doc @?gn/text-bites bites)
    (when index
      (index-frame index doc @?gn/text-bites bites)))
  (let ((quotes (get (text->frames quote-fragment text) 'content)))
    (add! doc @?gn/quotes quotes)
    (when index
      (index-frame index doc @?gn/quotes quotes))))

;;; Analyzing HEAD content

(define title-pattern
  '(IC #("<title>" (label title (not> "</title>")) "</title>")))
(define meta-pattern
  '(IC #("<meta"
	 {
	  #((spaces) "NAME=" {#("'" (label name (not> "'") #t) "'")
			      #("\"" (label name (not> "\"") #t) "\"")}
	    (spaces) "CONTENT="
	    {#("'" (label content (not> "'") #t) "'")
	     #("\"" (label content (not> "\"")) "\"")})})))
(define link-pattern
  '(IC #("<link" (not> ">") ">")))
(define base-pattern
  '(IC #("<base" (not> ">") ">")))

(define href-pattern
  '(IC  #((isspace) "href="
	  {#("'" (label href (not> "'")) "'")
	   #("\"" (label href (not> "\"")) "\"")}
	  {(isspace) ">"})))
(define rel-pattern
  '(IC  #((isspace) "rel="
	  {#("'" (label rel (not> "'") #t) "'")
	   #("\"" (label rel (not> "\"") #t) "\"")
	   (label rel (greedy (isalpha+)) #t)}
	  {(isspace) ">"})))

(define (process-head doc head)
  (add! doc 'title (get (text->frames title-pattern head) 'title))
  (add! doc 'base (get (text->frames href-pattern (gather base-pattern head))
		       'href))
  (do-choices (link (gather link-pattern head))
    (add! doc 'links link)
    (let ((rel (get (text->frames rel-pattern link) 'rel))
	  (href (get (text->frames href-pattern link) 'href)))
      (when (and (exists? rel) (exists? href))
	(add! doc rel
	      (url2frame href (try (get doc 'base) (get doc 'loc) #f))))))
  (do-choices (meta-tag (text->frames meta-pattern head))
    (when (test meta-tag 'name 'keywords)
      (let* ((keystring (get meta-tag 'content))
	     (keywords (stdspace (elts (segment keystring ",")))))
	(add! doc @?gn/keywords keywords)))
    (add! doc (get meta-tag 'name) (get meta-tag 'content))))

;;; The core analyzer

;;; For slicing the body into paragraphs
(define paragraph-breaks
  #("<" (IC {"p" #("h" (isdigit)) "li"
	     "dd" "td" "div" "blockquote"
	     "pre" "script" "style" "script" "object"
	     #("br" (spaces*) (opt "/"))})
    {(isspace) ">"}))

(define (htmlanalyzer doc)
  (let* ((content (get doc 'content))
	 (head-pos (textsearch '(IC "<head>") content))
	 (body-pos (textsearch '(IC "<body") content (or head-pos 0)))
	 (index (or passageindex docindex)))
    (let ((preamble (if head-pos (subseq content 0 head-pos) ""))
	  (head (if head-pos (subseq content head-pos body-pos) ""))
	  (body (subseq content (or body-pos 0))))
      (add! doc 'preamble preamble)
      (add! doc 'head head)
      (process-head doc head)
      (extract-html-links doc body docindex)
      (extract-textlets doc body docindex)
      (let* ((heading-done (elapsed-time))
	     (textslices (textslice body paragraph-breaks))
	     (description
	      (makepassage (get doc 'description)
			   (stringout "DESCRIPTION "
			     (try (get doc 'title) (get doc @?doc/source)))
			   doc))
	     (title
	      (makepassage (get doc 'title)
			   (stringout "TITLE " (get doc @?doc/source))
			   doc))
	     (slices (map maybepassage textslices))
	     (passages (pick (elts slices) oid?))
	     (slicing-done (elapsed-time)))
	;; Handle the parsed description
	(when (exists? description)
	  (add! doc @?doc/description description)
	  (add! doc @?doc/passages description))
	;; Handle the parsed title
	(when (exists? description)
	  (add! doc @?doc/title description)
	  (add! doc @?doc/passages description))
	;; Handle the body content
	(add! doc @?doc/slices slices)
	(add! doc @?doc/passages passages)
	(add! passages @?gn/parent doc)
	(index-frame docindex passages @?gn/parent doc)
	;; Parse the body content
	(doseq (slice slices)
	  (when (oid? slice)
	    (extract-html-links slice (get slice 'text) index)))
	doc))))
(define analyzer htmlanalyzer)

(define skip-pattern
  '(IC {"<pre" "<script" "<style" "<object"}))

(define (get-passage-id string words)
  (stringout (do ((scan words (cdr scan))
		  (charcount 0 (+ (length (car scan)) charcount)))
		 ((or (null? scan) (> charcount 45)) (xmlout))
	       (if (> charcount 0) (printout " "))
	       (printout (car scan)))))

(define (heading? string)
  (or (has-prefix string "<h") (has-prefix string "<H")
      (search "heading" string 0 (position #\> string))
      (search "title" string 0 (position #\> string))))

(define (maybepassage string)
  (if (or (exists? (textmatcher skip-pattern string))
	  (> (markup% string) 25))
      string
      (let ((words (getwords string)))
	(if (< (length words) 4) string
	    (let* ((id (get-passage-id string words))
		   (passage (makepassage string id)))
	      (when (heading? string)
		(add! passage 'type 'heading))
	      passage)))))


