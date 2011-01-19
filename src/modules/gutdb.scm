;;; -*- Mode: scheme; -*-

(in-module 'gutdb)

(use-module '{texttools fdweb logger domutils domutils/index})

;;(define %loglevel %debug!)
(define %loglevel %notify!)

(module-export! '{gutdb gutdb/rdf})

(define (gutdb/rdf id)
  (let* ((url (if (number? id)
		  (stringout "http://www.gutenberg.org/ebooks/" id ".rdf")
		  (if (string? id) id #f)))
	 (response (and url (urlget url)))
	 (seen {}))
    (when response
      (while (and (test response 'location)
		  (not (overlaps? (get response 'location) seen)))
	(debug%watch "GUTDB/RDF redirect" url)
	(set+! seen (get response 'location))
	(set! url  (get response 'location))
	(set! response (urlget (get response 'location))))
      (debug%watch "GUTDB/RDF fetched" url))
    (and response (xmlparse (get response '%content)))))

(define (gutdb arg)
  (if (or (string? arg) (number? arg))
      (handle-rdf (gutdb/rdf arg))
      (and (table? arg) (handle-rdf arg))))

(define (nodetext n) (decode-entities (xmlcontent n)))
(define (subjtext n (slotid #f))
  (let ((text (decode-entities
	       (if slotid (get (get n slotid) 'value)
		   (difference (xmlcontent n) "")))))
    (tryif (exists? text)
      (if (textsearch #((spaces) "--" (spaces)) text)
	  (textslice text #((spaces) "--" (spaces)) #f)
	  text))))
(define (rightstext n)
  (if (string? n) (decode-entities n)
      (if (test n 'resource) (get n 'resource)
	  (decode-entities (xmlcontent n)))))
(define (getvals x slot)
  (choice (reject (get x slot) 'bag)
	  (get (get (get x slot) 'bag) 'li)))
(define (getresource x) (get x 'resource))
(define (handle-rdf rdf)
  (let ((info (frame-create #f))
	(index (make-hashtable)))
    (dom/index! index rdf)
    (do-choices (field '{title rights publisher})
      (add! info field (nodetext (xmlget rdf field))))
    (do-choices (field '{(creator . creator)
			 (trl . translator)
			 (ctb . contributor)})
      (let* ((value (xmlget rdf (car field)))
	     (about (get value 'resource))
	     (ref (find-frames index 'about about))
	     (sum (frame-create #f)))
	(when (exists? value)
	  (do-choices (slotid '{name webpage alias birthdate deathdate})
	    (when (exists? (xmlget ref slotid))
	      (store! sum slotid (difference ({xmlcontent getresource} (xmlget ref slotid)) ""))))
	  (store! info (cdr field) sum))))
    info))




