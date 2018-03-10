;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'condense)

(define (asis? object)
  (or (not (cons? object))
      (number? object) (symbol? object)
      (and (string? object) (< (length object) 200))
      (and (packet? object) (<= (length object) 64))))

(define (condense? object)
  (or (ambiguous? object) (table? object) (vector? object) 
      (and (compound? object) (not (opaque-compound? object)))
      (and (string? object) (> (length object) 200))
      (and (packet? object) (> (length object) 64))))

(defambda (count-refs object refcounts)
  (unless (asis? object)
    (let ((ptr (hashptr object)))
      (hashtable-increment! refcounts ptr)
      (cond ((ambiguous? object)
	     (do-choices (elt object) (when (cons? elt) (count-refs elt refcounts))))
	    ((vector? object)
	     (doseq (elt object) (when (cons? elt) (count-refs elt refcounts))))
	    ((pair? object)
	     (let ((scan object))
	       (while (pair? scan)
		 (count-refs (car scan) refcounts)
		 (set! scan (cdr scan)))
	       (when (cons? scan) (count-refs scan refcounts))))
	    ((and (compound? object) (not (compound-opaque? object)))
	     (dotimes (i (compound-length object))
	       (when (cons? (compound-ref object i))
		 (count-refs (compound-ref object i) refcounts))))
	    (else)))))

(defambda (add-condensed object root uuids condensed (uuid (getuuid)))
  (store! object uuid)
  (store! root uuid condensed)
  uuid)

(defambda (dumper object refcounts root uuids)
  (cond ((asis? object) object)
	((ambiguous? object)
	 (let* ((ptr (hashptr object))
		(refcount (get refcounts ptr)))
	   (if (and (= refcount 1) (< (choice-size object) 17))
	       (for-choices (elt object) (dumper elt refcounts root uuids))
	       (for-choices (elt object)
		 (if (asis? elt)
		     elt
		     (let* ((elt-ptr (hashptr object))
			    (elt-refcount (get refcounts elt-ptr)))
		       (if (and (exists? elt-refcount) (> elt-refcount refcount))
			   (add-condensed object root uuids
					  (for-choices (elt object) (dumper elt refcounts root uuids)))
			   (dumper elt refcounts root uuids))))))))
	((pool? object)
	 (make-compound 'pool
			(pool-source object) 
			(try (poolctl object 'metadata 'opts) #f)))
	((index? object)
	 (make-compound 'index
			(index-source object) 
			(try (indexctl object 'metadata 'opts) #f)))
	((test uuids object) (get uuids object))
	((test uuids (hashptr object)) (get uuids (hashptr object)))
	((vector? object)
	 (let* ((ptr (hashptr object))
		(refcount (get refcounts ptr)))
	   (if (and (= refcount 1) (< (length object) 17))
	       (forseq (elt object) (dumper elt refcounts root uuids))
	       (forseq (elt object)
		 (if (asis? elt)
		     elt
		     (let* ((elt-ptr (hashptr object))
			    (elt-refcount (get refcounts elt-ptr)))
		       (if (and (exists? elt-refcount) (> elt-refcount refcount))
			   (add-condensed object root uuids
					  (forseq (elt object) (dumper elt refcounts root uuids)))
			   (dumper elt refcounts root uuids))))))))
	((compound? object)
	 (let* ((ptr (hashptr object))
		(refcount (get refcounts ptr))
		(tag (compound-tag object))
		(len (compound-length object))
		(components (make-vector len #f))
		(uuid #f))
	   (when (or (> refcount 1) (> len 7))
	     (set! uuid (getuuid))
	     (store! uuids {ptr object} uuid))
	   (dotimes (i len)
	     (vector-set! components i
			  (if (asis? (record-ref object i)) (record-ref object i)
			      (vector-set! components i )
			      (if (test uuids {object ptr})
				  (try) (get uuids {object ptr})))))
	   
	   (let* ((elt-ptr (hashptr object))
		  (elt-refcount (get refcounts elt-ptr)))
	     (if (and (exists? elt-refcount) (> elt-refcount refcount))
		 (add-condensed object root uuids
				(forseq (elt object) (dumper elt refcounts root uuids)))
		 (dumper elt refcounts root uuids)))
	   (if (and (= refcount 1) (< (length object) 7))
	       (let ((vec)))
	       (vector->compound )
	       (forseq (elt object) (dumper elt refcounts root uuids))
	       (forseq (elt object)
		 (if (asis? elt)
		     elt
		     (let* ((elt-ptr (hashptr object))
			    (elt-refcount (get refcounts elt-ptr)))
		       (if (and (exists? elt-refcount) (> elt-refcount refcount))
			   (add-condensed object root uuids
					  (forseq (elt object) (dumper elt refcounts root uuids)))
			   (dumper elt refcounts root uuids))))))))
	((pair? object))
	((table? object))
	((exists > (get refcounts (hashptr object)) 1)
	 (add-condensed object root uuids object))
	(else object)))

(define (dumptable table)
  (let ((refcounts (make-hashtable))
	(condensed (make-hashtable)))
    (count-refs table refcounts)
    ))

