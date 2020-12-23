;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'condense)

(use-module '{kno/reflect binio binio varconfig logger})

(define %loglevel %debug%)

(module-export! '{condense vaporize condense/expand})

(defambda (asis? object)
  (if (cons? object)
      (and (not (ambiguous? object))
	   (or (and (string? object) (< (length object) 200))
	       (and (packet? object) (<= (length object) 64))
	       (uuid? object) (timestamp? object)
	       (number? object)))
      (or (not (immediate? object))
	  (symbol? object) (fail? object) (not object) 
	  (true? object) (void? object) (default? object))))

(defambda (count-refs object refcounts)
  (when (and (bound? object) (cons? object))
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
	    ((module? object))
	    ((table? object)
	     (do-choices (key (getkeys object))
	       (unless (or (not (bound? key)) (void? key))
		 (when (cons? key) (count-refs key refcounts))
		 (count-refs (get object key) refcounts))))
	    ((exception? object)
	     (count-refs (exception-context object) refcounts)
	     (when (exception-irritant? object)
	       (count-refs (exception-irritant object) refcounts))
	     (count-refs (exception-stack object) refcounts))
	    (else)))))

(defambda (add-condensed object root uuids condensed (ptr) (uuid (getuuid)))
  (cond ((asis? object) object)
	(else
	 (default! ptr (hashptr object))
	 (unless uuid (set! uuid (try (get uuids ptr) (getuuid))))
	 (store! uuids ptr uuid)
	 (store! root uuid condensed)
	 uuid)))

(defambda (dump-elt object container refcounts root uuids (cptr) (optr) (uuid))
  (cond ((asis? object) object)
	(else
	 (default! optr (hashptr object))
	 (default! cptr (hashptr container))
	 (default! uuid (and (> (get refcounts optr) (get refcounts cptr))
			     (getuuid)))
	 (cond ((test uuids optr) (get uuids optr))
	       ((not uuid) (dumper object refcounts root uuids))
	       (else
		(store! uuids optr uuid)
		(let ((v (dumper object refcounts root uuids uuid)))
		  (unless (and (equal? v uuid) (test root uuid))
		    (store! root uuid object))
		  v))))))

(define dumpit
  (macro expr
    `(if (bound? ,(cadr expr))
	 (if (,asis? ,(cadr expr)) 
	     ,(cadr expr)
	     (,dump-elt ,@(cdr expr)))
	 '|#unbound|)))

(defambda (dumper object refcounts root uuids (uuid #f))
  (cond ((not (bound? object)) '|#unbound|)
	((ambiguous? object)
	 (let* ((ptr (hashptr object))
		(refcount (get refcounts ptr)))
	   (if (and (= refcount 1) (< (choice-size object) 17))
	       (for-choices (elt object) 
		 (dumpit elt object refcounts root uuids ptr))
	       (add-condensed
		object root uuids
		(for-choices (elt object)
		  (dumpit elt object refcounts root uuids ptr))
		ptr uuid))))
	((or (void? object) (not object) (fail? object)) object)
	((constant? object) (intern (stringout object)))
	((asis? object) object)
	((pool? object)
	 (make-compound 'pool
			(pool-source object) 
			(try (poolctl object 'metadata 'opts) #f)))
	((index? object)
	 (make-compound 'index
			(index-source object) 
			(try (indexctl object 'metadata 'opts) #f)))
	((regex? object) (make-compound '|regex| (stringout object)))
	((and (or (special-form? object) (procedure? object) (macro? object))
	      (procedure-name object))
	 (add-condensed object root uuids
			(if (compound-procedure? object)
			    (make-compound '|lambda|
					   (procedure-name object)
					   (or (procedure-module object) 
					       (procedure-filename object))
					   (lambda-source object))
			    (make-compound '|primitive|
					   (procedure-name object)
					   (or (procedure-module object) 
					       (procedure-filename object))))
			(hashptr object)
			uuid))
	((test uuids (hashptr object)) (get uuids (hashptr object)))
	((not (cons? object)) (make-compound '%opaque (stringout object)))
	#|
	((and (or (vector? object) (pair? object) 
		  (string? object) (packet? object) 
		  (secret? object))
	      (test uuids object))
	 (get uuids object))
	|#
	((or (vector? object) (proper-list? object))
	 (let* ((ptr (hashptr object))
		(refcount (get refcounts ptr)))
	   (if (and (= refcount 1) (< (length object) 17))
	       (forseq (elt object) 
		 (dumpit elt object refcounts root uuids))
	       (add-condensed
		object root uuids
		(forseq (elt object)
		  (dumpit elt object refcounts root uuids ptr))
		ptr uuid))))
	((compound-procedure? object)
	 (try (get uuids (hashptr object))
	      (add-condensed
	       object root uuids
	       (if (lambda-source object)
		   (make-compound '%definition (lambda-source object))
		   (make-compound '%lambda 
				  (procedure-name object)
				  (lambda-args object)
				  (lambda-body object)
				  (hashptr (lambda-env object))))
	       (hashptr object) uuid)))
	((procedure? object)
	 (try (get uuids (hashptr object))
	      (add-condensed
	       object root uuids
	       (make-compound '%fcn (stringout object))
	       (hashptr object) uuid)))
	((or (and (compound? object) (compound-opaque? object))
	     (macro? object) (special-form? object)
	     (dtype-stream? object) (port? object)
	     (%lexref? object) (service? object)
	     (opcode? object) (thread? object) (synchronizer? object))
	 (debug%watch "Wrapping OPAQUE object " object)
	 (try (get uuids (hashptr object))
	      (add-condensed object root uuids
			     (make-compound '%opaque (stringout object))
			     (hashptr object) uuid)))
	((and (compound? object) (sequence? object))
	 (try (get uuids (hashptr object))
	      (let ((components (->vector object)))
		(add-condensed
		 object root uuids
		 (vector->compound
		  (forseq (elt components)
		    (dumpit elt object refcounts root uuids))
		  (compound-tag object))
		 (hashptr object) uuid))))
	((compound? object)
	 (try (get uuids (hashptr object))
	      (let* ((len (compound-length object))
		     (components (make-vector len #f)))
		(dotimes (i len)
		  (vector-set! components i (compound-ref object i)))
		(add-condensed
		 object root uuids
		 (apply make-compound (compound-tag object)
			(forseq (elt components)
			  (dumpit elt object refcounts root uuids)))
		 (hashptr object) uuid))))
	((or (string? object) (packet? object) (secret? object))
	 (add-condensed object root uuids object (hashptr object) uuid))
	((hashtable? object)
	 (let ((uuid (or uuid (getuuid)))
	       (ptr (hashptr object))
	       (condensed (make-hashtable (table-size object))))
	   (store! uuids ptr uuid)
	   (store! root uuid condensed)
	   (do-choices (key (getkeys object))
	     (store! condensed
	       (dumpit key object refcounts root uuids)
	       (dump-elt (get object key) object refcounts root uuids)))
	   condensed))
	((hashset? object)
	 (let* ((uuid (or uuid (getuuid)))
		(ptr (hashptr object))
		(elts (hashset-elts object))
		(celts (for-choices (elt elts)
			 (dumpit elt object refcounts root uuids)))
		(condensed (choice->hashset celts)))
	   (store! uuids ptr uuid)
	   (store! root uuid condensed)
	   condensed))
	((pair? object)
	 (cons (dump-elt (car object) object refcounts root uuids)
	       (dump-elt (cdr object) object refcounts root uuids)))
	((module? object)
	 (make-compound '|module| (pick (get object '%moduleid) symbol?)))
	((table? object)
	 (let ((condensed (frame-create #f)))
	   (when uuid
	     (store! root uuid condensed)
	     (store! uuids (hashptr object) uuid))
	   (do-choices (key (getkeys object))
	     (unless (bound? key) (set! key '|#unbound|))
	     (store! condensed
	       (if (asis? key) key (dumpit key object refcounts root uuids))
	       (dump-elt (get object key) object refcounts root uuids)))
	   condensed))
	((exception? object)
	 (let* ((condition (exception-condition object))
		(caller (exception-caller object))
		(details (exception-details object))
		(threadno (exception-threadno object))
		(sessionid (exception-sessionid object))
		(moment (exception-moment object))
		(timebase (exception-timebase object)))
	   (add-condensed object root uuids
			  (if (exception-irritant? object)
			      (make-compound '|exception|
					     condition caller details threadno sessionid moment timebase
					     (dump-elt (exception-context object) object refcounts root uuids)
					     (dump-elt (exception-stack object) object refcounts root uuids)
					     (dump-elt (exception-irritant object) object refcounts root uuids))
			      (make-compound '|exception|
					     condition caller details threadno sessionid moment timebase
					     (dump-elt (exception-context object) object refcounts root uuids)
					     (dump-elt (exception-stack object) object refcounts root uuids)))
			  (hashptr object) uuid)))
	(else (try (get uuids (hashptr object))
		   (let ((stringform (stringout object))
			 (ptr (hashptr object)))
		     (debug%watch "Fall through to opaque " object)
		     (if (> (get refcounts ptr) 1)
			 (add-condensed object root uuids
					(make-compound '%opaque stringform))
			 (make-compound '%opaque stringform)))))))

(define (condense object (into (make-hashtable)))
  (let ((refcounts (make-hashtable))
	(uuids (make-hashtable))
	(rootid #f))
    (count-refs object refcounts)
    (store! into '%root #f)
    (set! rootid (dumper object refcounts into uuids))
    (store! into '%root (get into rootid))
    (drop! into rootid)
    into))

(define (vaporize roots (cache (make-hashtable)))
  (try (get roots '%expanded)
       (let ((expanded (expander (get roots '%root) roots cache)))
	 (store! roots '%expanded expanded)
	 expanded)))
(define (condense/expand roots) (vaporize roots))

(defambda (expander object dump cache)
  (cond ((not (cons? object)) object)
	((ambiguous? object) (for-choices (elt object) (expander elt dump cache)))
	((uuid? object)
	 (try (get cache object)
	      (if (test dump object)
		  (let ((expanded (expander (get dump object) dump cache)))
		    (store! cache object expanded)
		    expanded)
		  object)))
	((or (vector? object) (proper-list? object)) 
	 (forseq (elt object) (expander elt dump cache)))
	((pair? object)
	 (cons (expander (car object) dump cache)
	       (expander (cdr object) dump cache)))
	((compound? object '|PROCEDURE|)
	 (let* ((name (string->symbol (compound-ref object 0)))
		(source (compound-ref object 1))
		(module (and (symbol? source) (get-loaded-module source))))
	   (if (and module (test module name))
	       (get module name)
	       object)))
	((compound? object '|EXCEPTION|)
	 (let* ((len (compound-length object))
		(vec (make-vector len)))
	   (dotimes (i len) 
	     (vector-set! vec i 
			  (expander (compound-ref object i) dump cache)))
	   (let* ((ex (vector->compound vec '|exception|))
		  (condition (compound-ref ex 0))
		  (caller (compound-ref ex 1))
		  (details (compound-ref ex 2))
		  (threadno (compound-ref ex 3))
		  (sessionid (compound-ref ex 4))
		  (moment (compound-ref ex 5))
		  (timebase (compound-ref ex 6))
		  (context (expander (compound-ref ex 7) dump cache))
		  (stack (expander (compound-ref ex 8) dump cache)))
	   (make-exception condition caller details 
		threadno sessionid moment timebase
		stack context
		(if (> (compound-length ex) 9)
		    (expander (compound-ref ex 9) dump cache)
		    #default)))))
	((compound? object '|regex|)
	 (string->lisp (compound-ref object 0)))
	((and (compound? object) (sequence? object))
	 (vector->compound (forseq (elt object) (expander elt dump cache))
			   (compound-tag object)))
	((compound? object)
	 (let* ((len (compound-length object))
		(components (make-vector len #f)))
	   (dotimes (i len)
	     (vector-set! components i
			  (expander (compound-ref object i) dump cache)))
	   (apply (if (compound-opaque? object) make-opaque-compound make-compound) 
		  (compound-tag object)
		  (->list components))))
	((hashtable? object)
	 (let ((expanded (make-hashtable)))
	   (do-choices (key (getkeys object))
	     (store! expanded (expander key dump cache) 
		     (expander (get object key) dump cache)))
	   expanded))
	((or (slotmap? object) (schemap? object))
	 (let ((expanded (frame-create #f)))
	   (do-choices (key (getkeys object))
	     (store! expanded (expander key dump cache) 
		     (expander (get object key) dump cache)))
	   expanded))
	((error? object)
	 (make-exception (exception-condition object)
			 (exception-caller object)
			 (expander (exception-details object) dump cache)
			 (exception-threadno object)
			 (exception-sessionid object)
			 (exception-moment object)
			 (exception-timebase object)
			 (expander (exception-stack object) dump cache)
			 (expander (exception-context object) dump cache)
			 (if (exception-irritant? object)
			     (expander (exception-irritant object) dump cache)
			     #default)))
	(else object)))
