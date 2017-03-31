(load-component "common.scm")

;;;; Random object generation

(define random-super-pools
  (make-oid (choice (random 200000)
		    (random 200000)
		    (random 200000)
		    (random 200000)
		    (random 200000)
		    (random 200000))
	    (* (random 256) 0x100000)))

(define (randomized-oid)
  (oid-plus (pick-one random-super-pools)
	    (random 0x100000)))
(define (random-fixnum) (random 1000000))
(define (random-bignum)
  (* (random-fixnum) (random-fixnum) (random-fixnum)))
(define (random-packet)
  (let ((len (random 100)))
    (let ((vector (make-vector len)))
      (dotimes (i len)
	(vector-set! vector i (random 256)))
      (vector->packet vector))))
(define (random-character)
  (integer->char (random 0x1000)))
(define (random-string)
  (let ((len (if (zero? (random 25)) (random 100) (random 15))))
    (stringout
     (dotimes (i len)
       (if (zero? (random 10))
	   (putchar (integer->char (1+ (random 256))))
	 (putchar (integer->char (1+ (random 128)))))))))
(define (random-symbol)
  (pick-one (difference (allsymbols) '%%slotids)))

(define (random-primobj)
  (let ((type (random 8)))
    (cond ((= type 0) (random-string))
	  ((= type 1) (random-symbol))
	  ((= type 2) (randomized-oid))
	  ((= type 3) (random (* 65536 256 64)))
	  ((= type 4) (random 65536))
	  ((= type 5) (random-symbol))
	  ((= type 6) (randomized-oid))
	  ((= type 7) (random-character))
	  (else (pick-one (allsymbols))))))

(define (randomelts n)
  (if (zero? n) '()
      (cons (random-primobj)
	    (randomelts (1- n)))))

(define (random-list)
  (randomelts (if (zero? (random 10)) 
		  (random 25)
		  (random 7))))

(define (random-vector)
  (->vector (randomelts (if (zero? (random 10)) (random 25)
			    (random 7)))))

(define (random-object)
  (let ((branch (random 9)))
    (cond ((= branch 0) (random-vector))
	  ((= branch 1) (random-list))
	  ((= branch 2) (random-slotmap))
	  (else (random-primobj)))))

(define (random-slotid)
  (if (zero? (random 2)) (randomized-oid) (random-symbol)))

(define (random-pair)
  (cons (random-primobj) (random-primobj)))

(define (random-slotvalue)
  (random-primobj))

(define random-slotids
  (let ((slotids {}))
    (dotimes (i 20) (set+! slotids (random-slotid)))
    slotids))

(define (random-slotmap)
  (let ((size (random 16)) (f (frame-create #f)))
    (dotimes (i size)
      (add! f (random-slotid)
	    (if (zero? (random 2)) (random-object)
		(let ((n-values (random 32)) (result {}))
		  (dotimes (i n-values)
		    (set+! result (random-slotvalue)))
		  result))))
    f))

(define (random-slotkey)
  (cons (pick-one random-slotids) (random-slotid)))

;;; Getting components

(define (get-components x)
  (cond ((vector? x) (get-components (elts x)))
	((pair? x)
	 (choice (get-components (car x))
		 (get-components (cdr x))))
	((oid? x) x)
	((table? x)
	 (for-choices (key (getkeys x))
	   (cons key (get-components (get x key)))))
	(else x)))
(define (get-atomic-components x)
  (cond ((vector? x) (get-atomic-components (elts x)))
	((string? x) (fail))
	((pair? x)
	 (choice (get-atomic-components (car x))
		 (get-atomic-components (cdr x))))
	((oid? x) x)
	((table? x)
	 (for-choices (key (getkeys x))
	   (choice key (get-atomic-components (get x key)))))
	(else x)))

;;; Generating random tables

(define (fill-table! table size (atomicp #f))
  (let ((items {}) (totalsize 0))
    (until (> totalsize size)
      (let ((item (random-object)))
	(set+! items item)
	(set! totalsize (+ totalsize (count-objects item)))))
    (add! table '{%items %xitems} items)
    (do-choices (item items)
      (let ((components (if atomicp
			    (get-atomic-components item)
			    (get-components item))))
	(add! table components item)))))

(define bad-maps {})
(define missed-maps {})

(defambda (atomic-contains? structure atom)
  (or (eq? structure atom)
      (if (ambiguous? structure)
	  (or (overlaps? atom structure)
	      (try-choices (s (reject structure {pair? vector? slotmap}))
		(tryif (atomic-contains? s atom) #t)))
	  (or (and (or (vector? structure) (pair? structure))
		   (or (position atom structure)
		       (atomic-contains? 
			(pick (elts structure) {pair? vector? slotmap}) 
			atom)))
	      (and (slotmap? structure) 
		   (let* ((keys (getkeys structure))
			  (all-elts (choice keys (get structure keys)))
			  (atomic-elts (reject all-elts {pair? vector? slotmap}))
			  (other-elts (difference all-elts atomic-elts)))
		     (or (overlaps? atom atomic-elts)
			 (atomic-contains? other-elts atom))))))))

(define (check-table table (atomicp #f))
  (let ((ok #t))
    (clearcaches)
    (do-choices (key (difference (getkeys table) '{%items %xitems}))
      (do-choices (value (get table key))
	(let ((components (if atomicp
			      (cachecall get-atomic-components value)
			      (cachecall get-components value))))
	  (unless (overlaps? key components)
	    (set! ok #f)
	    (message "Bad map for " key)
	    (set+! bad-maps (cons key value))))))
    (do-choices (item (get table '%items))
      (do-choices (component (if atomicp
				 (cachecall get-atomic-components item)
				 (cachecall get-components item)))
	(unless (test table component item)
	  (set! ok #f)
	  (message "Missed map for " component)
	  (set+! missed-maps (cons component item)))))
    ok))

;;; Top level

(define (table-for file (consed #f) (indextype (config 'indextype 'fileindex)))
  (cond ((has-suffix file ".slotmap") (frame-create #f))
	((has-suffix file ".table") (make-hashtable))
	((has-suffix file ".index")
	 (make-index file `#[type ,indextype slots 1000000
			     offtype ,(config 'offtype 'b40)]))
	(else (make-hashtable))))
(define (table-from file)
  (if (or (has-suffix file ".index")
	  (has-suffix file ".hashindex"))
      (open-index file)
    (file->dtype file)))

(define (save-table table file)
  (if (or (has-suffix file ".index")
	  (has-suffix file ".hashindex"))
      (begin (commit table) (swapout table) table)
    (begin (dtype->file table file)
	   (file->dtype file))))

(define intable #f)
(define outtable #f)

(define (count-objects x (size 0) (pair #f) (sum 0))
  (cond ((ambiguous? x)
	 (do-choices (elt x) (set! size (count-objects elt size)))
	 (1+ size))
	((null? x) size)
	((pair? x)
	 (set! pair x)
	 (while (pair? pair)
	   (set! size (count-objects (qc (car pair)) size))
	   (set! pair (cdr pair)))
	 (count-objects (qc pair) size))
	((vector? x)
	 (doseq (elt x) (set! size (count-objects (qc elt) size)))
	 size)
	((or (oid? x) (symbol? x) (string? x) (number? x)) 
	 (1+ size))
	((table? x)
	 (let ((keys (getkeys x)))
	   (do-choices (key keys)
	     (set! size (count-objects key size)))
	   (do-choices (key keys)
	     (set! size (count-objects (qc (get x key)) size)))
	   size))
	(else size)))

(define (table-report table (name "Table"))
  (message name " covers "
	   (choice-size (get table '%items)) " items, "
	   (table-size table) " keys, and "
	   (count-objects table) " objects"))

(define (edit-tests filename editfile)
  (if (file-exists? editfile)
      (let ((table (table-from filename))
	    (in (file->dtype editfile))
	    (worked #t))
	(message "Testing edits with " in)
	(when (test table '%items in)
	  (message "Drop didn't work")
	  (set! worked #f))
	(unless (identical? (get table '%xitems) in)
	  (message "Store didn't work")
	  (set! worked #f))
	(if worked
	    (message "Drop and store worked")
	    (exit 1)))
      (let* ((table (table-from filename))
	     (items (get table '%items))
	     (drop (try (pick-one (pick items {vector? slotmap? pair?}))
			(pick-one items))))
	(store! table '%xitems drop)
	(message "Dropping item " drop)
	(drop! table '%items drop)
	(dtype->file drop editfile)
	(commit))))

(define (tablecheckfn table atomicp)
  (unless (check-table table atomicp)
    (message "Table is inconsistent"))
  #t)

(define (get-rthreads)
  (config 'RTHREADS 
	  (and (CONFIG 'NTHREADS) 
	       (* 3 (CONFIG 'NTHREADS #f)))))
(define (get-wthreads) (config 'WTHREADS (CONFIG 'NTHREADS #f)))

(define (main filename (size #f) 
	      (rthreads (get-rthreads)) (wthreads (get-wthreads)))
  (when (and (config 'reset) (string? filename) (file-exists? filename))
    (remove-file filename))
  (message "■■ TABLETEST " (write filename) 
    (when size (printout " BUILD=" size))
    " cache=" (config 'cachelevel) 
    (when (config 'indextype) (printout " type=" (config 'indextype)))
    (when (config 'offtype) (printout " offtype=" (config 'offtype))))
  (if (and size (number? size))
      (let ((table (table-for filename (config 'CONSINDEX #f)))
	    (atomicp (has-suffix filename ".slotmap")))
	(when (file-exists? (glom filename ".edit.dtype")) 
	  (remove-file (glom filename ".edit.dtype")))
	(when wthreads (set! size (1+ (quotient size wthreads))))
	(set! intable table)
	(message "Generating table for " size " items")
	(if wthreads
	    (let ((use-size (1+ (quotient size wthreads)))
		  (threads {}))
	      (dotimes (i wthreads)
		(set+! threads (threadcall fill-table! table use-size atomicp)))
	      (threadjoin threads))
	    (fill-table! table size atomicp))
	(unless (config 'noreport) (table-report table))
	(message "Checking consistency....")
	(if rthreads
	    (let ((threads {}))
	      (dotimes (i rthreads)
		(set+! threads (threadcall tablecheckfn table atomicp)))
	      (threadjoin threads))
	    (tablecheckfn table atomicp))
	(message "Table is consistent, saving...")
	(save-table table filename)
	(message "Table saved, reloaded...")
 	(set! table (table-from filename))
	(set! outtable table)
	(unless (config 'noreport) (table-report table "Reloaded table"))
	(message "Checking consistency of reloaded table...")
	(if rthreads
	    (let ((threads {}))
	      (dotimes (i rthreads)
		(set+! threads (threadcall tablecheckfn table atomicp)))
	      (threadjoin threads))
	    (tablecheckfn table atomicp))
	(clearcaches)
	(message "Reloaded table is consistent."))
      (if size
	  (begin (edit-tests filename size)
		 (clearcaches))
	  (let* ((table (table-from filename))
		 (atomicp (has-suffix filename ".slotmap"))
		 (objectcount (count-objects table)))
	    (message "Loaded table with " (table-size table) " keys")
	    (unless (config 'noreport) (table-report table "Restored table"))
	    (message "Checking restored table for consistency...")
	    (if rthreads
		(let ((threads {}))
		  (dotimes (i rthreads)
		    (set+! threads (threadcall tablecheckfn table atomicp)))
		  (threadjoin threads))
		(tablecheckfn table atomicp))
	    (message "Restored table is consistent")
	    (clearcaches))))
  (test-finished "TABLETEST"))





