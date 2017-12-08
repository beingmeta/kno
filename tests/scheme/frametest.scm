(use-module '{fileio logger varconfig})

(load-component "common.scm")

(config! 'CHECKDTSIZE #t)

(define dbsource #f)
(define testpool #f)
(define testindex #f)

(define threaded #f)
(varconfig! THREADED threaded)

(define slotids-vec
  #(type in-file created defines expr atoms 
	 filename created contents-as-choice contents-as-frames
	 has %id))

(define (add-suffix file suffix)
  (if (has-suffix file suffix) file
      (glom file suffix)))

(define (sourcepool source (opts #f))
  (if (position #\@ source)
      (use-pool source opts)
      (let ((xopts (cons (frame-create #f
			   'type pooltype
			   'module (config 'poolmod {} #t)
			   'base @17/0 'capacity 65000
			   'offtype (config 'pooloff (config 'offtype {}))
			   'slotcodes (config 'slotcodes 16)
			   'readonly #f)
			 opts)))
	(cond ((file-exists? (add-suffix source ".pool"))
	       (use-pool (add-suffix source ".pool") xopts))
	      ((file-exists? source)
	       (use-pool (add-suffix source ".pool") xopts))
	      (else (make-pool (add-suffix source ".pool") xopts))))))

(define (sourceindex source (opts #f))
  (if (position #\@ source)
      (open-index source opts)
      (let ((xopts (cons (frame-create #f
			   'type indextype
			   'module (config 'indexmod {} #t)
			   'size 65000
			   'offtype (config 'indexoff (config 'offtype {}))
			   'slotcodes (config 'slotcodes 16)
			   'oidcodes (config 'oidcodes 16)
			   'readonly #f)
			 opts)))
	(cond ((file-exists? (add-suffix source ".index"))
	       (open-index (add-suffix source ".index") xopts))
	      ((file-exists? source) (open-index source xopts))
	      (else
	       (make-index (add-suffix source ".index") xopts))))))

(define (initdb source (opts #f))
  (set! testpool (sourcepool source opts))
  (set! testindex (sourceindex source opts))
  (set! dbsource source)
  (logwarn |Pool| testpool)
  (logwarn |Index| testindex))

(defambda (fix-flags flags)
  (cond ((or (null? flags) (pair? flags) (vector? flags)) flags)
	((ambiguous? flags) (choice->vector flags))
	(else (vector flags))))

(define (get-contents-as-list file)
  (let* ((elts '())
	 (infile (open-input-file file))
	 (input (read infile)))
    (until (eof-object? input)
      (set! elts (cons input elts))
      (set! input (read infile)))
    (reverse elts)))

(define (get-contents-as-vector file)
  (let* ((elts '())
	 (infile (open-input-file file))
	 (input (read infile)))
    (until (eof-object? input)
      (set! elts (cons input elts))
      (set! input (read infile)))
    (->vector (reverse elts))))

(define (get-contents-as-choice file)
  (let* ((elts {})
	 (infile (open-input-file file))
	 (input (read infile)))
    (until (eof-object? input)
      (set+! elts input)
      (set! input (read infile)))
    elts))

(define (get-expr-atoms x)
  (if (pair? x)
      (choice (get-expr-atoms (car x))
	      (get-expr-atoms (cdr x)))
      x))
(define (random-atom x)
  (if (pair? x)
      (if (null? (cdr x))
	  (random-atom (car x))
	(try (random-atom (pick-one (choice (car x) (cdr x))))
	     (random-atom (car x))
	     (random-atom (cdr x))))
    x))

(define (analyze-file file pool index)
  (message "Analyzing file " file)
  (let* ((now (timestamp))
	 (file-frame
	  (frame-create pool
	    'filename file 'obj-name file 'type 'filename '%id file
	    'created (get now '{shortstring hour season})
	    'contents-as-string (filestring file "iso-latin0")
	    'contents-as-list (get-contents-as-list file)
	    'contents-as-vector (get-contents-as-vector file)
	    'contents-as-choice (get-contents-as-choice file))))
    (do-choices (expr (get-contents-as-choice file))
      (let* ((exprnow (timestamp))
	     (expr-frame
	      (frame-create pool
		'expr expr 'type 'expr
		'created (get exprnow '{shortstring hour season})
		'in-file file-frame
		'context file-frame
		'atoms (get-expr-atoms expr))))
	(add! file-frame 'contents-as-frames expr-frame)
	(if (and (pair? expr) (eq? (car expr) 'define))
	    (let ((name (if (pair? (cadr expr)) (car (cadr expr))
			    (cadr expr))))
	      (add! expr-frame 'defines name)
	      (add! expr-frame '%id (list 'definition name)))
	    (add! expr-frame '%id expr))
 	(index-frame index expr-frame
 	  '{in-file created defines expr atoms type})
 	(index-frame index expr-frame 'has (getkeys expr-frame))))
     (index-frame index file-frame 'type)
     (index-frame index file-frame 'has (getkeys file-frame))
     (index-file-frame file-frame)))

(define (index-file-frame file-frame (index testindex) )
  (index-frame index file-frame 'contents-as-frames)
  (index-frame index file-frame
    '{filename created contents-as-choice contents-as-frames type}))

(define (makedb pool index files)
  (message "Building DB")
  (let* ((symbols-oid (allocate-oids pool))
	 (numbers-oid (allocate-oids pool))
	 (files-oid (allocate-oids pool))
	 (noslots-oid (frame-create pool))
	 (allsyms (allsymbols)))
    ;; For the non-frame OIDs, we store values in two places
    ;; and compare.

    ;; The symbols OID
    (set-oid-value! symbols-oid allsyms)
    (dtype->file allsyms (stringout dbsource "-symbols.dtype"))

    ;; The numbers OID
    (let ((nums {}))
      (dotimes (i 20) (set+! nums (+ i (* i 10))))
      (set! nums (choice nums (* nums 1.0)))
      (dtype->file nums (stringout dbsource "-numbers.dtype"))
      (set-oid-value! numbers-oid nums))

    ;; The files OID
    (set-oid-value! files-oid (elts files))
    (dtype->file (elts files) (stringout dbsource "-files.dtype"))

    ;; Try adding to the empty (noslots) OID.  This produces an error
    ;;  if the slotmap isn't initialized correctly.
    (add! noslots-oid 'x "x")
    ;; Reset the value
    (set-oid-value! noslots-oid (frame-create #f))

    ;; Simple checks that pools are working right
    (applytest #t in-pool? numbers-oid pool)
    (applytest pool getpool files-oid)
    
    ;; Index the oids created at the beginning
    (index-frame index symbols-oid '%id 'symbols)
    (index-frame index numbers-oid '%id 'numbers)
    (index-frame index files-oid '%id 'files)
    (index-frame index noslots-oid '%id 'noslots)
    
    ;; Analyze the specified files
    (if threaded
	(begin (thread/wait (thread/call analyze+commit
				(elts files) pool index))
	  (commit))
	(dolist (file files) 
	  (analyze-file file pool index)
	  (commit))))
  (message "Done building DB"))
  
(define (analyze+commit file pool index)
  (analyze-file file pool index)
  (commit pool)
  (commit index))

(define (checkoids pool index)
  (message "Testing basic OID functionality for " pool)
  (applytest #t oid? (string->lisp "@17/0"))
  (let* ((symbols-oid (find-frames index '%id 'symbols))
	 (numbers-oid (find-frames index '%id 'numbers))
	 (files-oid (find-frames index '%id 'files))
	 (noslots-oid (find-frames index '%id 'noslots)))
    ;; The symbols oid should have been allocated first
    (applytest symbols-oid pool-base pool)
    (applytest #t symbol? (oid-value symbols-oid))
    (applytest #t number? (oid-value numbers-oid))
    (when (exists? (oid-value files-oid))
      (applytest #t string? (oid-value files-oid)))
    (applytest (file->dtypes (stringout dbsource "-symbols.dtype"))
	       oid-value symbols-oid)
    (applytest (file->dtypes (stringout dbsource "-numbers.dtype"))
	       oid-value numbers-oid)
    (applytest (file->dtypes (stringout dbsource "-files.dtype"))
	       oid-value files-oid)
    (add! noslots-oid 'firstslot 1)
    (add! noslots-oid 'firstslot "one")
    (add! noslots-oid 'firstslot 'one)
    (set-oid-value! noslots-oid (frame-create #f)))
  (message "Done testing basic OID functionality for " pool))

(define (checkfilename frame pool index)
  (message "Checking filename frame " frame)
  (let* ((filename (get frame 'filename))
	 (stringval (filestring filename "iso-latin0"))
	 (listval (get-contents-as-list filename))
	 (vectorval (get-contents-as-vector filename))
	 (exprsval (get-contents-as-choice filename)))
    
    (applytest frame find-frames index 'filename filename)
    
    (applytest #t test frame 'contents-as-string)
    (applytest #t test frame 'contents-as-string stringval)
    (applytest stringval get frame 'contents-as-string)
    (applytest #t test frame 'contents-as-list)
    (applytest #t test frame 'contents-as-list listval)
    (applytest listval get frame 'contents-as-list)
    
    (applytest #t test frame 'contents-as-vector)
    (applytest #t test frame 'contents-as-vector vectorval)
    (applytest vectorval get frame 'contents-as-vector)
    (applytest exprsval get frame 'contents-as-choice)
    (applytest (choice-size exprsval)
 	       choice-size (find-frames index 'type 'expr 'in-file frame)))
  (message "Done checking filename frame " frame))

(define (checkexpr frame pool index)
  (applytest (get frame 'in-file)
	     find-frames index 'contents-as-frames frame)
  (applytest #t overlaps? frame
	     (find-frames index 'in-file (get frame 'in-file)))
  (applytest #t overlaps? (get frame 'in-file)
	     (find-frames index 'contents-as-choice (get frame 'expr)))
  (do-choices (atom (get-expr-atoms (get frame 'expr)))
    (applytest #t test frame 'atoms atom)
    (applytest #t overlaps? frame
	       (find-frames testindex 'atoms atom))))

(define (checkdb count pool index)
  ;; Check basic oid functionality
  (checkoids pool index)
  (message "Running DB wide integrity checks")
  (applytest #t identical?
	     (find-frames index 'has 'filename)
	     (find-frames index 'type 'filename))
  (applytest #t identical?
	     (find-frames index 'has 'expr)
	     (find-frames index 'type 'expr))
  (applytest #t identical?
	     (get (find-frames index 'has 'filename) 'filename)
	     (file->dtypes (stringout dbsource "-files.dtype")))
  (message "Checking individual filename frames")
  (do-choices (fframe (find-frames index 'type 'filename))
    (checkfilename fframe pool index))
  (message "Checking " count " random expressions")
  (let ((exprs (find-frames index 'type 'expr)))
    (dotimes (i count)
      (checkexpr (pick-one exprs) pool index)))
  (message "Successfully checked " count " expressions")
  (message "Finished checking database integrity"))

(define (doremove file)
  (cond ((not (file-exists? file)))
	((file-directory? file)
	 (let ((contents (getfiles file))
	       (subdirs (getdirs file)))
	   (remove-file! contents)
	   (doremove subdirs))
	 (rmdir file))
	(else (remove-file! file))))

(define (setup)
  (doremove (append "dbtest" {".pool" ".index"
			      "-symbols.dtype" "-files.dtype"
			      "-numbers.dtype"}))
  (initdb "dbtest" #[readonly #f])
  (makedb testpool testindex
	  '("r4rs.scm" "misctest.scm" "seqtest.scm" "choicetest.scm")))

(define (main source (operation "test") . files)
  (cond ((not (equal? operation "init")))
	((position #\@ source)
	 (unless (zero? (pool-load (use-pool (add-suffix source ".pool"))))
	   (message "Doing init on non-virgin pool")))
	(else (doremove
	       (append source {".pool" ".index"
			       "-symbols.dtype" "-files.dtype"
			       "-numbers.dtype"}))))
  (initdb source (and (equal? operation "test") #[readonly #t]))
  (when (equal? operation "init")
    (makedb testpool testindex files)
    (checkdb (config 'COUNT 1000) testpool testindex)
    (commit)
    (swapout))
  (checkdb (config 'COUNT 1000) testpool testindex)
  (swapout)
  (checkdb (config 'COUNT 1000) testpool testindex)
  (swapout))





