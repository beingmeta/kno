(in-module 'bench/randobj)

(module-export! '{random-fixnum random-bignum 
		  random-packet random-character
		  random-string random-symbol
		  random-pair random-vector 
		  random-slotmap
		  random-table})
(module-export! '{get-components get-atomic-components})
(module-export! '{count-objects table-report})

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
      (->packet vector))))
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

(define (random-pair)
  (randomelts (if (zero? (random 10)) 
		  (random 25)
		  (random 7))))

(define (random-vector)
  (->vector (randomelts (if (zero? (random 10)) (random 25)
			    (random 7)))))

(define (random-object)
  (let ((branch (random 9)))
    (cond ((= branch 0) (random-vector))
	  ((= branch 1) (random-pair))
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

;;; Reporting

(define (count-objects x (size 0) (pair #f) (sum 0))
  (cond ((ambiguous? x)
	 (do-choices (elt x)
	   (set! size (count-objects elt size)))
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

;;; Making random tables

(define (random-table table (size 128) (atomicp #f) (aggindex (config 'aggindex)))
  (let ((items ({random-fixnum random-bignum 
		 random-packet random-character
		 random-string random-symbol
		 random-pair random-vector 
		 random-slotmap}))
	(totalsize 0))
    (until (> totalsize size)
      (let ((item (random-object)))
	(set+! items item)
	(set! totalsize (+ totalsize (count-objects item)))))
    (add! table '{%items %xitems} items)
    (do-choices (item items)
      (let ((components (if atomicp
			    (get-atomic-components item)
			    (get-components item))))
	(add! (if aggindex 
		  (pick-one (indexctl table 'partitions))
		  table)
	      components item)))))
