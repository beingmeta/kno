;;; -*- Mode: fdscript; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'reflection)

(applytest #t procedure? car)
(applytest #f procedure? if)
(applytest #f procedure? '())
(applytest #f procedure? 'car)
(applytest #t applicable? car)
(applytest #f applicable? if)
(applytest #f applicable? '())
(applytest #f applicable? 'car)
(applytest #f special-form? 'if)
(applytest #t special-form? if)
(applytest #f special-form? car)
(applytest #f primitive? 'car)
(applytest #t primitive? get)
(applytest #t primitive? applytest)
(applytest 1 fcn-arity car)
(applytest 2 fcn-arity cons)
(applytest #t primitive? get)

(define f2 (frame-create #f  'bar 8 'foo 3))

(define f1 (frame-create #f 'foo 3 'bar 8))
;; Note that this definition also (in dbtest.scm) using slotmaps as keys
(define f3 #[bar 8 foo 3])

(applytest #f eq? f1 (deep-copy f1))
(applytest #t equal? f1 (deep-copy f1))

(applytest #f eq? f1 f2)
(applytest #t equal? f1 f2)

(applytest #f eq? f1 f3)
(applytest #t equal? f1 f3)

;;; Testing iterative environments

(define intfns '())
(dotimes (i 5) (set! intfns (cons (lambda () i) intfns)))
(applytest 4 (car intfns))
(applytest 3 (cadr intfns))
(set! intfns '())

(define lfns '())
(doseq (v '("alpha" "beta" "gamma" "delta"))
  (set! lfns (cons (lambda () v) lfns)))
(applytest "delta" (car lfns))
(applytest "gamma" (cadr lfns))
(set! lfns '())

(define chfns '())
(do-choices (v {"alpha" "beta" "gamma" "delta"})
  (set! chfns (cons (lambda () v) chfns)))
(applytest #f equal? ((car chfns)) ((cadr chfns)))
(set! chfns '())

;;; Testing packets

(define p1 #"\1f\03")
(define p2 #"\c0\bd\4e\c9\37\cf\d6\78\0e\a2\9c\4b\a6\cf\30\74")
(define p3 #"\1f\03\20")

(applytest 2 length p1)
(applytest 16 length p2)
(applytest 3 length p3)
(applytest 3 elt p1 1)
(applytest 120 elt p2 7)
(applytest 0x1f elt p3 0)

;;; Testing timeout
;; (when (bound? with-time-limit)
;;  (testing 'with-time-limit
;;	   '(with-time-limit 1 (dotimes (i 1000000000)) #f)
;;	   #f))

(define memoization-index
  (make-file-index "memoization.index" 1000))

(applytest #t real? 0.1)
(applytest #t real? .1)
(applytest #t number? 0.1)
(applytest #t real? (/ 1 12))
(applytest #t number? (/ 1 12))
(applytest #t number? .1)
(applytest '(1) subseq '(1 2) 0 1)
(applytest '() subseq '(1 2) 0 0)
(applytest '() subseq '(1 2) 1 1)

;;; Sorting tests

(define sort-sample
  '{33 34 134 33.2 33.1 34.5
       ("thirty" "three")
       1300000000000
       #("thirty" "one" "hundred")
       "thirty-one" "thirty-eight"
      #("thirty" "three" "hundred" "five")})
(applytest #(33 33.100000 33.200000 34 34.500000 134 1300000000000
		"thirty-eight" "thirty-one"
		("thirty" "three")
		#("thirty" "one" "hundred")
		#("thirty" "three" "hundred" "five"))
	   sorted sort-sample)
(define sort-seq-sample
  (filter-choices (elt sort-sample)
    (sequence? elt)))

(applytest #(("thirty" "three")
	     #("thirty" "one" "hundred") 
	     #("thirty" "three" "hundred" "five")
	     "thirty-one" "thirty-eight")
	   sorted sort-seq-sample length)

(applytest #(33 33.100000 33.200000 34 34.500000 134 1300000000000
		"thirty-eight" "thirty-one"
		("thirty" "three")
		#("thirty" "one" "hundred")
		#("thirty" "three" "hundred" "five"))
	   sorted sort-sample deep-copy)

(applytest #("thirty-one"
	     "thirty-eight"
	     #("thirty" "three" "hundred" "five")
	     #("thirty" "one" "hundred")
	     ("thirty" "three"))
	   sorted sort-seq-sample
	   (lambda (x) (elt x -1)))

;;;; Test writing of DTypes to disk files

(dtype->file 33 "thirtythree")
; (let ((p (fopen "thirtythree" "rb")))
;   (unless (= (read-byte p) #x03)
;     (lineout "Integer DType written funny"))
;   (let ((first-byte (read-byte p)))
;     (unless (zero? first-byte)
;       (lineout "Integer data written funny")
;       (if (= first-byte 33)
; 	  (lineout "Byte order incorrect")))
;     (unless (and (zero? (read-byte p))
; 		 (zero? (read-byte p)))
;       (lineout "Integer data written funny"))
;     (unless (= (read-byte p) 33)
;       (lineout "Integer data written funny")))
;   (fclose p))

;(remove-file "thirtythree") 

(define (nrange start end)
  (let ((answer {}))
    (dotimes (i (- end start))
      (set+! answer (+ start i)))
    answer))

(let ((original (nrange 30 50)))
  (dtype->file original "thirty2fifty")
  (if (= (choice-size (intersection original
				    (file->dtype "thirty2fifty")))
	 (choice-size original))
      (lineout "Sets seem to be dumped right")
      (lineout "Sets seem to be dumped wrong"))) 
;(remove-file "thirty2fifty")

(define dtype-test-obj
  `(#[foo 3 bar 8] #"12\345\6789\abcdef123\45\6789"
    #t #f #\a () 9739 -3 3.1415 #\u3f4e
    ,(vector 34 35 1024)
    ,(vector 34 35 1024 150000)
    ,(vector 3.4 3.5 1000000000.05 .00000001)
    ,(vector 3.4 3.5 100000000000.05 .000000000000000001)
    ,@'((test) "te \" \" st" "" test #() b c)))

(applytest 309 dtype->file dtype-test-obj "test.dtype")
(applytest dtype-test-obj file->dtype "test.dtype")

;;; These are various regression tests for some GC problems

(use-module 'texttools)

;;; These check that the binding iterators don't get messed up
;;;  if the symbol they are binding is changed.

(define strings (elts (segment "alpha beta gamma delta epsilon phi omega")))
(define stringlist (segment "alpha beta gamma delta epsilon phi omega"))
(define (runtest args)
  (let ((results {}))
    (do-choices (string args)
      (set! string (string-subst string "a" "x"))
      (set+! results string))
    results))
(define (runseqtest arg)
  (let ((results {}))
    (doseq (string arg)
      (set! string (string-subst string "a" "x"))
      (set+! results string))
    results))
(define (runlisttest arg)
  (let ((results {}))
    (dolist (string arg)
      (set! string (string-subst string "a" "x"))
      (set+! results string))
    results))

(define (runtest2 args)
  (let ((results {}))
    (do-choices (string args)
      (set! string ((lambda (x) (string-subst string "a" "x")) string))
      (set+! results string))
    results))
(define (runseqtest2 arg)
  (let ((results {}))
    (doseq (string arg)
      (set! string ((lambda (x) (string-subst string "a" "x")) string))
      (set+! results string))
    results))
(define (runlisttest2 arg)
  (let ((results {}))
    (doseq (string arg)
      (set! string ((lambda (x) (string-subst string "a" "x")) string))
      (set+! results string))
    results))

(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
 	   runtest strings)
(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runtest strings)

(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
 	   runtest2 strings)
(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runtest2 strings)

(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runseqtest stringlist)
(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runseqtest stringlist)

(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runseqtest2 stringlist)
(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runseqtest2 stringlist)

(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runlisttest stringlist)
(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runlisttest stringlist)

(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runlisttest2 stringlist)
(applytest {"phi" "betx" "deltx" "gxmmx" "omegx" "xlphx" "epsilon"}
	   runlisttest2 stringlist)

(define (get-expr-atoms expr)
  (cond ((pair? expr)
	 (choice (get-expr-atoms (car expr)) (get-expr-atoms (cdr expr))))
        ((vector? expr) (get-expr-atoms (elts expr)))
        ((table? expr)
         (let ((keys (getkeys expr)))
           (choice (get-expr-atoms keys) (get-expr-atoms (get expr keys)))))
        (else expr)))

(define (get-atoms x)
   (if (procedure? x) (get-expr-atoms (procedure-body x))
       (get-expr-atoms x)))

(define (leaker x)
  (let* ((atoms (get-atoms x))
         (pnames (symbol->string (pick atoms symbol?)))
         (pnamelist (sorted pnames)))
     (map (lambda (x) (length x)) pnamelist)))

(applytest #(5 9 6 6 4 3 4 9 6 6 14 7 1) leaker leaker)

(message "MISCTEST successfuly completed")


