;;; -*- Mode: Scheme; text-encoding: utf-8 -*-

(load-component "common.scm")

(use-module '{reflection varconfig stringformats})

(optimization-leaks)

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
(applytest 1 procedure-arity car)
(applytest 2 procedure-arity cons)
(applytest #t primitive? get)

(define f2 (frame-create #f  'bar 8 'foo 3))

(define f1 (frame-create #f 'foo 3 'bar 8))
;; Note that this definition also (in dbtest.scm) using slotmaps as keys
(define f3 #[bar 8 foo 3])

(applytest #f eq? f1 (deep-copy f1))
(applytest #t equal? f1 (deep-copy f1))

(applytest #f eq? f1 f2)
(applytest #f equal? f1 f2)

(applytest #f eq? f1 f3)
(applytest #f equal? f1 f3)

(applytest #f eq? f2 f3)
(applytest #t equal? f2 f3)

(applytest #f test f1 'baz)
(applytest #f test f1 'baz 9)
(applytest #t test f1 'bar)
(applytest #t test f1 'bar 8)
(applytest #f test f1 'bar 9)

(define l2 '("e" "f"))
(define l3 (cons "d" l2))
(define l4 (cons "c" l3))
(define l5 (cons "b" l4))
(define l6 (cons "a" l5))

(applytester "a" car l6)
(applytester "b" cadr l6)
(applytester "c" caddr l6)
(applytester l2 cdr l3)
(applytester l2 cddr l4)

;; This is a regression test for the TEST primitive for slotmaps using
;; kno_sortvec_get rather than kno_keyvec_get internally.
(evaltest #t (and (test f1 'bar 8)
		  (test f1 'foo 3)
		  (test f2 'bar 8)
		  (test f2 'foo 3)))

(applytest 8 get f1 'bar)
(applytest 3 get f1 'foo)
(applytest {} get f1 'quux)

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

(define p1 #X"1f03")
(define p2 #X"c0bd4ec937cfd6780ea29c4ba6cf3074")
(define p3 #X"1f0320")

(define s1 #*X"1f0320")
(define s2 #*"keep it safe")

(applytest #t packet? p1)
(applytest #t packet? p2)
(applytest #t packet? p3)
(applytest #t packet? s1)
(applytest #t packet? s2)
(applytest #f packet? "foo")
(applytest #f packet? 3)
(applytest #f packet? #(a b c))
(applytest #f packet? '(a b c))

(applytest #t secret? s1)
(applytest #t secret? s2)
(applytest #f secret? "foo")
(applytest #f secret? 3)
(applytest #f secret? #(a b c))
(applytest #f secret? '(a b c))

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
  (make-index "memoization.index" #[type fileindex slots 1000]))

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
		#("thirty" "one" "hundred")
		#("thirty" "three" "hundred" "five")
		("thirty" "three"))
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
		#("thirty" "one" "hundred")
		#("thirty" "three" "hundred" "five")
		("thirty" "three"))
	   sorted sort-sample deep-copy)

(applytest #("thirty-one"
	     "thirty-eight"
	     #("thirty" "three" "hundred" "five")
	     #("thirty" "one" "hundred")
	     ("thirty" "three"))
	   sorted sort-seq-sample
	   (lambda (x) (elt x -1)))

(applytest #(("thirty" "three")
	     #("thirty" "one" "hundred")
	     #("thirty" "three" "hundred" "five")
	     "thirty-eight"
	     "thirty-one")
	   rsorted sort-seq-sample
	   (lambda (x) (elt x -1)))

;; (errtest (sorted sort-seq-sample (lambda (x) (elt x 900))))
;; (errtest (rsorted sort-seq-sample (lambda (x) (elt x 900))))

(applytest #() sorted {})
(applytest #() sorted {} car)

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

(define-tester (nrange start end)
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
  `(#[foo 3 bar 8] #"12\0645\14789\253cdef123\105\14789"
    #t #f #\a () 9739 -3 3.1415 #\u3f4e
    ,(vector 34 35 1024)
    ,(vector 34 35 1024 150000)
    ,(vector 3.4 3.5 1000000000.05 .00000001)
    ,(vector 3.4 3.5 100000000000.05 .000000000000000001)
    ,@'((test) "te \" \" st" "" test #() b c)))

(applytest #t string? (stringout (pprint dtype-test-obj)))
(applytest #t string? (stringout (listdata dtype-test-obj)))
(applytest #t string? (stringout (pprint sort-sample)))
(applytest #t string? (stringout (listdata sort-sample)))
(applytest #t string? (stringout (pprint sort-seq-sample)))
(applytest #t string? (stringout (listdata sort-seq-sample)))

(applytest 309 dtype->file dtype-test-obj "test.dtype")
(applytest dtype-test-obj file->dtype "test.dtype")

(applytest 8 length "Foo\|bar\&middot;")
(errtest (string->lisp "@abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"))
(errtest (string->lisp "@123/kef"))
(errtest (string->lisp "@kef"))

(applytest 12 length #"ab\n\a\b\\\f\h\t\r\z\#")
(applytest 14 length #"ab\n\a\b\\\f\h\t\r\z\#\035\013")
;; (errtest (string->lisp #"ab\n\a\b\\\f\h\t\r\z\#\035\013\090"))
;; (errtest (string->lisp "#\"ab\n\a\b\\\f\h\t\r\z\#\035\013\x2g;\""))
(errtest (string->lisp "#\"xyz\\x2g\""))
(errtest (string->lisp "#\"ab\n\a\b\\\f\h\t\r\z\#\035\013\\xg2;\""))

;;; These are various regression tests for some GC problems

(use-module 'texttools)

;;; These check that the binding iterators don't get messed up
;;;  if the symbol they are binding is changed.

(define strings (elts (segment "alpha beta gamma delta epsilon phi omega")))
(define stringlist (segment "alpha beta gamma delta epsilon phi omega"))
(define-tester (runtest args)
  (let ((results {}))
    (do-choices (string args)
      (set! string (string-subst string "a" "x"))
      (set+! results string))
    results))
(define-tester (runseqtest arg)
  (let ((results {}))
    (doseq (string arg)
      (set! string (string-subst string "a" "x"))
      (set+! results string))
    results))
(define-tester (runlisttest arg)
  (let ((results {}))
    (dolist (string arg)
      (set! string (string-subst string "a" "x"))
      (set+! results string))
    results))

(define-tester (runtest2 args)
  (let ((results {}))
    (do-choices (string args)
      (set! string ((lambda (x) (string-subst string "a" "x")) string))
      (set+! results string))
    results))
(define-tester (runseqtest2 arg)
  (let ((results {}))
    (doseq (string arg)
      (set! string ((lambda (x) (string-subst string "a" "x")) string))
      (set+! results string))
    results))
(define-tester (runlisttest2 arg)
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

(define-tester (get-expr-atoms expr)
  (cond ((pair? expr)
	 (choice (get-expr-atoms (car expr)) (get-expr-atoms (cdr expr))))
        ((vector? expr) (get-expr-atoms (elts expr)))
        ((table? expr)
         (let ((keys (getkeys expr)))
           (choice (get-expr-atoms keys) (get-expr-atoms (get expr keys)))))
        (else expr)))

(define-tester (get-atoms x)
   (if (procedure? x) (get-expr-atoms (lambda-body x))
       (get-expr-atoms x)))

(define (leaker x)
  (let* ((atoms (get-atoms x))
         (pnames (symbol->string (pick atoms symbol?)))
         (pnamelist (sorted pnames)))
    (map (lambda (x) (length x)) pnamelist)))

(applytest #(5 9 6 6 4 3 4 9 6 6 14 7 1) leaker leaker)

;;; Various generic table functions

(define slotmap (frame-create #f 'a 3 'b 4 'c {5 6} 'd {}))
(define slotmap-literal #[a 3 b 4 c {5 6} d {}])

(applytest '{a b c} getkeys slotmap)
(applytest '{3 4 5 6} getvalues slotmap)
(applytest '{(a . 3) (b . 4) (c . {5 6})} getassocs slotmap)

(applytest '{a b c d} getkeys slotmap-literal)
(applytest '{3 4 5 6} getvalues slotmap-literal)
(applytest '{(a . 3) (b . 4) (c . {5 6}) (d . {})}
	   getassocs slotmap-literal)

(define hashtable
  (let ((table (make-hashtable)))
    (do-choices (key (getkeys slotmap-literal))
      (store! table key (qc (get slotmap-literal key))))
    table))

(applytest '{a b c d} getkeys hashtable)
(applytest '{3 4 5 6} getvalues hashtable)
(applytest '{(a . 3) (b . 4) (c . {5 6}) (d . {})}
	   getassocs hashtable)

(applytest slotmap-literal plist->table
	   '(A 3 b 4 C {5 6} D {}))
(applytest slotmap-literal alist->table
	   '((A . 3) (b . 4) (C . {5 6}) (D . {})))
(applytest slotmap-literal blist->table
	   '((A 3) (b 4) (C 5 6) (D)))

;;; This finds an anti-leak in optional arguments
;;; With this bug, the passed in argument Y to break-defaults
;;; is GC'd one time too many.

(message "Checking for double-GC bug in optional arguments defaults")
(define external-state (vector 3))
(define (break-defaults x (y) (z (vector))) (set! y 5) 3)
(break-defaults 8 external-state)
(evaltest #t (vector? external-state))

;;; Quasiquote oddness

(define-tester (splicetest)
  (let* ((x '(a b c))
         (y `(q ,@x d e f)))
    x))

(applytest '(A B C) splicetest)

;;; This tests that comments as arguments work

(define errors {})

(define-tester (plus3 x y z) (+ x y z))
(onerror (evaltest 27 (plus3 8 9 10))
  (lambda (ex) (set! errors ex)))
(onerror (evaltest 27 (plus3 8 #;() 9 10))
  (lambda (ex) (set+! errors ex)))

(define (plus4 x y z (q 8)) (+ x y z q))
(evaltest 77 (plus4 8 9 10 50))
(onerror (evaltest 30 (plus4 8 #;() 9 10 3))
  (lambda (ex) (message "ERROR!" ex) (set+! errors ex)))
(onerror (evaltest 35 (plus4 8 #;() 9 10))
  (lambda (ex) (message "ERROR!" ex) (set+! errors ex)))

(evaltest {3 4 5 7} (choice 3 4 5 7))
(onerror (evaltest {3 4 5 7} (choice 3 4 5 #;6 7))
  (lambda (ex) (message "ERROR!" ex) (set+! errors ex)))

(evaltest #f (test '(a . b) 'c 8))
(onerror (evaltest #f (test '(a . b) 'c #;"test" 8))
  (lambda (ex) (message "ERROR!" ex) (set+! errors ex)))

(evaltest #t (test '(a . b) 'a 'b))
(onerror (evaltest #t (test '(a . b) 'a #;"test" 'b))
  (lambda (ex) (message "ERROR!" ex) (set+! errors ex)))

(evaltest #f (number? (string->number "5a")))
(evaltest #f (number? (parse-arg "5a")))
(evaltest #f (number? (string->lisp "5a")))
(evaltest 90 (string->number "0x5a"))
(evaltest 90 (string->lisp "0x5a"))

(applytest 1024 string->lisp "#kib")
(applytest 1024 string->lisp "#1kib")
(applytest 2048 string->lisp "#2kib")
(applytest 1048576 string->lisp "#mib")
(applytest 1048576 string->lisp "#1mib")
(applytest 3145728 string->lisp "#3mib")
(applytest 1073741824 string->lisp "#gib")
(applytest 1073741824 string->lisp "#1gib")
(applytest 2147483648 string->lisp "#2gib")

(evaltest 2147483648 #2gib)
(applytest 2147483648 second '(first #2gib))

(define (add2numbers x y)
  "This procedure adds 2 numbers and returns the result"
  (+ x y))
(define (add2numbers.longdoc x y)
  "This procedure adds 2 numbers and returns the result."
  "It uses the underlying +/-/etc arithmetic operators, "
  "so it's relatively generic."
  (+ x y))

(applytest "`(add2numbers x y)`\nThis procedure adds 2 numbers and returns the result"
	   procedure-documentation add2numbers)
(applytest "`(add2numbers.longdoc x y)`\nThis procedure adds 2 numbers and returns the result.\nIt uses the underlying +/-/etc arithmetic operators, \nso it's relatively generic."
	   procedure-documentation add2numbers.longdoc)

;; This checks a bug where errors in an else were ignored
(evaltest #t
	  (onerror
	      (cond ((= 3 4) (error "Not invoked") #f)
		    (else (error "should abort") #f))
	    (lambda (x) #t)
	    (lambda () #f)))
(evaltest #t
	  (onerror
	      (when #t (error "should abort") #f)
	    (lambda (x) #t)
	    (lambda () #f)))
(evaltest #t
	  (onerror
	      (unless #f (error "should abort") #f)
	    (lambda (x) #t)
	    (lambda () #f)))
(evaltest #f
	  (onerror
	      (unless #t (error "should abort") #f)
	    (lambda (x) #t)
	    (lambda ()
	      (message "ONERROR works with no values returned")
	      #f)))

;;; Testing use-binding and %use-binding

(define $num (get-binding 'stringfmts '$num))
(define $num2 (importvar 'stringfmts '$num))

(evaltest "3" (stringout ($num 3)))
(evaltest "186,000" (stringout ($num 186000)))

(evaltest "3" (stringout ($num2 3)))
(evaltest "186,000" (stringout ($num2 186000)))

;;; Testing schemap evaluation

(applytest 7 get [x 3 y (+ 3 4)] 'y)

;;; Testing structural evaluation

(evaltest #(1 2 3) #.((/ 2 2) (+ 1 1) (/ 9 3)))
(evaltest #[alpha #\a beta "B" gamma "abcd"]
	  #.[alpha 
	     (elt "abcde" 0) 
	     beta (intersection {"B" "C"} "B")
	     gamma (slice (glom "ab" "cd" "ef") 0 4)
	     delta (intersection '{"A" "B"} {"C" "D"})])
(evaltest #[alpha #\a beta "B" gamma "abcd" delta {}]
	  #.[alpha 
	     (elt "abcde" 0) 
	     beta (intersection {"B" "C"} "B")
	     gamma (slice (glom "ab" "cd" "ef") 0 4)
	     delta (qc (intersection '{"A" "B"} {"C" "D"}))])

;;; Hash function bugs

(define (neghash x) (< (hash-lisp x) 0))

(applytest #f neghash 3.653593e+10)

;;; Check for stack bugs

(applytest 3
	   (try (singleton (let ((x 3) (y 4)) (choice x y)))
		(singleton (let ((x 3) (y 3)) (choice x y)))
		(singleton (let ((x 3) (y 5)) (choice x y)))))

;;;; Other small tests

(define z 8)

(evaltest 8 (prog1 z (set! z (1+ z)) z) )

(define seq #("foo" foo 3))
(evaltest 3 (tryseq (e seq) (tryif (number? e) e)))
(evaltest 6 (tryseq (e seq i) (tryif (number? e) (* i e))))

(applytest #t coderef? (make-coderef 5))

;;; Check for apply bugs with different arities

(applytest 20 _plus4 5 5 5 5)
(applytest 15 _plus4 5 5 5)
(applytest 15 _plus15 5 5 5)
(applytest 75 _plus15 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5)

;;; Assoc errors

(errtest (assq))
(errtest (assv))
(errtest (assoc))
(errtest (assq 'x))
(errtest (assv 'x))
(errtest (assoc 'x))
(errtest (assq 'x "string"))
(errtest (assv 'x "string"))
(errtest (assoc 'x "string"))
(errtest (assq 'x '("string")))
(errtest (assv 'x '("string")))
(errtest (assoc 'x '("string")))

(errtest (memq))
(errtest (memv))
(errtest (member))
(errtest (memq 'x))
(errtest (memv 'x))
(errtest (member 'x))
(errtest (memq 'x "string"))
(errtest (memv 'x "string"))
(errtest (member 'x "string"))

;;; Tests of testops

(define (void-cruiser x)
  (if (< x 3) x))

(applytest > 3 * 3 3)
(applytest 'err * 3 "three")
(applytest 2 void-cruiser 2)
(applytest 'void void-cruiser 9)

(evaltest 'err (+ 3 "two"))
(evaltest 'void (if #f 3))

(define (bad-predicate x) (irritant x 'just-because))
(define (bad-relation x y) (irritant (cons x y) 'just-because))

(errtest (evaltest 9 (+ 4 "five")))

(errtest (applytest bad-predicate + 2 3))
(errtest (applytest bad-relation 9 + 2 3))

(errtest (evaltest))

;;; Gather errors

(if (exists? errors)
    (begin (message (choice-size errors)
		    " Errors during MISCTSEST")
	   (error 'tests-failed))
    (message "MISCTEST successfuly completed"))

(test-finished "MISCTEST")


