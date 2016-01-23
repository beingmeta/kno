;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(applytest  '() subseq '(1 2 3) 0 0)
(applytest  '() subseq '(1 2 3) 1 1)
(applytest  "" subseq "123" 1 1)

(applytest  #\e elt "meme" 3)
(applytest  #\é elt "méme" 1)

(define s1 "francais")

(define p1 #"\cd\fe\33\ba\9c\88\33\44\5a")
(applytest  0x33 elt p1 2)
(applytest  #"\33\ba" subseq p1 2 4)
(applytest  #"\5a\44\33\88\9c\ba\33\fe\cd" reverse p1)
(applytest  2 position 51 p1)
(applytest  3 search #"\ba\9c" p1)

(define short-vec
  (vector 0 127 256 (* 256 16) (1+ (* 256 16)) (1- (* 256 16))))
(define int-vec
  (vector 0 127 256 (* 256 256)
		(1+ (* 256 256 16)) (1- (* 256 256 16))))
(define float-vec
  (vector 0.1 1.0 10.0 1000.0 1000000.53 1000000000000.0
	  .0000000001 0.0))
(define double-vec
  (vector 0.1 1.0 10.0 1000.0 1000000.53 1000000000000000000.0
	  .000000000000000001 0.0))

(applytest 6 length short-vec)
(applytest #t find 4096 short-vec)
(applytest #f find 8192 short-vec)
(applytest #f find 1048578 short-vec)
(applytest 3 position 4096 short-vec)
(applytest #f position 17 short-vec)
(applytest 4095 elt short-vec 5)
(applytest 2 search (vector 256 4096) short-vec)
(applytest 2 search (vector 256 4096) short-vec)
(applytest 2 search (vector 256 4096) short-vec)

(applytest 6 length int-vec)
(applytest 3 position 65536 int-vec)
(applytest #f position 17 int-vec)
(applytest 1048575 elt int-vec 5)
(applytest 3 search (vector 65536 1048577) int-vec)
(applytest 3 search (vector 65536 1048577) int-vec)
(applytest 1 search (vector 127 256) int-vec)

(applytest 8 length float-vec)
(applytest 4 position 1000000.53 float-vec)
(applytest #f position 1000000.54 float-vec)
(applytest 0.000000000000000001 elt double-vec 6)
(applytest 1 search (vector 1.0 10.0) float-vec)
(applytest 1 search (vector 1.0 10.0) float-vec)
(applytest 1 search (vector 1.0 10.0) float-vec)

(applytest 8 length double-vec)
(applytest 4 position 1000000.53 double-vec)
(applytest #f position 1000000.54 double-vec)
(applytest .000000000000000001 elt double-vec 6)
(applytest 1 search (vector 1.0 10.0) double-vec)
(applytest 1 search (vector 1.0 10.0) float-vec)
(applytest 1 search (vector 1.0 10.0) float-vec)

(applytest #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append #(a b c "d" 3) #(3.1 g h i j k '(l m) n))
(applytest #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append #(a b c "d" 3) '(3.1 g h i j k '(l m) n))
(applytest '(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append '(a b c "d" 3) '(3.1 g h i j k '(l m) n))
(applytest #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append '(a b c "d" 3) #(3.1 g h i j k '(l m) n))

(applytest #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   reverse #(N '(L M) K J I H G 3.100000 3 "d" C B A))
(applytest '(a b c "d" 3 3.1 g h i j k '(l m) n)
	   reverse '(N '(L M) K J I H G 3.100000 3 "d" C B A))
(applytest "dlrow rorrim" reverse "mirror world")

(applytest #(51 52 53 54 55 56 57) append #(51 52) #"56789")
(applytest #"3456789" append #"34" #"56789")

(applytest '() subseq '() 0 0)

(applytest 3 search '(d e f) '(a b c d e f g h i j))
(applytest 3 search '#(d e f) '#(a b c d e f g h i j))
(applytest 3 search '(d e f) '#(a b c d e f g h i j))
(applytest 3 search '#(d e f) '(a b c d e f g h i j))
(applytest 5 search '("d" "e" "f") '("a" "b" "c" 3 #f "d" "e" "f" "g" "h" "i" "j"))
(applytest 5 search '#("d" "e" "f") '#("a" "b" "c" #f 3 "d" "e" "f" "g" "h" "i" "j"))
(applytest 5 search '("d" "e" "f") '#("a" "b" #t 9 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytest 5 search '#("d" "e" "f") '("a" "b" #f 8 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytest #f search '("b" "c" "d") '("a" "b" "c" 3 #f "d" "e" "f" "g" "h" "i" "j"))
(applytest #f search '#("b" "c" "d") '#("a" "b" "c" #f 3 "d" "e" "f" "g" "h" "i" "j"))
(applytest #f search '("b" "c" "d") '#("a" "b" #t 9 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytest #f search '#("b" "c" "d") '("a" "b" #f 8 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytest 27 search '".." "http://foo.bar.mit.edu/foo/../bar")
(applytest 29 search '".." "http://foo.\u0135bar.mit\u0139.edu/foo/../bar")
(applytest 6 search #"\00\00\00\05" #"foobar\00\00\00\05baz")

(define mixed-list
  '("b\u00c0ar"
    #[foo 3 bar 8] 
    "alpha"  
    "real\vlylongst\aring\nreallylong\nstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstrin\fgreal\rlylon\bgstri\tngreallylongstring"
    #"reallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstring"
    #"ab\03cd\04qnno\ffkk\cd"
    #t #f #\a () 9739 -3 3.1415 -2.7127
    715919103 -715919103 2415919103 -2415919103 12345678987654321 -12345678987654321
    #"abc def gh\aa jkl"
    #\u45ab {} {A |aBc| "b" C 3} {} {C A 3 "b"} {"A"}
    #((test) "te \" \" st" "" test #() b c)))
(define mixed-vector
  '#("b\u00c0ar"
     #[foo 3 bar 8] 
     "alpha" 
     "real\vlylongst\aring\nreallylong\nstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstrin\fgreal\rlylon\bgstri\tngreallylongstring"
     #"reallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstring"
     #"ab\03cd\04qnno\ffkk\cd"
     #t #f #\a () 9739 -3 3.1415 -2.7127
     715919103 -715919103 2415919103 -2415919103 12345678987654321 -12345678987654321
     #"abc def gh\aa jkl"
     #\u45ab {} {A |aBc| "b" C 3} {} {C A 3 "b"} {"A"}
     #((test) "te \" \" st" "" test #() b c)))

(applytest mixed-list deep-copy mixed-list)
(applytest mixed-vector ->vector mixed-list)
(applytest mixed-list ->list mixed-vector)

(define numsum 9742.000000)
(evaltest numsum
	  (let ((sum 0))
	    (dolist (elt mixed-list)
	      (if (singleton? elt)
		  (if (number? elt) (set! sum (+ sum elt)))
		  (do-choices (e elt)
		    (if (number? e) (set! sum (+ sum e))))))
	    sum))
(evaltest numsum
	  (let ((sum 0))
	    (doseq (elt mixed-list)
	      (if (= (choice-size elt) 1)
		  (if (number? elt) (set! sum (+ sum elt)))
		  (do-choices (e elt)
		    (if (number? e) (set! sum (+ sum e))))))
	    sum))
(evaltest numsum
	  (let ((sum 0))
	    (doseq (elt mixed-vector)
	      (unless (fail? elt)
		(if (ambiguous? elt)
		    (do-choices (e elt)
		      (if (number? e) (set! sum (+ sum e))))
		    (if (number? elt) (set! sum (+ sum elt))))))
	    sum))
(define stringsum
  "bÀar—alpha—real\vlylongst\aring\nreallylong\nstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstrin\fgreal\rlylon\010gstri\tngreallylongstring—b—b—A")

(evaltest stringsum
	  (let ((string ""))
	    (dolist (elt mixed-list i)
	      (if (ambiguous? elt)
		  (do-choices elt
		    (when (string? elt)
		      (set! string (glom string (and (> i 0) "\&mdash;") elt))))
		  (when (and (exists? elt) (string? elt))
		    (set! string (glom string (and (> i 0) "\&mdash;") elt)))))
	    string))
(evaltest stringsum
	  (let ((string ""))
	    (dolist (elt mixed-list)
	      (unless (fail? elt)
		(do-choices elt
		  (when (string? elt)
		    (set! string (if (empty-string? string) elt
				     (glom string "\&mdash;" elt)))))))
	    string))
(evaltest stringsum
	  (let ((string ""))
	    (doseq (elt mixed-list)
	      (when (exists? elt)
		(if (ambiguous? elt)
		    (do-choices elt
		      (when (string? elt)
			(set! string (glom string (and (not (empty-string? string)) "\&mdash;")
				       elt))))
		    (when (string? elt)
		      (set! string (glom string (and (not (empty-string? string)) "\&mdash;")
				     elt))))))
	    string))
(evaltest stringsum
	  (let ((string ""))
	    (doseq (elt mixed-vector)
	      (unless (fail? elt)
		(do-choices elt
		  (when (string? elt)
		    (set! string (if (empty-string? string) elt
				     (glom string "\&mdash;" elt)))))))
	    string))
(evaltest stringsum
	  (let ((string ""))
	    (doseq (elt (->vector mixed-list))
	      (when (exists? elt)
		(if (ambiguous? elt)
		    (do-choices elt
		      (when (string? elt)
			(set! string (glom string (and (not (empty-string? string)) "\&mdash;")
				       elt))))
		    (when (string? elt)
		      (set! string (glom string (and (not (empty-string? string)) "\&mdash;")
				     elt))))))
	    string))
(evaltest stringsum
	  (let ((string ""))
	    (doseq (elt (->vector mixed-vector))
	      (when (exists? elt)
		(if (ambiguous? elt)
		    (do-choices elt
		      (when (string? elt)
			(set! string (glom string (and (not (empty-string? string)) "\&mdash;")
				       elt))))
		    (when (string? elt)
		      (set! string (glom string (and (not (empty-string? string)) "\&mdash;")
				     elt))))))
	    string))
(evaltest stringsum
	  (let ((string ""))
	    (doseq (elt (->vector mixed-list))
	      (unless (fail? elt)
		(do-choices elt
		  (when (string? elt)
		    (set! string (if (empty-string? string) elt
				     (glom string "\&mdash;" elt)))))))
	    string))

;;; Reduce tests

(applytest 15 reduce + '(1 2 3 4 5))
(applytest 15 reduce + #(1 2 3 4 5))
(applytest 3  reduce - '(1 2 3 4 5))

(applytest '(5 4 3 2 . 1) reduce cons '(1 2 3 4 5))
(applytest '(5 4 3 2 1) reduce cons '(1 2 3 4 5) '())
(applytest '(5 4 3 2 . 1) reduce cons #(1 2 3 4 5))
(applytest '(5 4 3 2 1) reduce cons #(1 2 3 4 5) '())

(applytest "deltagammabetaalpha"
	   reduce string-append
	   '("alpha" "beta" "gamma" "delta"))

(define (square x) (* x x))(define (square x) (* x x))

(applytest 0.0 reduce min (map square '(-2.7 0 0.5 1.8 30.33)))
(applytest 0.0 reduce min
	   (map * '(-2.7 0 0.5 1.8 30.33) '(-2.7 0 0.5 1.8 30.33)))
(applytest 0.0 reduce min (map square '(-2.7 0 0.5 1.8 30.33)))
(applytest -2.7 reduce min '(-2.7 0 0.5 1.8 30.33))
(applytest 30.33 reduce max '(-2.7 0 0.5 1.8 30.33))

(applytest 0.0 reduce min (map square #(-2.7 0 0.5 1.8 30.33)))
(applytest 0.0 reduce min
	   (map * '(-2.7 0 0.5 1.8 30.33) #(-2.7 0 0.5 1.8 30.33)))
(applytest 0.0 reduce min (map square #(-2.7 0 0.5 1.8 30.33)))
(applytest -2.7 reduce min #(-2.7 0.0 0.5 1.8 30.33))
(applytest 30.33 reduce max #(-2.7 0.0 0.5 1.8 30.33))

;;; Length compare tests

(applytest #t length> "alpha" 3)
(applytest #f length> "ab" 3)
(applytest #t length< "ab" 3)
(applytest #f length< "alpha" 3)
(applytest #t length= "alpha" 5)
(applytest #f length= "abc" 5)

(applytest #t length>0 "beta")
(applytest #t length>0 "a")
(applytest #f length>0 "")

(applytest #t length>1 "beta")
(applytest #t length>1 "az")
(applytest #f length>1 "a")
(applytest #f length>1 "")

;;; Packet parsing

(applytest #"foobar889" string->packet "foobar889")
(applytest #t equal? #"foobar889" #X"666F6F626172383839")
(applytest #t equal? #"foobar889" #x"666F6F626172383839")
(applytest #t equal? #"foobar889" #x"666f6f626172383839")
(applytest "Zm9vYmFyODg5" packet->base64  #"foobar889")
(applytest "666F6F626172383839" packet->base16  #"foobar889")
(applytest #t equal? #"foobar889" #X@"Zm9vYmFyODg5")
(applytest #t equal? #"foobar889" #x@"Zm9vYmFyODg5")

(define (parsefail string)
  (onerror (begin (string->lisp 
		   (string-subst* string
		     "\&ldquo;" "\""  "\&rdquo;" "\""))
	     #f)
    (lambda (x) #t)))

;; Check that parsing fails, too
(applytest #f parsefail "#X“33ff”")
(applytest #t parsefail "#X“3z3ff”")
(applytest #f parsefail "#X@“M/8=”")
(applytest #t parsefail "#X@“M/_8=”")
(applytest #t parsefail "#X@“_M/8=”")

(message "SEQTEST successfuly completed")
