;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(applytester  '() subseq '(1 2 3) 0 0)
(applytester  '() subseq '(1 2 3) 1 1)
(applytester  "" subseq "123" 1 1)

(applytester  #\e elt "meme" 3)
(applytester  #\é elt "méme" 1)

(define s1 "francais")

(define p1 #X"cdfe33ba9c8833445a")
(applytester  0x33 elt p1 2)
(applytester  #X"33ba" subseq p1 2 4)
(applytester  #X"5a4433889cba33fecd" reverse p1)
(applytester  2 position 51 p1)
(applytester  3 search #X"ba9c" p1)

(applytester 2 position 'foo #({a b} {9 11} {foo bar}))
(applytester 2 position 'bar #({a b} {9 11} {foo bar}))
(applytester 1 position 9 #({a b} {9 11} {foo bar}))
(applytester 1 position 11 #({a b} {9 11} {foo bar}))

(applytest #t some? odd? #(1 2 3 4 5 6))
(applytest #f some? odd? #(2 4 6))
(applytest #t every? even? #(2 4 6))
(applytest #(2 4 6) remove-if odd? #(1 2 3 4 5 6))
(applytest '(2 4 6) remove-if odd? '(1 2 3 4 5 6))
(applytest "CC" remove-if lowercase? "CamelCase")

(evaltest #(4 5 6) (map 1+ #(3 4 5)))
(evaltest {4 5 6} (map->choice 1+ #(3 4 5)))
(evaltest #((1 . 4) (2 . 5) (3 . 6)) (map cons #(1 2 3) #(4 5 6)))

(errtest (map 1+ ))
(errtest (map 1+ 33))
(errtest (map 1+ #(3 4 5) 33))
(errtest (map 1+ #(3 4 5) 33))

(errtest (length 'foo))
(errtest (position 3 'foo))
(errtest (position 3 "345" 'start))
(errtest (position 3 "345" 0 'end))
(errtest (subseq 3 0 9))
(errtest (subseq 'foo 0 9))
(errtest (subseq "foo" 'start))
(errtest (subseq "foo" 0 'end))
(errtest (search 'foo "barfoofuzz"))
(errtest (elt #(1 2 3)))
(errtest (elt))
(errtest (elt 'foo 1))
(errtest (elt 3 1))
(errtest (elts 'foo))
(errtest (elts "foo" 'start))
(errtest (elts "foo" 0 'end))
(evaltest '{a b c} (elts '(a b c . d)))

(applytest 3 position 32 #"abc def")
(applytest 3 rposition 32 #"abc def")
(applytest 17 position #\u00e9  "a nice little café on the left bank")
(applytest 17 rposition #\u00e9  "a nice little café on the left bank")
(applytest 17 search "é" "a nice little café on the left bank")
(applytest 17 rposition #\u00e9  "a nice little café on the left bank")

(applytest #("foo" "bar" "quuz") remove "baz" #("foo" "bar" "baz" "quuz"))
(applytest "abcdegh" remove #\f "abcdefgh")
(applytest '("foo" "bar" "quuz") remove "baz" '("foo" "bar" "baz" "quuz"))
(applytest #"abcdegh" remove (char->integer #\f) #"abcdefgh")

(define short-vec
  (vector 0 127 256 (* 256 16)
	  (1+ (* 256 16)) (1- (* 256 16))
	  127))
(define int-vec
  (vector 0 127 256 (* 256 256)
		(1+ (* 256 256 16))
		(1- (* 256 256 16))
		127))
(define flonum-vec
  (vector 0.1 127 1.0 10.0 1000.0 1000000.53 1000000000000.0
	  .0000000001 127 0.0))

(define (seqtest seq len . pos.vals)
  (applytester len length seq)
  (applytest #t sequence? seq)
  (errtest (elt seq (+ (+ len 10))))
  (errtest (elt seq (+ len 5)))
  (errtest (elt seq (1+ len)))
  (errtest (elt seq (- (1+ len))))
  (let ((scan pos.vals))
    (while (pair? scan)
      (let* ((posdata (car scan)) 
	     (val (cadr scan))
	     (pos (and posdata
		       (if (pair? posdata) (car posdata) posdata)))
	     (rpos (and posdata (pair? posdata) (cdr posdata))))
	(set! scan (cddr scan))
	(when pos
	  (applytest val elt seq pos)
	  (applytest val elt seq (- (- len pos))))
	(applytest (and pos #t) find val seq)
	(applytest pos position val seq)
	(when pos
	  (if rpos
	      (applytest rpos rposition val seq)
	      (applytest pos rposition val seq)))	
	(when (> len 0) (applytest (elt seq 0) (first seq)))
	(when (> len 1) (applytest (elt seq 1) (second seq)))
	(when (> len 2) (applytest (elt seq 2) (third seq))))))
  (dotimes (i len)
    (unless (find (elt seq i) seq
		  (1+ (position (elt seq i) seq)))
      ;; Unless seq[i] is duplicated, removing
      ;;  it will reduce the length by 1
      (applytest (-1+ len) length
		 (remove (elt seq i) seq))))
  (when (and (> len 4) (not (compound? seq)))
    (let ((part (slice seq 2 4)))
      (applytest 2 search part seq)))
  #t)

(seqtest short-vec 7 
	 '(1 . 6) 127
	 3 4096
	 4 4097
	 #f 17
	 5 4095)
(seqtest int-vec 7 
	 '(1 . 6) 127
	 3 65536
	 4 1048577
	 #f 17
	 5 1048575)
(seqtest (->shortvec short-vec) 7 
	 '(1 . 6) 127
	 3 4096
	 4 4097
	 #f 17
	 5 4095)
(seqtest (->intvec short-vec) 7 
	 '(1 . 6) 127
	 3 4096
	 4 4097
	 #f 17
	 5 4095)
(seqtest (->intvec int-vec) 7 
	 '(1 . 6) 127
	 3 65536
	 4 1048577
	 #f 17
	 5 1048575)
(seqtest (->longvec int-vec) 7 
	 '(1 . 6) 127
	 3 65536
	 4 1048577
	 #f 17
	 5 1048575)

(seqtest flonum-vec 10
	 '(1 . 8) 127
	 3 10.0
	 4 1000.0
	 #f 17
	 5 1000000.53
	 9 0.0)
(seqtest (->floatvec flonum-vec) 10
	 '(1 . 8) 127.0
	 3 10.0
	 4 1000.0
	 #f 17
	 ;; We lose precision here
	 5 1000000.50
	 9 0.0)
(seqtest (->doublevec flonum-vec) 10
	 '(1 . 8) 127.0
	 3 10.0
	 4 1000.0
	 #f 17
	 5 1000000.53
	 9 0.0)

(seqtest (sequence->compound (->vector short-vec) 'seqtest) 7 
	 '(1 . 6) 127
	 3 4096
	 4 4097
	 #f 17
	 5 4095)

(applytester #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append #(a b c "d" 3) #(3.1 g h i j k '(l m) n))
(applytester #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append #%(TYPE1 a b c "d" 3) #%(TYPE2 3.1 g h i j k '(l m) n))
(applytester #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append #(a b c "d" 3) '(3.1 g h i j k '(l m) n))
(applytester '(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append '(a b c "d" 3) '(3.1 g h i j k '(l m) n))
(applytester #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   append '(a b c "d" 3) #(3.1 g h i j k '(l m) n))

(applytester #(a b c "d" 3 3.1 g h i j k '(l m) n)
	   reverse #(N '(L M) K J I H G 3.100000 3 "d" C B A))
(applytester '(a b c "d" 3 3.1 g h i j k '(l m) n)
	   reverse '(N '(L M) K J I H G 3.100000 3 "d" C B A))
(applytester "dlrow rorrim" reverse "mirror world")

(applytester #(51 52 53 54 55 56 57) append #(51 52) #"56789")
(applytester #"3456789" append #"34" #"56789")

(applytester '() subseq '() 0 0)

(applytester 3 search '(d e f) '(a b c d e f g h i j))
(applytester 3 search '#(d e f) '#(a b c d e f g h i j))
(applytester 3 search '(d e f) '#(a b c d e f g h i j))
(applytester 3 search '#(d e f) '(a b c d e f g h i j))
(applytester 5 search '("d" "e" "f") '("a" "b" "c" 3 #f "d" "e" "f" "g" "h" "i" "j"))
(applytester 5 search '#("d" "e" "f") '#("a" "b" "c" #f 3 "d" "e" "f" "g" "h" "i" "j"))
(applytester 5 search '#("d" "e" "f") '#%(FOO "a" "b" "c" #f 3 "d" "e" "f" "g" "h" "i" "j"))
(applytester 5 search '("d" "e" "f") '#("a" "b" #t 9 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytester 5 search '#("d" "e" "f") '("a" "b" #f 8 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytester #f search '("b" "c" "d") '("a" "b" "c" 3 #f "d" "e" "f" "g" "h" "i" "j"))
(applytester #f search '#("b" "c" "d") '#("a" "b" "c" #f 3 "d" "e" "f" "g" "h" "i" "j"))
(applytester #f search '("b" "c" "d") '#("a" "b" #t 9 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytester #f search '#("b" "c" "d") '("a" "b" #f 8 "c" "d" "e" "f" "g" "h" "i" "j"))
(applytester 27 search '".." "http://foo.bar.mit.edu/foo/../bar")
(applytester 29 search '".." "http://foo.\u0135bar.mit\u0139.edu/foo/../bar")
(applytester 6 search #"\000\000\000\005" #"foobar\000\000\000\005baz")

(define mixed-list
  '("b\u00c0ar"
    #[foo 3 bar 8] 
    "alpha"  
    "real\vlylongst\aring\nreallylong\nstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstrin\fgreal\rlylon\bgstri\tngreallylongstring"
    #"reallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstring"
    #"ab\003cd\004qnno\377kk\315"
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
     #"ab\003cd\004qnno\377kk\315"
     #t #f #\a () 9739 -3 3.1415 -2.7127
     715919103 -715919103 2415919103 -2415919103 12345678987654321 -12345678987654321
     #"abc def gh\aa jkl"
     #\u45ab {} {A |aBc| "b" C 3} {} {C A 3 "b"} {"A"}
     #((test) "te \" \" st" "" test #() b c)))
(define mixed-compound
  '#%(TESTING
      "b\u00c0ar"
      #[foo 3 bar 8] 
      "alpha" 
      "real\vlylongst\aring\nreallylong\nstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstrin\fgreal\rlylon\bgstri\tngreallylongstring"
      #"reallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstringreallylongstring"
      #"ab\003cd\004qnno\377kk\315"
      #t #f #\a () 9739 -3 3.1415 -2.7127
      715919103 -715919103 2415919103 -2415919103 12345678987654321 -12345678987654321
      #"abc def gh\aa jkl"
      #\u45ab {} {A |aBc| "b" C 3} {} {C A 3 "b"} {"A"}
      #((test) "te \" \" st" "" test #() b c)))

(applytester mixed-list deep-copy mixed-list)
(applytester mixed-vector ->vector mixed-list)
(applytester mixed-vector ->vector mixed-compound)
(applytester mixed-list ->list mixed-vector)
(applytester mixed-list ->list mixed-compound)
(applytester mixed-compound vector->compound mixed-vector 'testing)

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

(applytester 15 reduce + '(1 2 3 4 5))
(applytester 15 reduce + #(1 2 3 4 5))
(applytester 3  reduce - '(1 2 3 4 5))

(applytester '(5 4 3 2 . 1) reduce cons '(1 2 3 4 5))
(applytester '(5 4 3 2 1) reduce cons '(1 2 3 4 5) '())
(applytester '(5 4 3 2 . 1) reduce cons #(1 2 3 4 5))
(applytester '(5 4 3 2 1) reduce cons #(1 2 3 4 5) '())

(applytester "deltagammabetaalpha"
	   reduce string-append
	   '("alpha" "beta" "gamma" "delta"))

(define (square x) (* x x))(define (square x) (* x x))

(applytester 0.0 reduce min (map square '(-2.7 0 0.5 1.8 30.33)))
(applytester 0.0 reduce min
	   (map * '(-2.7 0 0.5 1.8 30.33) '(-2.7 0 0.5 1.8 30.33)))
(applytester 0.0 reduce min (map square '(-2.7 0 0.5 1.8 30.33)))
(applytester -2.7 reduce min '(-2.7 0 0.5 1.8 30.33))
(applytester 30.33 reduce max '(-2.7 0 0.5 1.8 30.33))

(applytester 0.0 reduce min (map square #(-2.7 0 0.5 1.8 30.33)))
(applytester 0.0 reduce min
	   (map * '(-2.7 0 0.5 1.8 30.33) #(-2.7 0 0.5 1.8 30.33)))
(applytester 0.0 reduce min (map square #(-2.7 0 0.5 1.8 30.33)))
(applytester -2.7 reduce min #(-2.7 0.0 0.5 1.8 30.33))
(applytester 30.33 reduce max #(-2.7 0.0 0.5 1.8 30.33))

;;; Length compare tests

(applytester #t length> "alpha" 3)
(applytester #f length> "ab" 3)
(applytester #t length< "ab" 3)
(applytester #f length< "alpha" 3)
(applytester #t length= "alpha" 5)
(applytester #f length= "abc" 5)

(applytester #t length>0 "beta")
(applytester #t length>0 "a")
(applytester #f length>0 "")

(applytester #t length>1 "beta")
(applytester #t length>1 "az")
(applytester #f length>1 "a")
(applytester #f length>1 "")

;;; Packet parsing

(applytester #"foobar889" string->packet "foobar889")
(applytester #t equal? #"foobar889" #X"666F6F626172383839")
(applytester #t equal? #"foobar889" #x"666F6F626172383839")
(applytester #t equal? #"foobar889" #x"666f6f626172383839")
(applytester "Zm9vYmFyODg5" packet->base64  #"foobar889")
(applytester "666F6F626172383839" packet->base16  #"foobar889")
(applytester #t equal? #"foobar889" #B"Zm9vYmFyODg5")
(applytester #t equal? #"foobar889" #B"Zm9vYmFyODg5")

(applytester  #X"666F6F626172383839" parser/roundtrip #"foobar889")
(applytester  #X"666F6F626172383839" dtype/roundtrip #"foobar889")
(applytester "#*\"9:d927ea9799dc294722671dc3f73dcd0e\""
	     lisp->string #*"foobar889")

(define (parsefail string)
  (onerror (begin (string->lisp 
		   (string-subst* string
		     "\&ldquo;" "\""  "\&rdquo;" "\""))
	     #f)
    (lambda (x) #t)))

;; Check that parsing fails, too
(applytester #f parsefail "#X“33ff”")
(applytester #t parsefail "#X“3z3ff”")
(applytester #f parsefail "#B“M/8=”")
(applytester #t parsefail "#B“M/_8=”")
(applytester #t parsefail "#B“_M/8=”")

;;; Secret operations

(define asecret #*"keepitsafe")

(applytest-pred secret? glom "thering" asecret)
(applytest-pred secret? glom asecret "thering")
(applytest-pred secret? slice asecret 0 2)
(applytest 10 length asecret)

(errtest (elt asecret 3))
(errtest (position (char->integer #\k) asecret))
(errtest (rposition (char->integer #\k) asecret))
(errtest (search #"xx" asecret))
(errtest (search #*"xx" #"abc"))
(errtest (rposition (char->integer #\k) asecret))
(errtest (elts asecret))

;;; FORSEQ

(evaltest #(2 3 4) (forseq (e #(1 2 3)) (+ e 1)))
(errtest (forseq))
(errtest (forseq ("x" '(a b))))
(errtest (forseq (x 3)))
(errtest (forseq (x #(1 2 3)) (if (even? x) (irritant x "EvenValue") x)))

(evaltest '() (forseq (x '()) (if (even? x) (irritant x "EvenValue") x)))
(evaltest #() (forseq (x #()) (if (even? x) (irritant x "EvenValue") x)))
(evaltest "" (forseq (x "") (if (even? x) (irritant x "EvenValue") x)))
(evaltest #"" (forseq (x #"") (if (even? x) (irritant x "EvenValue") x)))

;;; Some simple test for non vector compounds

(define not-sequence (make-opaque-compound 'noelts 1 2 3 4))
(evaltest #f (onerror (elt not-sequence 1) #f))

;;; Check the if/if-not methods

(define (even-string? s) (even? (string->number s)))
(define (odd-string? s) (odd? (string->number s)))

(define vec #("1" "2" "3" "4" "5" "6" "7"))
(define vec2 #("a" "b" "c" "d" "e" "f"))
(define lst #("1" "2" "3" "4" "5" "6" "7"))
(define lst2 #("a" "b" "c" "d" "e" "f"))

(errtest (position-if))
(errtest (position-if even?))
(errtest (position-if-not))
(errtest (position-if-not even?))

(begin
  (applytest 1 position-if even-string? vec)
  (applytest 0 position-if odd-string? vec)
  (applytest 0 position-if-not even-string? vec)
  (applytest 1 position-if-not odd-string? vec)
  (applytest #f position-if even-string? vec2)
  (applytest #f position-if odd-string? vec2)

  (applytest 3 position-if even-string? vec 2)
  (applytest 2 position-if odd-string? vec 2)
  (applytest 2 position-if-not even-string? vec 2)
  (applytest 3 position-if-not odd-string? vec 2)
  (applytest #f position-if even-string? vec2)
  (applytest #f position-if odd-string? vec2)

  (applytest #f position-if even-string? vec 2 2)
  (applytest #f position-if even-string? vec 2 3)
  (applytest 2 position-if odd-string? vec 2 3)
  (applytest #f position-if even-string? vec2 1 3)
  (applytest #f position-if odd-string? vec2 1 3)

  (applytest 5 position-if even-string? vec -1 0)
  (applytest 6 position-if odd-string? vec -1 0)
  (applytest 6 position-if-not even-string? vec -1 0)
  (applytest 5 position-if-not odd-string? vec -1 0)
  (applytest #f position-if even-string? vec2 -1 0)
  (applytest #f position-if odd-string? vec2 -1 0)

  (applytest "2" find-if even-string? vec)
  (applytest "1" find-if odd-string? vec)
  (applytest "1" find-if-not even-string? vec)
  (applytest "2" find-if-not odd-string? vec)
  (applytest #f find-if even-string? vec2)
  (applytest #f find-if odd-string? vec2)
  )

(begin
  (applytest 1 position-if even-string? lst)
  (applytest 0 position-if odd-string? lst)
  (applytest 0 position-if-not even-string? lst)
  (applytest 1 position-if-not odd-string? lst)
  (applytest #f position-if even-string? lst2)
  (applytest #f position-if odd-string? lst2)

  (applytest 3 position-if even-string? lst 2)
  (applytest 2 position-if odd-string? lst 2)
  (applytest 2 position-if-not even-string? lst 2)
  (applytest 3 position-if-not odd-string? lst 2)
  (applytest #f position-if even-string? lst2)
  (applytest #f position-if odd-string? lst2)

  (applytest #f position-if even-string? lst 2 2)
  (applytest #f position-if even-string? lst 2 3)
  (applytest 2 position-if odd-string? lst 2 3)
  (applytest #f position-if even-string? lst2 1 3)
  (applytest #f position-if odd-string? lst2 1 3)

  (applytest 5 position-if even-string? lst -1 0)
  (applytest 6 position-if odd-string? lst -1 0)
  (applytest 6 position-if-not even-string? lst -1 0)
  (applytest 5 position-if-not odd-string? lst -1 0)
  (applytest #f position-if even-string? lst2 -1 0)
  (applytest #f position-if odd-string? lst2 -1 0)

  (applytest "2" find-if even-string? lst)
  (applytest "1" find-if odd-string? lst)
  (applytest "1" find-if-not even-string? lst)
  (applytest "2" find-if-not odd-string? lst)
  (applytest #f find-if even-string? lst2)
  (applytest #f find-if odd-string? lst2)
  )

(errtest (find-if))
(errtest (find-if even?))

(applytest "4" find-if even-string? vec 2)
(applytest "3" find-if odd-string? vec 2)
(applytest "3" find-if-not even-string? vec 2)
(applytest "4" find-if-not odd-string? vec 2)
(applytest #f find-if even-string? vec2)
(applytest #f find-if odd-string? vec2)

(applytest #f find-if even-string? vec 2 2)

(applytest #f find-if even-string? vec 2 3)
(applytest "3" find-if odd-string? vec 2 3)
(applytest #f find-if even-string? vec2 1 3)
(applytest #f find-if odd-string? vec2 1 3)

(applytest "6" find-if even-string? vec -1 0)
(applytest "7" find-if odd-string? vec -1 0)
(applytest "7" find-if-not even-string? vec -1 0)
(applytest "6" find-if-not odd-string? vec -1 0)
(applytest #f find-if even-string? vec2 -1 0)
(applytest #f find-if odd-string? vec2 -1 0)

;;; Pair tests

(define (xseqtest seq (numeric #f))
  (when numeric
    (applytest #t every? number? seq)
    (applytest #t some? number? seq)
    (applytest-pred sequence? map 1+ seq)
    (applytest 'void for-each 1+ seq)
    (evaltest (largest (elts seq)) (apply max (->list seq)))
    (evaltest (smallest (elts seq)) (apply min (->list seq))))
  (applytest-pred string? lisp->string seq)
  (applytest (elt seq 0) first seq)
  (applytest (elt seq 1) second seq)
  (applytest (elt seq 2) third seq)
  (applytest (elt seq 3) fourth seq)
  (applytest (elt seq 4) fifth seq)
  (applytest (elt seq 5) sixth seq)
  (applytest (elt seq 6) seventh seq)
  (unless (compound? seq)
    (applytest (slice seq 1) rest seq)
    (errtest (first (slice seq 0 0)))
    (errtest (second (slice seq 0 1)))
    (errtest (third (slice seq 0 2)))
    (errtest (fourth (slice seq 0 3)))
    (errtest (fifth (slice seq 0 4)))
    (errtest (sixth (slice seq 0 5)))
    (errtest (seventh (slice seq 0 6))))
  (applytest-pred ambiguous? elts seq))

(define vec #("three" "four" "five" "six" "seven" "eight" "nine" 
	      "ten" "eleven" "twelve" "thirteen"))

(applytest  {"three" "four" "five" "six" "seven" "eight" "nine" 
	     "ten" "eleven" "twelve" "thirteen"}
	    vector->elts 
	    vec)
(xseqtest vec)
(xseqtest (sequence->compound vec 'xseqtest))
(xseqtest (->list vec))
(xseqtest "abcdefghijklmnopqrs")

(applytest #t veclen>=? vec 3)
(applytest #f veclen<=? vec 3)
(applytest #t veclen=? vec (length vec))
(applytest #f veclen>? vec 20)
(applytest #f veclen>=? vec 20)

(applytest #t seqlen>=? vec 3)
(applytest #f seqlen<=? vec 3)
(applytest #t seqlen=? vec (length vec))
(applytest #f seqlen>? vec 20)
(applytest #f seqlen>=? vec 20)

(define numvec #(1 11 2 4 5 3 10 6 7 13 8 2 9 10 12))

(xseqtest numvec #t)
(xseqtest (->shortvec numvec) #t)
(xseqtest (->intvec numvec) #t)
(xseqtest (->longvec numvec) #t)
(xseqtest (->floatvec numvec) #t)
(xseqtest (->doublevec numvec) #t)

(xseqtest (string->packet "abcdefghijklmnopqrs"))

(define (sort-test seq)
  (let ((increasing (sortvec seq))
	(decreasing (rsortvec seq))
	(len (length seq)))
    (applytest decreasing reverse increasing)
    (doseq (e increasing i)
      (when (> i 0) 
	(applytest #t >= e (elt increasing (-1+ i)))))
    (doseq (e decreasing i)
      (when (> i 0)
	(applytest #t <= e (elt decreasing (-1+ i)))))))
(sort-test numvec)

(applytest "foo" cdar '((a . "foo") (b . "bar")))
(errtest (caar '(a b)))
(errtest (caddr '(a b)))
(errtest (cdddr '(a b)))
(errtest (cadr '(a . b)))
(errtest (cddr '(a . b)))

(let ((lst '("foo" "bar" "baz")))
  (applytest "foo" car lst)
  (applytest "bar" cadr lst)
  (applytest "baz" caddr lst)
  (set-car! (cddr lst) "quux")
  (applytest "quux" caddr lst)
  (applytest 3 length lst)
  (set-cdr! (cdr lst) '())
  (applytest 2 length lst))

(errtest (make-vector -5 "backward"))

(test-finished "SEQUENCES")

