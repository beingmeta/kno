;;; -*- Mode: scheme; text-encoding: latin-1; -*-

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
(applytest 27 search '".." "http://foo.bar.mit.edu/foo/../bar")
(applytest 3 search '#(d e f) '#(a b c d e f g h i j))
(applytest 29 search '".." "http://foo.\u0135bar.mit\u0139.edu/foo/../bar")
(applytest 6 search #"\00\00\00\05" #"foobar\00\00\00\05baz")

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

(message "SEQTEST successfuly completed")
