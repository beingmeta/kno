;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(applytest 2 elt (shortvec 3 2 1) 1)
(applytest 3 length (shortvec 3 2 1))

(applytest 2 elt (intvec 3 2 1) 1)
(applytest 3 length (intvec 3 2 1))

(applytest 2 elt (longvec 3 2 1) 1)
(applytest 3 length (longvec 3 2 1))

(applytest 2.0 elt (doublevec 3 2 1) 1)
(applytest 3 length (doublevec 3 2 1))

(applytest 2.0 elt (floatvec 3 2 1) 1)
(applytest 3 length (floatvec 3 2 1))

(define (v+ x y) (->vector (+ x y)))
(define (v* x y) (->vector (* x y)))
(define (v⋅ x y) (* x y))

(applytest #(11 13 16) v+ (shortvec 3 4 6) (longvec 8 9 10))
(applytest #(11 13 16) v+ (intvec 3 4 6) (longvec 8 9 10))
(applytest #(11.0 13.0 16.0) v+ (shortvec 3 4 6) (floatvec 8 9 10))
(applytest #(11.0 13.0 16.0) v+ (intvec 3 4 6) (doublevec 8 9 10))
(applytest #(11.0 13.0 16.0) v+ (doublevec 3 4 6) (doublevec 8 9 10))

(applytest #(9 12 18) v* (longvec 3 4 6) 3)
(applytest #(9.0 12.0 18.0) v* (doublevec 3 4 6) 3)
(applytest #(9.0 12.0 18.0) v* (longvec 3 4 6) 3.0)
(applytest #(9.0 12.0 18.0) v* (vector 3 4 6) 3.0)
(applytest #(9.0 12.0 18.0) v* (vector 3.0 4.0 6.0) 3)
(applytest #(9 12 18) v* (vector 3 4 6) 3)

(applytest 120.0 v⋅ (intvec 3 4 6) (vector 8.0 9.0 10.0))
(applytest 120 v⋅ (intvec 3 4 6) (shortvec 8 9 10))
(applytest 120 v⋅ (intvec 3 4 6) (vector 8 9 10))
(applytest 120 v⋅ (vector 8 9 10) (intvec 3 4 6))
(applytest 120.0 v⋅ (vector 8 9 10) (floatvec 3 4 6))
(applytest 120.0 v⋅ (floatvec 3 4 6) (vector 8 9 10))

(evaltest 'error
	  (onerror (+ (intvec 1 2 3) (intvec 1 2 3 4))
	    (lambda (ex) 'error)))
(evaltest 'error
	  (onerror (v⋅ (intvec 1 2 3) (intvec 1 2 3 4))
	    (lambda (ex) 'error)))
(evaltest 'error
	  (onerror (v⋅ (floatvec 1 2 3) (intvec 1 2 3 4))
	    (lambda (ex) 'error)))

(applytest #(5 7 9) + #(1 2 3) #(4 5 6))
(applytest #(-3 -3 -3) - #(1 2 3) #(4 5 6))
(applytest 32 * #(1 2 3) #(4 5 6))

(do-choices (convert {->longvec ->shortvec ->intvec ->floatvec ->doublevec})
  (applytest (convert #(5 7 9)) + (convert #(1 2 3)) (convert #(4 5 6)))
  (applytest (convert #(-3 -3 -3)) - (convert #(1 2 3)) (convert #(4 5 6))))

(applytest #t inexact? 3.3)
(applytest #f inexact? 3)
(applytest #f exact? 3.3)
(applytest #t exact? 3)
(applytest #f exact? 0.333333)
(applytest #t exact? 1/3)

(applytest 3 * 3)
(applytest 3.0 * 3.0)
(applytest 3+5i * 3+5i)
(applytest 3.0+5i * 3.0+5i)

(applytest 3 + 3)
(applytest 3.0 + 3.0)
(applytest 3+5i + 3+5i)
(applytest 3.0+5i + 3.0+5i)

(applytest -3 - 3)
(applytest -3.0 - 3.0)
(applytest -3-5i - 3+5i)
(applytest -3.0-5i - 3.0+5i)

(applytest #t complex? 3.0+5i)
(applytest #t complex? 3)
(applytest #t complex? 0.33)
(applytest #f complex? 'symbol)
(applytest #f complex? "string")

(applytest #t real? 3)
(applytest #t real? 3.33)
(applytest #t real? 1/3)
(applytest #f real? 1/3+9i)

(applytest #t rational? 1/3)
(applytest #f rational? 1/3+5i)
(applytest #t rational? 8)
(applytest #f rational? 0.333)

(applytest #(4 11) + #(1 5) #(2 5) #(1 1))
(applytest 33 + 11 11 11)
(applytest 33.0 + 11.0 11 11)
(applytest 1331 * 11 11 11)
(applytest 1331.0 * 11 11.0 11)

(applytest 50 - 100 15 25 10)
(applytest 50.0 - 100 15.0 25 10)

(applytest #(10 16) * #(10 16) )
(applytester 'err * #"packet")
(applytest #(100 80) * 5 #(20 16) )
(applytest #(100.0 80.0) * 5.0 #(20 16) )
(applytest #(100.0 80) * 5 #(20.0 16) )

(define onebig
  (* (* 4 1024 1024 1024)  (* 4 1024 1024 1024)))

(applytest inexact? + 3 4 4.5 onebig)
(applytest inexact? - 100 15.0 25 onebig)
(applytest inexact? random 200.0)
(applytest exact? random (* 1024 1024 4))
(when fix61
  (applytest exact? random (* 1024 1024 1024 64)))

(applytest '{(420 . 10) (422 . 2) (425 . 5)} scalerep 423 {10 5 2})
(applytest '(4230 . -10) scalerep 423.0 -10)

(applytest #(6 8 10) + #(3 4 5) #(3 4 5))
(applytest 'err + #(3 4 5) #(3 4 5) 25)
(applytest 'err + 11 12 13 4 5 "six" 25)

(applytest #(0 0 0) - #(3 4 5) #(3 4 5))
(applytest 'err - #(3 4 5) #(3 4 5) 25)
(applytest 'err - 11 12 13 4 5 "six" 25)

(applytest #(1 2 3) * 1 #(1 2 3))
(applytest (->shortvec #(1 2 3)) * 1 (->shortvec #(1 2 3)))
(applytest 'err * "one" 8)

(applytest #t number? onebig)

(applytest #t bignum? onebig)
(applytest #f bignum? 33)
(applytest #f bignum? 1/3)
(applytest #f bignum? 3.33)
(applytest #f bignum? 3.33+5i)

(applytest #t even? onebig)
(applytest #f odd? onebig)
(applytest #f even? (1+ onebig))
(applytest #f even? (+ onebig 7))
(applytest #f even? (1+ onebig))

(applytest #t odd? (+ onebig 7))

(applytest 3 inexact->exact 3.2)
(applytest 3 inexact->exact 3.9)
(applytest -3 inexact->exact -3.9)

(applytest inexact? * 3.0 5 0.0)

(applytest "3.50" inexact->string 3.5)
(applytest "3.50000" inexact->string 3.5 5)
(applytest "3/2" inexact->string 3/2)
(applytest "17" u8itoa 17)
(applytest "11" u8itoa 17 16)
(applytest 'err u8itoa 17 5)

(applytest 9 ilog 260)
(applytest 3 ilog 260 10)

(applytest 2664677276096687168 wang-hash32 424238335)
(applytest 13006425073790605821 wang-hash64 144825285711317)
(applytest #X"5e3d250d0f8b0a12f97781b8da7307c8" cityhash128 "this is one sentence")
(applytest #X"d655a3c206f2a01b" cityhash64 "this is one sentence")
(applytest 9223372036854775808 flip64 128)
(applytest 0x8000000000000000 flip64 128)

(applytest 4 ->exact 3.9 1)
(applytest 3 ->exact 3.9 -1)
(applytest 4 ->exact 3.9 0)
(applytest 3 ->exact 3.2 0)
(applytest 4 ->exact 3.5 0)

(applytest 2.236068 sqrt 5.000000)
(applytest 2.236068 lsqrt 5.000000)
(applytest -0.958924 sin 5.000000)
(applytest 0.283662 cos 5.000000)
(applytest 1.373401 atan 5.000000)
(applytest -3.380515 tan 5.000000)
(applytest 1.609438 log 5.000000)
(applytest 148.413159 exp 5.000000)
(applytest 1.047198 acos 0.5)
(applytest 0.523599 asin 0.5)

(let ((rat (make-rational 2 6)))
  (applytest 1 numerator rat)
  (applytest 3 denominator rat)
  (applytest #t = 1/3 rat)
  (applytest #t exact? rat)
  (applytest 0.33333333 * rat 1.0))
(applytest 3 numerator 3)
(applytest 1 denominator 3)

(let ((complex (make-complex 3 5)))
  (applytest #t = 3+5i complex)
  (applytest 3 real-part complex)
  (applytest 5 imag-part complex)
  (applytest 3 - complex 5i))
(applytest 3 real-part 3)
(applytest 0 imag-part 3)
(applytest 3.0 real-part 3.0)
(applytest 0 imag-part 3.0)
(let ((complex (make-complex 3 5.0)))
  (applytest #t = 3+5.0i complex)
  (applytest 3 real-part complex)
  (applytest 5.0 imag-part complex)
  (applytest complex? - complex 5.0i))

(applytest 'err real-part #"dream")
(applytest 'err imag-part #"dream")
(applytest 'err numerator #"dream")
(applytest 'err denominator #"dream")

(applytest flonum? exact->inexact onebig)
(applytest flonum? /~ onebig)
(applytest flonum? /~ 1 onebig)
(applytest flonum? * onebig 1.0)
(applytest flonum? * onebig 1.0)
(applytest flonum? + onebig 1.0)
(applytest flonum? - onebig 1.0)

(applytest 'err /~ 3+9i)
(applytest 3.000000 /~ 0.3333333)
(applytest 1.333333 /~ 0.3333333 0.25)

(applytest 1.0 * onebig (/~ onebig))

(applytest 4.0 -1+ 5.0)
(applytest 4.0+9i -1+ 5.0+9i)
(applytest 3/5 -1+ 8/5)

(applytest 4+9i ->exact 4.0+9i)

(applytest (/~ 5) /~ 1 5)
(applytest (/~ 10) /~ 2 20)

(applytest flonum? ->flonum 1/3)

(applytest 3.0+5.0i ->inexact 3+5i)
(errtest (->flonum 3+5i))

(applytest 9 pow 3 2)
(applytest 1 pow 3 0)
(applytest (* onebig onebig) pow onebig 2)
(applytest 9.0 pow 3.0 2)
(applytest 125.0 pow~ 5 3)
(applytest 0.124355 atan2 0.5 4)

(applytest 3 nthroot 9 2)
(applytest 3 nthroot 27 3)
(applytest 3.0 nthroot 27.0 3)
(applytest 2.0 nthroot 4 2.0)

(applytest (* 1024 1024 1024 4)
	   nthroot 
	   (*  (* 1024 1024 1024 4) (* 1024 1024 1024 4) )
	   2)

(applytest 0 modulo (*  (* 1024 1024 1024 4) (* 1024 1024 1024 4) ) 2)
(applytest 1 modulo (1+ (* (* 1024 1024 1024 4) (* 1024 1024 1024 4) )) 2)
(applytest -1 modulo (- (1+ (* (* 1024 1024 1024 4) (* 1024 1024 1024 4) ))) -2)
(applytest -1 modulo (1+ (* (* 1024 1024 1024 4) (* 1024 1024 1024 4) )) -2)
(applytest 1 modulo (- (1+ (* (* 1024 1024 1024 4) (* 1024 1024 1024 4) ))) 2)
(applytest 0 modulo 0 (+ (*  (* 1024 1024 1024 4) (* 1024 1024 1024 4) ) 1))
(applytest 'err modulo (+ (*  (* 1024 1024 1024 4) (* 1024 1024 1024 4) ) 1) 0)

(applytest 'err nthroot 3+5i 2)

(applytest 3.0 nthroot~ 9 2)
(applytest 3.0 nthroot~ 27 3)

(applytest onebig abs (- 0 onebig))
(applytest 3.0 abs 3.0)
(applytest 3.0 abs -3.0)
(applytest 3-4i abs 3-4i)
(applytest 3-4i abs -3-4i)
;; (applytest 2/3 abs 2/3+4i)
;; (applytest 2/3 abs -2/3+4i)
;; (applytest 3-4i abs -3-4i)

(applytest 4.0 truncate 4.5)
(applytest -4.0 truncate -4.5)
(applytest -5.0 floor -4.5)
(applytest 4.0 floor 4.5)
(applytest -4.0 ceiling -4.5)
(applytest 5.0 ceiling 4.5)

(applytest 4.0 round 4.4)
(applytest 5.0 round 4.6)
(applytest -5.0 round -4.6)
(applytest -4.0 round -4.4)

(applytest 4 round 31/8)
(applytest -3 round -31/8)
(applytest 3 round 27/8)
(applytest -3 round -27/8)

(applytest 4 truncate 18/4)
(applytest -4 truncate -18/4)
(applytest -5 floor -18/4)
(applytest 4 floor 18/4)
(applytest -4 ceiling -18/4)
(applytest 5 ceiling 18/4)

(applytest "21" ->hex 33)
(applytest 33 ->hex "21")

(applytest 99.999 string->number "99.999")
(applytest -99.999 string->number "-99.999")
(applytest 99 string->number "99")
(applytest 10 string->number "0a" 16)
(applytest 1/3 string->number "1/3")

(applytest 55340232221128655749/3 + 1/3 300 onebig)
(applytest -55340232221128655747/3 - 1/3 300 onebig)
(applytest 1844674407370955161600 * 1/3 300 onebig)
(applytest 1/16602069666338596454400 / 1/3 300 onebig)

(define (close-float? x y (r))
  (default! r
    (sqrt (+ (* (- (real-part x) (real-part y))
		(- (real-part x) (real-part y)))
	     (* (- (imag-part x) (imag-part y))
		(- (imag-part x) (imag-part y))))))
  (< r 0.00001))

(applytest close-float? 336.633333+9.000000i + 1/3 300 33.3 3+9.0i)
(applytest close-float? -335.966667-9.000000i - 1/3 300 33.3 3+9.0i)
(applytest close-float? 9990.000000+29970.000000i * 1/3 300 33.3 3+9.0i)
;;Not sure what the spec for complex numbers says about this.
;;(applytest close-float? 1.112223e-06-3.336670e-06i / 1/3 300 33.3 3+9.0i)

(applytest 55340232221128655758/3+9i + 1/3 300 onebig 3+9i)
(applytest -55340232221128655756/3-9i - 1/3 300 onebig 3+9i)
(applytest 5534023222112865484800+16602069666338596454400i * 1/3 300 onebig 3+9i)
(applytest 1/498062089990157893632000-1/166020696663385964544000i / 1/3 300 onebig 3+9i)

(errtest (string->number 99))
(errtest (string->number 99.5))

(applytest 99 ->number "99")
(applytest 10 ->number "0a" 16)

(applytest 99 ->number 99)
(applytest 10 ->number 0x0a)

(applytest 'err min)
(applytest 'err max)

(applytest 'err gcd 200 10.0)
(applytest 'err gcd 200.0 10)
(applytest 'err lcm 200 10.0)
(applytest 'err lcm 200.0 10)

;;; Check arithmetic operators

(define-tester (p x y z) (+ x y z))
(define-tester (s x y z) (- x y z))
(define-tester (m x y z) (* x y z))

(applytest 10 p 5 2 3)
(applytest 0 s 5 4 1)
