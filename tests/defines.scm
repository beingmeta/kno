;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{ezrecords reflection text/stringfmts})

(define-tester (add-separators string . remainder)
  (local separator (slice string (-1+ (length string))))
  (stringout (printout string)
    (dolist (each remainder i) (printout (if (> i 0) separator) each))))

(applytest "foo,bar,baz,quux" add-separators "foo," 'bar 'baz 'quux)

(define-tester (add-separators2 string . remainder)
  (define separator (slice string (-1+ (length string))))
  (stringout (printout string)
    (dolist (each remainder i) (printout (if (> i 0) separator) each))))

(applytest "foo,bar,baz,quux" add-separators2 "foo," 'bar 'baz 'quux)

(define-tester (fibi2 n)
  (define (fib-iter i cur prev)
    (if (= i 1) cur (fib-iter (-1+ i) (+ cur prev) cur)))
  (if (= n 0) 0 (fib-iter n 1 0)))

(applytest 5 fibi2 5)
(applytest 55 fibi2 10)
(applytest 354224848179261915075 fibi2 100)
(applytest 43466557686937456435688527675040625802564660517371780402481729089536555417949051890403879840079255169295922593080322634775209689623239873322471161642996440906533187938298969649928516003704476137795166849228875
	   fibi2 1000)

(define-tester (fibi3 n)
  (locals fib-iter
	  (lambda (i cur prev)
	    (if (= i 1) cur (fib-iter (-1+ i) (+ cur prev) cur))))
  (if (= n 0) 0 (fib-iter n 1 0)))
  
(applytest 5 fibi2 5)
(applytest 55 fibi2 10)
(applytest 354224848179261915075 fibi2 100)
(applytest 43466557686937456435688527675040625802564660517371780402481729089536555417949051890403879840079255169295922593080322634775209689623239873322471161642996440906533187938298969649928516003704476137795166849228875
	   fibi2 1000)
