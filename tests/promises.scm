;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

;;; Promises

(define promise1 (delay (+ 2 3)))

(applytest #t promise? promise1)
(applytest #f promise? "promise1")
(applytest #f promise? 5)
(applytest #f promise/broken? promise1)
(applytest #f promise/resolved? promise1)
(applytest #f promise/satisfied? promise1)

(applytest-pred string? lisp->string promise1)

(applytest 5 force promise1)
(applytest #f promise/broken? promise1)
(applytest #t promise/resolved? promise1)
(applytest #t promise/satisfied? promise1)

(define two 2)
(define promise2 (delay (+ two 3)))

(applytest #t promise? promise2)
(applytest #f promise/broken? promise2)
(applytest #f promise/resolved? promise2)
(applytest #f promise/satisfied? promise2)

(applytest 5 force promise2)
(applytest #f promise/broken? promise2)
(applytest #t promise/resolved? promise2)
(applytest #t promise/satisfied? promise2)

(define (add-promise n) (delay (+ n 3)))

(define promise3 (add-promise two))

(applytest #t promise? promise3)
(applytest #f promise/broken? promise3)
(applytest #f promise/resolved? promise3)
(applytest #f promise/satisfied? promise3)

(applytest #f promise/probe promise3 #f)

(applytest 5 force promise3)
(applytest #f promise/broken? promise3)
(applytest #t promise/resolved? promise3)
(applytest #t promise/satisfied? promise3)

(applytest 5 promise/probe promise3 #f)

(define promise4 (make-promise 666))
(applytest #f promise/broken? promise4)
(applytest #t promise/resolved? promise4)
(applytest #t promise/satisfied? promise4)
(applytest 666 force promise4)

(define promise5 (delay (+ 3 "four")))
(applytest #f promise/broken? promise5)
(applytest #f promise/resolved? promise5)
(applytest #f promise/satisfied? promise5)

(errtest (force promise5))
(applytest #t promise/broken? promise5)
(applytest #t promise/resolved? promise5)
(applytest #f promise/satisfied? promise5)

(define promise6 (delay {3 4 5 two}))
(applytest {3 4 5 2} (force promise6))

(set! promise1 #f)
(set! promise2 #f)
(set! promise3 #f)
(set! promise4 #f)
(set! promise5 #f)
(set! promise6 #f)

;;; All done

(test-finished "PROMISES")
