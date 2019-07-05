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

(applytest string? lisp->string promise1)

(applytest 5 force promise1)
(applytest #f promise/broken? promise1)
(applytest #t promise/resolved? promise1)
(applytest #t promise/satisfied? promise1)

(applytest string? lisp->string promise1)

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

(applytest string? lisp->string promise5)

(define promise6 (delay {3 4 5 two}))
(applytest {3 4 5 2} (force promise6))

(set! promise1 #f)
(set! promise2 #f)
(set! promise3 #f)
(set! promise4 #f)
(set! promise5 #f)
(set! promise6 #f)

(errtest (delay))
(applytest promise? (delay (+ 2 3)))
(applytest promise? (delay 5))
(applytest "five" force "five")
(applytest 5 force (delay 5))
(applytest 5 force (delay (+ 2 3)))
(applytest "five" promise/probe "five")

(define bad-promise (delay (+ 3 "three")))
(errtest (force bad-promise))
(applytest exception? force bad-promise)

(define deliveries 0)
(define multi-promise (delay (begin (set! deliveries (1+ deliveries)) deliveries)))
(applytest 1 force multi-promise)
(applytest 1 force multi-promise)

(define common-counter 0)

(define (counter-proc)
  (set! common-counter (1+ common-counter))
  common-counter)
(define counter-promise (delay (counter-proc)))
(evaltest 1 (parallel (force counter-promise) (force counter-promise) (force counter-promise)))

;;; All done

(test-finished "PROMISES")
