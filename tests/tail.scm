(load-component "common.scm")

(define (balance-iter i up down)
  (if (= i 1) (- up down)
      (balance-iter (-1+ i) (1+ up) (1+ down))))
(define (balancer n)
  (if (= n 0) 0 (balance-iter n 0 0)))

(define (nested-balance-iter i up down)
  (let ((one 1) 
	(next (- up down)))
    (if (= i one) next
	(nested-balance-iter (-1+ i) (1+ up) (1+ down)))))
(define (nested-balancer n)
  (if (= n 0) 0 (nested-balance-iter n 0 0)))

(define (fib-iter i cur prev)
  (if (= i 1) cur (fib-iter (-1+ i) (+ cur prev) cur)))
(define (fibi n)
  (if (= n 0) 0 (fib-iter n 1 0)))

(define (test-tail-calls)
  (applytest 0 balancer 100)
  ;; This should blow out the stack if tail recursion is broken
  (applytest 0 balancer 10000)
  (applytest 0 nested-balancer 10000)
  (applytest 6765 fibi 20)
  (applytest 280571172992510140037611932413038677189525 fibi 200)
  (applytest 43466557686937456435688527675040625802564660517371780402481729089536555417949051890403879840079255169295922593080322634775209689623239873322471161642996440906533187938298969649928516003704476137795166849228875
	     fibi 1000))

;; This tests that tail calls in WHEN are evaluated

(define test-flag #f)

(define (set-test-flag! val)
  (set! test-flag val))

(define (bug-test (val #f))
  (when (= 3 3)
    (if (= 2 2)
	(set-test-flag! val))))

(define (optimized-tail-testfn)
  (set! test-flag #f)
  (bug-test #t)
  test-flag)

(define (countup n (i 0))
  (if (= n 0) (dbg i) (countup (-1+ n) (1+ i))))

(define (countup n (i 0))
  (if (= n 0) (dbg i) (countup (-1+ n) (1+ i))))

(define (reduce-string string)
  (if (= (length string) 0) 
      (dbg string)
      (reduce-string (slice string 1))))

(define (smerge s1 s2)
  (if (zero? (length s1)) s2
      (smerge (slice s1 1) (glom (slice s1 0 1) s2))))

(define (domerge l1 l2)
  (if (null? l1) l2
      (domerge (cdr l1) (cons (car l1) l2))))

(define (domerge-bug1 l1 l2)
  (smerge "ab" "c")
  (if (null? l1) l2
      (domerge-bug1 (cdr l1) (cons (car l1) l2))))

(define (domerge-bug2 l1 l2)
  (if (and (null? l1) (smerge "ab" "c"))
      l2
      (domerge-bug2 (cdr l1) (cons (car l1) l2))))

(define (domerge-bug3 l1 l2)
  (if (begin (smerge "ab" "c") (null? l1))
      l2
      (domerge-bug3 (cdr l1) (cons (car l1) l2))))

(define (main)
  ;;(dbg main)
  ;;(%watch (smerge "ab" "c"))
  (applytest "bac" (smerge "ab" "c"))
  (applytest '(b a c) (domerge '(a b) '(c)))
  (countup 10000)
  (reduce-string "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789")
  (domerge-bug1 '(a b) '(c))
  (domerge-bug2 '(a b) '(c))
  (domerge-bug3 '(a b) '(c))
  (test-tail-calls)
  (applytest #t optimized-tail-testfn))


(test-finished "R4RS test")

