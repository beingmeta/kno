;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(define (glom-ab (alpha "foo") (beta "bar"))
  (glom alpha beta))
(applytest "foobar" glom-ab)
(applytest "foobar" req/call glom-ab)

(define (plus-ts (tt 1) (ttt 2.1)) (+ tt ttt))
(applytest 3.1 req/call plus-ts)

(with/request
 (req/log %info% |Startup| requests.sym
	  "Starting test of request functions")
 (req/set! 'alpha "alpha")
 (req/set! 'beta "beta")
 (req/set! 'words "alpha")
 (req/add! 'words "beta")
 (req/add! 'words "gamma")
 (req/set! 'tt "33")
 (req/set! 'ttt "33.3")
 (req/push! 'lst 'first)
 (req/push! 'lst 'second)
 (applytest slotmap? (req/data))
 (applytest "33" req/get 'tt)
 (applytest "33.3" req/get 'ttt)
 (applytest 33 req/val 'tt)
 (applytest 33.3 req/val 'ttt)
 (applytest #t req/test 'alpha)
 (applytest #f req/test 'gamma)
 (applytest #t req/test 'alpha "alpha")
 (applytest #t req/test 'words "gamma")
 (req/drop! 'words "gamma")
 (applytest #f req/test 'words "gamma")
 (req/log %info% |ReqCall| requests.sym
	  "Starting req/call tests")
 (applytest "alphabeta" req/call glom-ab)
 (applytest 66.3 req/call plus-ts)
 (applytest '(second first) req/get 'lst)
 (applytest #t req/live?)
 (let ((len (req/loglen))
       (string (req/getlog)))
   (when (and (applytest len length string)
	      (applytest > 0 req/loglen))
     (req/log %info% |ReqLogOK| requests.sym
	      "REQ/LOG appears to be written okay")
     (applytest #f equal? string (req/getlog))))
 )