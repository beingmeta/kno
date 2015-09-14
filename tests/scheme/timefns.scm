(load-component "common.scm")

(define est-time1 #T2011-12-03T03:23:00EST)
(define gmt-time1 #T2011-12-03T08:23:00Z)

(define est-time1-string #T2011-12-03T03:23:00EST)
(define gmt-time1-string "2011-12-03T08:23:00Z")

(define (get-sign n)
  (if (< n 0) -1 (if (> n 0) 1 0)))

(define time1-tick 1322900580)
(applytest #t equal? est-time1 gmt-time1)
(applytest #t equal? (get est-time1 'tick) (get gmt-time1 'tick))
(applytest time1-tick get est-time1 'tick)
(applytest 0 get est-time1 'dstoff)
(applytest -18000 get est-time1 'gmtoff)
(applytest -18000 get est-time1 'tzoff)

(applytest #f time<? time1-tick)
(applytest #t time>? time1-tick)
(evaltest 1 (get-sign (time-since time1-tick)))
(evaltest -1 (get-sign (time-until time1-tick)))

(applytest time1-tick get gmt-time1 'tick)
(applytest 0 get gmt-time1 'gmtoff)
(applytest 0 get gmt-time1 'tzoff)
(applytest 0 get gmt-time1 'dstoff)

(define est-time2 #T2011-06-08T15:00:00EDT)
(define gmt-time2 #T2011-06-08T19:00:00Z)
(define cet-time2 #T2011-06-08T15:00:00EDT)
(store! cet-time2 'tzoff 3600)
(define time2-tick 1307563200)

(applytest #t equal? est-time2 gmt-time2)
(applytest #t equal? (get est-time2 'tick) (get gmt-time2 'tick))
(applytest #t equal? (get est-time2 'tick) (get cet-time2 'tick))
(applytest time2-tick get est-time2 'tick)
(applytest time2-tick get cet-time2 'tick)
(applytest 3600 get est-time2 'dstoff)
(applytest -14400 get est-time2 'gmtoff)
(applytest -18000 get est-time2 'tzoff)

(applytest 3600 get cet-time2 'dstoff)
(applytest 7200 get cet-time2 'gmtoff)
(applytest 3600 get cet-time2 'tzoff)

(applytest time2-tick get gmt-time2 'tick)
(applytest 0 get gmt-time2 'gmtoff)
(applytest 0 get gmt-time2 'tzoff)
(applytest 0 get gmt-time2 'dstoff)

(message "TIMEFNS tests successfuly completed")

