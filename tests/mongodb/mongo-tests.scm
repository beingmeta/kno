(use-module '{mongodb logger})

(define db (mongo/open "mongodb://writer:none@localhost:7777/knotest" ))
(define testing (collection/open db "testing"))
(collection/remove! testing {#[a 3] #[a 4] #[a 5]})
(collection/insert! testing #[a 3 b 8])
(collection/insert! testing #[a 4 b 7])
(collection/insert! testing #[a 5 b 12])
(collection/insert! testing #[a 5 b 13])
(define (count/matches db q)
  (choice-size (collection/find db q)))
(applytest 1 count/matches testing #[a 3])
(applytest 1 count/matches testing #[a 4])
(applytest 2 count/matches testing #[a 5])

(define idtesting (collection/open db "idtesting"))

(collection/remove! idtesting #[])

(collection/insert! idtesting #[_id 1 text "one"])
(collection/insert! idtesting #[_id 2 text "two"])
(collection/insert! idtesting #[_id 3 text "three"])
(collection/insert! idtesting #[_id 4 text "four"])
(evaltest #t (onerror (begin 
			(logwarn |ExpectedError| 
			  "This will signal an error which should be 'ignored' if you're not in the debugger")
			(collection/insert! idtesting #[_id 3 text "trois"])
			#f)
	       (lambda (ex) #t)))
(applytest #[_ID 3 TEXT "three"] collection/get idtesting 3)


