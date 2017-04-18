(use-module 'mongodb)

(define db (mongodb/open "mongodb://writer:none@localhost/fdtest" ))
(define testing (mongodb/collection db "testing"))
(mongodb/insert! testing #[a 3 b 8])
(mongodb/insert! testing #[a 4 b 7])
(mongodb/insert! testing #[a 5 b 12])
(mongodb/insert! testing #[a 5 b 13])
(define (count/matches db q)
  (choice-size (mongodb/find db q)))
(applytest 1 count/matches testing #[a 3])
(applytest 1 count/matches testing #[a 4])
(applytest 2 count/matches testing #[a 5])


(define idtesting (mongodb/collection db "idtesting"))

(mongodb/insert! idtesting #[_id 1 text "one"])
(mongodb/insert! idtesting #[_id 2 text "two"])
(mongodb/insert! idtesting #[_id 3 text "three"])
(mongodb/insert! idtesting #[_id 4 text "four"])
(evaltest #t (onerror (begin (mongodb/insert! idtesting #[_id 3 text "trois"])
			#f)
	       (lambda (ex) #t)))
(applytest #[_ID 3 TEXT "three"] mongodb/get idtesting 3)


