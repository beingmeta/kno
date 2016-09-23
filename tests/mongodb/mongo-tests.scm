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


