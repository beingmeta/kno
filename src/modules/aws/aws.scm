(in-module 'aws)

(module-export! '{awskey secretawskey awsaccount})

;; Default (non-working) values from the online documentation
;;  Helpful for testing requests
(define secretawskey
  (or (getenv "AWS_SECRET_ACCESS_KEY")
      #"uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o"))
(define awskey
  (or (getenv "AWS_ACCESS_KEY_ID")
      "0PN5J17HBGZHT7JJ3X82"))
(define awsaccount
  (or (getenv "AWS_ACCOUNT_NUMBER")
      "0PN5J17HBGZHT7JJ3X82"))

(config-def! 'secretawskey
	     (lambda (var (val))
	       (if (bound? val)
		   (set! secretawskey val)
		   secretawskey)))
(config-def! 'awskey
	     (lambda (var (val))
	       (if (bound? val)
		   (set! awskey val)
		   awskey)))
(config-def! 'awsaccount
	     (lambda (var (val))
	       (if (bound? val)
		   (set! awsaccount val)
		   awsaccount)))

