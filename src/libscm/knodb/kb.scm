(in-module 'knodb/kb)

(use-module 'knodb)

(module-export! '{kb/ref kb/make kb/partitions 
		  kb/container kb/container!
		  kb/commit! kb/save!})

(define kb/ref knokb/ref)
(define kb/make knokb/make)
(define kb/partitions knokb/partitions)
(define kb/mods knokb/mods)
(define kb/modified? knokb/modified?)
(define kb/commit! knokb/commit!)
(define kb/save! knokb/save!)
