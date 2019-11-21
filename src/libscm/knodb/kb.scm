(in-module 'knodb/kb)

(use-module 'knodb)

(module-export! '{kb/ref kb/make kb/partitions 
		  kb/modified? kb/mods
		  kb/container kb/container!
		  kb/commit! kb/save!})

(define knodb (get-module 'knodb))

(define kb/ref (fcn/alias knodb/ref knodb))
(define kb/make (fcn/alias knodb/make knodb))
(define kb/partitions (fcn/alias knodb/partitions knodb))
(define kb/mods (fcn/alias knodb/mods knodb))
(define kb/modified? (fcn/alias knodb/modified? knodb))
(define kb/commit! (fcn/alias knodb/commit! knodb))
(define kb/save! (fcn/alias knodb/save! knodb))
