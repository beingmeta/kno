;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(config! 'loadpath (get-component "data"))

(unless (has-suffix (config 'exe) "/knoc") (config! 'logloaderrs #f))

(use-module '{stringfmts testcapi})

(errtest (load-component "data/parsefail.scm"))
(errtest (load-component "data/nosuchfile.scm"))

(errtest (load (get-component "data/evalfail.scm")))
(evaltest #[x x y y] (load (get-component "data/evalpostfail.scm")))
(evaltest #[x x y y] (load (get-component "data/badpostload.scm")))
(errtest (load (get-component "data/nosuchfile.scm")))

(applytest "loading.scm" basename (get-component))
(applytest "loading.scm" basename (get-source))

(load-component "data/loadok.scm")

-(use-module 'reloadmod)

(lognotice |LoadPath| (config 'loadpath))

(define-tester (test-reloading)
  (let ((base (get-load-count)))
    (applytest base get-load-count)
    (reload-module 'reloadmod)
    (applytest (1+ base) get-load-count)
    (update-modules)
    (applytest (1+ base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp))
    (sleep 1)
    (update-module 'reloadmod)
    (applytest (+ 2 base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp))
    (sleep 1)
    (update-modules)
    (applytest (+ 3 base) get-load-count)))

(test-reloading)
(sleep 2)
(test-reloading)

(applytester 1 get-load-ok-count)

(load (get-component  "data/loadok.scm"))

(applytester 2 get-load-ok-count)

(load->env (get-component "data/loadok.scm") (%env))
(errtest (load->env (get-component "data/parsefail.scm")))
(errtest (load->env (get-component "data/nosuchfile.scm")))

(applytester 3 get-load-ok-count)

(begin (load-latest (get-component "data/loadok.scm"))
  (applytester 4 get-load-ok-count))
(begin (load-latest (get-component "data/loadok.scm"))
  (applytester 4 get-load-ok-count))

(begin (set-file-modtime! (get-component "data/loadok.scm") (timestamp))
  (load-latest (get-component "data/loadok.scm"))
  (applytester 5 get-load-ok-count))

(begin (sleep 2)
  (set-file-modtime! (get-component "data/loadok.scm") (timestamp))
  (load-latest)
  (applytester 6 get-load-ok-count))

(evaltest 1
	  (withenv #f 
	    (load->env (get-component "data/loadok.scm") (%env))
	    load-ok-count))

(evaltest 1
	  (withenv #f 
	    (load->env (get-component "data/loadok.scm") (%env))
	    (get-load-ok-count)))

;;; Different calling patterns

(load-component "data/loadok.scm" 'utf8)
(load-component "data/loadok.scm" "utf8")
(load-component "data/loadok.scm" 'latin1)

(load (get-component "data/loadok.scm") 'utf8)
(load (get-component "data/loadok.scm") "utf8")
(load (get-component "data/loadok.scm") 'latin1)

(errtest (load))
(errtest (load 88))
(errtest (load-component))
(errtest (load-component 88))

(errtest (load-component '("data" "loadok.scm")))
(errtest (load '("data" "loadok.scm")))

(applytest  #:path"temp.scm" mkpath (getcwd) "temp.scm")
(applytest  #:path"temp.scm" get-component "temp.scm" (getcwd))

(config! 'loadok (get-component "data/loadok.scm"))
(load 'loadok)
(errtest (load 'loadnotok))

;; (applytest environment? (load->env (get-component "data/loadok.scm")))
;; (applytest environment? (load->env (get-component "data/loadok.scm") #t))
;; (applytest environment? (load->env (get-component "data/loadok.scm") #default))
;; (applytest environment? (load->env (get-component "data/loadok.scm") #[]))

(define seen #f)
(applytest environment?
	   load->env (get-component "data/loadval.scm") #[]
	   (lambda ((obj)) (set! seen obj)))
(applytest #t pair? seen)
(applytest environment?
	   load->env (get-component "data/badpostload.scm") #[]
	   (lambda ((obj)) (error 'uncool)))
(applytest environment?
 	   load->env (get-component "data/loadok.scm") #[]
	   (lambda ((obj)) (unless (bound? obj) (set! seen #f))))
(applytest #t eq? seen #f)

(applytest 'err load->env (get-component "data/noloadok.scm") #[]
	   (lambda ((obj)) (unless (bound? obj) (set! seen #f))))
(applytest 'err load->env (get-component "data/loadval.scm") #[]
	   "notafn")
(applytest 'err load->env (get-component "data/loadval.scm") #[]
	   "notafn")


;;; Url loading

(load "https://s3.amazonaws.com/knomods.beingmeta.com/testload.scm")
(evaltest #t (bound? alt-plus))

;;; Errors

(errtest (load 33))
(errtest (load))

;;; Log load errors

(config! 'load:logerrs #t)

(errtest (load (get-component "data/evalfail.scm")))

;;; RUN-FILE

(applytest 3 kno/run-file (get-component "data/multiply.scm") 3 4 5)
(applytest "60" kno/run->string (get-component "data/multiply.scm") 3 4 5)
(applytest 'err kno/run->string (get-component "data/multiply.scm") 3 "four" 5)
