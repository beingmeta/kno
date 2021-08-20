;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(define-init foo #f)
(define-init bar #f)

(applytest #t timestamp? foo)
(applytest #t timestamp? bar)
(applytest #t time>? bar foo)

(applytest #t timestamp? (config 'startup.cfg))
(applytest (get (timestamp) 'year) config 'startup.cfg.year)

(evaltest #t (bound? gpath?))
(evaltest #t (bound? $size))
(evaltest #t (bound? gather))

(applytest (getenv "USER") config 'envuser)
(applytest (getenv "USER") config 'envuser1)
(applytest #t number? (config 'loadpid))
(applytest (abspath "r4rs.scm") config 'r4rs)

(applytest #t environment? (%appenv))
(applytest #t test (%appenv) 'foo)

(test-finished "STARTUP")
