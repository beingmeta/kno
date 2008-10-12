(in-module 'facebook)

(module-export! '{appname appid apikey apisecretkey})

(define applock #f)
(define appname #f)
(define appid #f)
(define apikey #f)
(define apisecretkey #f)

(config-def! 'fb/appinfo
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (if (and (vector? val) (= (length val) 4))
			   (begin (set! appname (first val))
				  (set! appid (second val))
				  (set! apikey (third val))
				  (set! apisecretkey (fourth val)))
			   (error "Invalid Facebook application info")))
		   (vector appname appid apikey))))

(config-def! 'fb/applock
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! applock val))
		   appname)))
(config-def! 'fb/appname
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! appname val))
		   appname)))
(config-def! 'fb/appid
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! appid val))
		   appid)))
(config-def! 'fb/key
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! apikey val))
		   apikey)))
(config-def! 'fb/secret
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! apisecretkey val))
		   apisecretkey)))
