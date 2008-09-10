(in-module 'webapi/facebook)

(use-module '{ezrecords fdweb texttools})

(defrecord facepp appid apikey secretkey)

(define apikey "1ab7fa75d12550c67cf11f3c6be17cb1")
(define secretkey "1932a9c8a3c12044b8bbd11397da16ed")
(define appid "24262579118")

(define (config-fb/appinfo var (val))
  (if (bound? val)
      (if (and (vector? val)
	       (= (length val) 3)
	       (every? string? val))
	  (begin (set! appid (first val))
		 (set! apikey (second val))
		 (set! secretkey (third val))))
      (vector appid apikey)))
(config-def! 'fb/appinfo config-fb/appinfo)

(define facebook-uri "https://api.facebook.com/restserver.php")

(define (pair-args args)
  (let ((results {}))
    (do ((scan args (cddr scan)))
	((null? scan) results)
      (set+! results (cons (car scan) (cadr scan))))))

(define (fb/calluri uri . args)
  (let* ((plist (sorted (pair-args args) car))
	 (sigstring
	  (stringout (doseq (p plist) (printout (car p) "=" (cdr p)))
	    secretkey))
	 (signature (md5 sigstring))
	 (urlpostargs (list "sig"
			    (downcase (packet->base16 signature)))))
    (doseq (p plist)
      (set! urlpostargs (cons* (car p) (cdr p) urlpostargs)))
    (apply urlpost uri (->list urlpostargs))))

(define (fb/call . args)
  (let* ((plist (sorted (pair-args args) car))
	 (sigstring
	  (stringout (doseq (p plist) (printout (car p) "=" (cdr p)))
	    secretkey))
	 (signature (md5 sigstring))
	 (urlpostargs (list "sig"
			    (downcase (packet->base16 signature)))))
    (doseq (p plist)
      (set! urlpostargs (cons* (car p) (cdr p) urlpostargs)))
    (apply urlpost facebook-uri (->list urlpostargs))))

(comment
 (apicall "http://api.facebook.com/restserver.php"
	  "v" "1.0" "api_key" apikey
	  "method" "facebook.auth.getSession"
	  "auth_token" authtoken))

