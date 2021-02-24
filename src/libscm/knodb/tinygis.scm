;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'knodb/tinygis)

;;; Provides some simple and roughly accurate GIS functionality, especially
;;;  distances and the ability to get ambiguous lat/long keys to use for
;;;  location indexing.

(use-module '{texttools text/parsetime})

(module-export! '{geodistance geodirection project-point
		  dms->float float->dms
		  getlat getlong conspoint pointmatch
		  get-degree-keys latrange longrange latlongrange
		  latspan longspan
		  parse-nmea-sentence})

;;; This implements some GIS like functions, assuming a spherical
;;; earth, etc.  It would be good to redo this using more accurate
;;; algorithms, but this will do for an approximation.
;;; The formulae were mildly adapted from:
;;;;  http://williams.best.vwh.net/avform.htm

;;; Dealing with latitudes and longitudes

(define PI 3.14159265358979323846)
(define PI*2 (* 2 PI))
(define PI/2 (/ PI 2))
(define EPSILON (* 1.0 (/ 7200)))

(define (square x) (* x x))
(define (modulus x y)
  (if (< x 0)
      (+ y (- x (* y (truncate (/ x y)))))
      (- x (* y (truncate (/ x y))))))

(define (degrees->radians x)
  (* x (/ PI 180)))
(define (radians->degrees x)
  (* x (/ 180 PI)))

(define (radians->meters x)
  (* 1852 x (/ (* 180 60) PI)))
(define (meters->radians x)
  (* (/ x 1852) (/  PI (* 180 60))))

;;; Generic lat and long functions

(define (getlat x)
  (cond ((pair? x) (car x))
	((vector? x) (elt x 0))
	((oid? x) (get x 'lat))
	((table? x) (get x 'lat))
	(else (fail))))
(define (getlong x)
  (cond ((pair? x)
	 (if (pair? (cdr x)) (cadr x) (cdr x)))
	((vector? x) (elt x 1))
	((oid? x) (get x 'long))
	((table? x) (get x 'long))
	(else (fail))))

(define (conspoint . args)
  (if (= (length args) 1)
      (for-choices (elt (car args))
	(vector (getlat elt) (getlong elt)))
    (vector (qc (get-arg args 0)) (qc (get-arg args 1)))))

(define (pointmatch point vec)
  (cons (intersection (getlat point) (getlat vec))
	(intersection (getlong point) (getlong vec))))

;;;; Simple calculations

(define (geodistance-radians lat1 long1 lat2 long2)
  (* 2 (asin (sqrt (+ (square (sin (/ (- lat1 lat2) 2)))
		      (* (cos lat1) (cos lat2)
			 (square (sin (/ (- long1 long2) 2)))))))))

(define (raw-geodirection lat1 long1 lat2 long2)
  (atan2 (* (sin (-  long2 long1)) (cos lat2))
	 (- (* (cos lat1) (sin lat2))
	    (* (sin lat1) (cos lat2) (cos (- long2 long1))))))
(define (geodirection-radians lat1 long1 lat2 long2)
  (if (< (cos lat1) EPSILON)
      (if (> lat1 0) pi pi*2)
    (modulus (raw-geodirection lat1 long1 lat2 long2) PI*2)))
;(define (geodirection-radians lat1 long1 lat2 long2)
;  (if (< (cos lat1) EPSILON)
;      (if (> lat1 0) pi PI*2)
;    (let ((d (geodistance-radians lat1 long1 lat2 long2)))
;      (if (< (sin (- long2 long1)) 0)
;	  (acos (/ (- (sin lat2) (* (sin lat1) (cos d)))
;		   (* (sin d) (cos lat1))))
;	(- PI*2 (acos (/ (- (sin lat2) (* (sin lat1) (cos d)))
;			 (* (sin d) (cos lat1)))))))))


(define (geodistance arg . args)
  (if (number? arg)
      (radians->meters
       (geodistance-radians (degrees->radians arg)
			    (degrees->radians (get-arg args 0))
			    (degrees->radians (get-arg args 1))
			    (degrees->radians (get-arg args 2))))
    (radians->meters
     (geodistance-radians (degrees->radians (getlat arg))
			  (degrees->radians (getlong arg))
			  (degrees->radians (getlat (get-arg args 0)))
			  (degrees->radians (getlong (get-arg args 0)))))))
(define (geodirection arg . args)
  (if (number? arg)
      (radians->degrees
       (geodirection-radians (degrees->radians arg)
			     (degrees->radians (get-arg args 0))
			     (degrees->radians (get-arg args 1))
			     (degrees->radians (get-arg args 2))))
    (radians->degrees
     (geodirection-radians (degrees->radians (getlat arg))
			   (degrees->radians (getlong arg))
			   (degrees->radians (getlat (get-arg args 0)))
			   (degrees->radians (getlong (get-arg args 0)))))))

(define (project-point-radians lat long direction distance)
  (let* ((lat2 (asin (+ (* (sin lat) (cos distance))
			(* (cos lat) (sin distance) (cos direction)))))
	 (dlong (atan2 (* (sin direction) (sin distance)
			 (cos lat))
		      (- (cos distance)
			 (* (sin lat) (sin lat2))))))
    (vector lat2 (- (modulus (- long dlong (- pi)) PI*2) PI))))
(define (project-point-radians lat long direction distance)
  (let* ((plat (asin (+ (* (sin lat) (cos distance))
			(* (cos lat) (sin distance) (cos direction)))))
	 (plong (- (modulus (+ long (/ (asin (* (sin direction) (sin
								 distance)))
				       (cos lat))
			       (- pi))
			    PI*2)
		   pi)))
    (vector plat plong)))
(define (project-point arg . args)
  (let ((lat (if (number? arg) arg (getlat arg))))
    (let ((long (if (number? arg) (get-arg args 0) (getlong arg)))
	  (direction (if (number? arg) (get-arg args 1) (get-arg args 0)))
	  (distance (if (number? arg) (get-arg args 2) (get-arg args 1))))
      (let ((vec (project-point-radians (degrees->radians lat)
					(degrees->radians long)
					(degrees->radians direction)
					(meters->radians distance))))
	(cons (radians->degrees (getlat vec))
	      (radians->degrees (getlong vec)))))))

;;;; Expanding search keys

;;;; Indexing

(define (fractional n) (- n (truncate n)))
(define (float->dms num)
  (let* ((sign (>= num 0))
	 (num (if sign num (- num)))
	 (degree (truncate num))
	 (minutes (truncate (round (* 60 (- num degree)))))
	 (time-left (- num degree (* minutes (/ 1.0 60))))
	 (seconds (truncate (round (* 3600 time-left)))))
    (stringout (if sign "+" "-")
	       (if (< degree 10) (printout "00" degree)
		 (if (< degree 100) (printout "0" degree)
		   degree))
	       (if (> minutes 9) minutes (printout "0" minutes))
	       (if (> seconds 9) seconds (printout "0" seconds)))))
(define (convert-dms degrees minutes seconds)
  (* (if (< degrees 0) -1.0  1.0) 
     (+ (abs degrees) (* minutes 1/60) (* seconds 1/3600))))
(define dms-patterns
  {#((label d (chunk #({"+" "-" ""} (isdigit+)))) ":"
     (label m (isdigit+)) ":" (label s (isdigit+)))
    #((label d (chunk #({"+" "-" ""} (isdigit+)))) ":"
      (label m (isdigit+)))
    #((label d (chunk #({"+" "-" ""} (isdigit+)))) ":"
      (label m (chunk #((isdigit+) "." (isdigit+)))))})
(define (dms->float string)
  (let ((map (text->frame dms-patterns string)))
    (if (exists? map)
	(convert-dms (string->lisp (get map 'd))
		     (string->lisp (get map 'm))
		     (try (string->lisp (get map 's)) 0))
      (* 1.0 (string->lisp string)))))

(define (get-degree-keys degrees)
  (choice (inexact->exact (round degrees)) ;; Just as an int
	  ;; (subseq (float->dms degrees) 0 {4 6 8}) ;; D, DM, and DMS strings
	  (scalerep degrees -60)
	  (scalerep degrees -3600)))

(define (lat+ x delta)
  (let ((new (+ x delta)))
    (if (< new -180) (+ 180 new)
      (if (> new 180) (- new 360)
	new))))
(define (lon+ x delta)
  (let ((new (+ x delta)))
    (if (< new -90) (+ 90 new)
      (if (> new 90) (- new 180)
	new))))
(define (latrange degrees fraction (radius 0))
  (let* ((base (round degrees))
	 (results base))
    (dotimes (i radius)
      (set+! results (choice results
			     (lat+ base (* fraction radius))
			     (lat+ base (- (* fraction radius))))))
    results))
(define (longrange degrees fraction (radius 0))
  (let* ((base (round degrees))
	 (results base))
    (dotimes (i radius)
      (set+! results (choice results
			     (lon+ base (* fraction radius))
			     (lon+ base (- (* fraction radius))))))
    results))
(define (latlongrange latlong fraction (latspan 0) (longspan #f))
  (vector (qc (latrange (getlat latlong) fraction latspan))
	  (qc (longrange (getlong latlong) fraction (or longspan latspan)))))

(define (%latspan start end fraction (inclusive #f))
  (if (< end start) (%latspan end start fraction inclusive)
    (let ((bottom (* (truncate (round (* start (/ fraction)))) fraction))
	  (top (* (truncate (round (* end (/ fraction)))) fraction)))
      (if (>= bottom top)
	  (if inclusive (choice bottom top) (fail))
	(do ((results {} (choice results lat))
	     (lat bottom (lat+ lat fraction)))
	    ((> lat top) results))))))
(define (%longspan start end fraction (inclusive #f))
  (if (< end start) (%longspan end start fraction inclusive)
    (let ((bottom (* (truncate (round (* start (/ fraction)))) fraction))
	  (top (* (truncate (round (* end (/ fraction)))) fraction)))
      (if (>= bottom top)
	  (if inclusive (choice bottom top) (fail))
	(do ((results {} (choice results long))
	     (long bottom (lon+ long fraction)))
	    ((> long top) results))))))

(define (latspan start end)
  (let ((degree-keys (%latspan start end 1)))
    (let ((minute-keys
	   (choice degree-keys
		   (if (fail? degree-keys) (%latspan start end 1/60) 
		     (choice (%latspan start (smallest degree-keys) 1/60)
			     (%latspan (largest degree-keys) end 1/60))))))
      (let ((second-keys
	     (choice minute-keys
		     (if (fail? minute-keys) (%latspan start end 1/3600 #t) 
		       (choice (%latspan start (smallest minute-keys)
					 1/3600 #t)
			       (%latspan (largest minute-keys) end
					 1/3600 #t))))))
	second-keys))))

(define (longspan start end)
  (let ((degree-keys (%longspan start end 1)))
    (let ((minute-keys
	   (choice degree-keys
		   (if (fail? degree-keys) (%longspan start end 1/60) 
		     (choice (%longspan start (smallest degree-keys) 1/60)
			     (%longspan (largest degree-keys) end 1/60))))))
      (let ((second-keys
	     (choice minute-keys
		     (if (fail? minute-keys) (%longspan start end 1/3600
							#t) 
		       (choice (%longspan start (smallest minute-keys)
					  1/3600 #t)
			       (%longspan (largest minute-keys) end
					  1/3600 #t))))))
	second-keys))))

;;; NMEA parsing

(define nmea-parsers (make-hashtable))

(define (parse-nmea-sentence line)
  (if (find #\, line)
      (let* ((end (position #\* line))
	     (cksum (if end (string->lisp (subseq line (1+ end))) #f))
	     (line (if end (subseq line 0 end) line))
	     (record (segment line ",")))
	(let ((handler (get nmea-parsers (car record))))
	  (if handler (handler record) (fail))))
    (fail)))

(define (parse-degree s)
  (if (= (length s) 0) (fail)
    (let ((dot (position #\. s)))
      (if dot (let ((degrees (string->lisp (subseq s 0 (- dot 2))))
		    (minutes (string->lisp (subseq s (- dot 2)
						       0))))
		(+ degrees (/ minutes 60)))
	(string->lisp s)))))

(define (read-lat rec i)
  (let ((deg (elt rec i))
	(south (member (elt rec (1+ i)) '("S" "s"))))
    (if south (- (parse-degree deg))
      (parse-degree deg))))
(define (read-lon rec i)
  (let ((deg (elt rec i))
	(west (member (elt rec (1+ i)) '("W" "w"))))
    (if west (- (parse-degree deg))
      (parse-degree deg))))

(define (readstring s)
  (if (= (length s) 0) (fail) (string->lisp s)))
(define (time-parse time date)
  (parsetime
   (if (= (length time) 0)
       (if (= (length date) 0) (fail)
	 (string-append (subseq date 4) "-"
			(subseq date 2 4) "-"
			(subseq date 0 2)))
     (string-append (subseq date 4) "-"
		    (subseq date 2 4) "-"
		    (subseq date 0 2) "T"
		    (subseq time 0 2) ":"
		    (subseq time 2 4) ":"
		    (subseq time 4)))))

(define (parse-gprmc rec)
  (frame-create #f
    'time (time-parse (elt rec 1) (elt rec 9))
    'lat (read-lat rec 3) 'long (read-lon rec 5)
    'speed (readstring (elt rec 7))
    'course (readstring (elt rec 8))))
(store! nmea-parsers "$GPRMC" parse-gprmc)
(define (parse-hchdg rec)
  (frame-create #f
    'heading (truncate (round (readstring (elt rec 1))))))
(store! nmea-parsers "$HCHDG" parse-hchdg)

;;;; Some Reference points

(define mleloc
  (cons (dms->float "53:20.517") (dms->float "-6:16.959" )))
(define bostonloc
  (cons 42.321597 -71.089115))
(define dcloc
  (cons 38.913611 -77.013222))
(define nycloc
  (cons 40.704234 -73.917927))
(define stephensgreen
  (cons 53.333333 -6.250000))

(module-export! '{mleloc bostonloc dcloc nycloc stephensgreen})


