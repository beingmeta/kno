;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'packetfns)

(module-export! '{packet->digits digit->packet
		  packetfns/base62 packetfns/base36 packetfns/base16
		  packetfns/condense}) 

(define packetfns/base62
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz")
(define packetfns/base36
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
(define packetfns/base16 "0123456789ABCDEF")

(define (packet->digits packet (digits packetfns/base62))
  (let ((num (string->number (packet->base16 packet) 16))
	(base (length digits))
	(result '()))
    (while (> num base)
      (set! result (cons (elt digits (remainder num base)) result))
      (set! num (quotient num base)))
    (set! result (cons (elt digits num) result))
    (apply string result)))

(define (digits->packet string (digits packetfns/base62))
  (let ((num 0) (base (length digits)) (i 0) (n (length string)))
    (dotimes (i n)
      (set! num (+ (* base num) (position (elt string i) digits))))
    (let* ((hex (%watch (number->string (%watch num) 16)))
	   (packet (if (zero? (remainder (length hex) 2))
		       (base16->packet hex)
		       (base16->packet (glom "0" hex)))))
      packet)))

(define (packetfns/condense packet factor)
  (let* ((len (length packet))
	 (input (->vector packet)) 
	 (i (remainder len factor))
	 (lim (- len i))
	 (output (if (= i 0) '()
		     `(, (remainder (apply * (->list (map 1+ (slice input 0 i))))
				    256)))))
    (while (< i lim)
      (%watch i output len)
      (set! output (cons (remainder (apply * (->list (map 1+ (slice input i (+ i 4)))))
				    256)
			 output))
      (set! i (+ i 4)))
    (->packet (reverse output))))

