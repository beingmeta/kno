;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'email)

(use-module '{fdweb jsonout})

(module-export! '{hostinfo/json})

(define (hostinfo)
  (frame-create #f
    'hostname (gethostname)
    'port (req/get 'server_port)
    'syshost (req/get 'syshost)
    'http (req/get 'server_protocol)
    'software (choice (req/get 'server_software)
		      (glom "framerd-" (config 'fdversion))
		      (glom "libu8-" (config 'u8version)))
    'servername (req/get 'servername)
    'serverip (req/get 'serveraddr)
    'remoteip (req/get 'remote_addr)
    'remoteport (req/get 'remote_port)
    'ip (try (difference (hostaddrs (gethostname)) "127.0.0.1")
	     (hostaddrs (gethostname)))
    'uptime (elapsed-time)))

(define (hostinfo/json)
  (req/set! 'doctype #f)
  (req/set! 'content-type "application/json")
  (req/set! 'content (json->string (hostinfo))))

(define (hostinfo/field slotid)
  (req/set! 'doctype #f)
  (req/set! 'content-type "text")
  (req/set! 'content (try (get (hostinfo) slotid)
			  (glom "Unknown field: " slotid))))

(module-export! '{hostinfo/json hostinfo/field hostinfo})
