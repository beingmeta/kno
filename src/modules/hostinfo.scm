;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved

(in-module 'hostinfo)

(use-module '{fdweb jsonout})

(module-export! '{hostinfo/json hostinfo/field hostinfo})

(define (hostinfo (hostname) (ips))
  (set! hostname (gethostname))
  (set! ips (hostaddrs (gethostname)))
  (frame-create #f
    'hostname hostname
    'ip (try (pick-one (difference ips "127.0.0.1")) ips) 'ips ips
    'port (req/get 'server_port)
    'syshost (req/get 'syshost)
    'http (req/get 'server_protocol)
    'software (choice (req/get 'server_software)
		      (glom "framerd-" (config 'fdversion))
		      (glom "libu8-" (config 'u8version)))
    'servername (req/get 'http_host)
    'serverip (req/get 'server_addr)
    'remoteip (req/get 'remote_addr)
    'remoteport (req/get 'remote_port)
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


