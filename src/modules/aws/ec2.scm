;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'aws/ec2)

(use-module '{aws aws/v4 fdweb texttools mimetable regex logctl
	      ezrecords rulesets logger varconfig})
(define %used_modules '{aws varconfig ezrecords rulesets})

(module-export! '{ec2/op ec2/filtered
		  ec2/instances ec2/images ec2/tags
		  ec2/tag!})

(define action-methods
  #["DescribeInstances" "GET"])

(define (deitemize result (into #[]))
  (do-choices (key (getkeys result))
    (let ((value (get result key)))
      (if (not (table? value))
	  (store! into key value)
	  (if (not (test value 'item))
	      (if (test value '%xmltag key)
		  (let ((copy (deep-copy value)))
		    (drop! copy '{%qname %xmltag})
		    (set! copy (deitemize copy))
		    (unless (empty? (getkeys copy))
		      (store! into key (deitemize copy)))))
	      (do-choices (item (get value 'item))
		(let ((copy (deep-copy item)))
		  (drop! copy '{%qname %xmltag})
		  (set! copy (deitemize copy))
		  (unless (empty? (getkeys copy))
		    (add! into key (deitemize copy)))))))))
  (when (test into '%xmltag (get into '%qname))
    (drop! into '{%xmltag %qname}))
  into)

(define (ec2/op action (args #[]) (req #[]))
  (store! args "Action" action)
  (store! args "Version" "2015-10-01")
  (let* ((method (getopt req 'method
			 (try (get action-methods action)
			      "GET")))
	 (response
	  (aws/v4/op req method "https://ec2.amazonaws.com/" #f
		     args)))
    (if (>= 299 (getopt response 'response) 200)
	(reject (elts (xmlparse (getopt response '%content) 'data)) string?)
	(irritant (cons response req) |EC2 Error|))))

(define resource-types
  (downcase '{customer-gateway dhcp-options image instance internet-gateway network-acl 
	      network-interface reserved-instances route-table security-group snapshot 
	      spot-instances-request subnet volume vpc vpn-connection vpn-gateway}))
(define resource-id-prefixes
  (glom (downcase '{i eipalloc sg eni ami vol snap 
		    subnet vpc rtb igw dopt vpce acl})
    "-"))

(defambda (ec2/filtered action (filters '()) (args #[]) (i 1))
  (when (and (odd? (length filters))
	     (table? (car filters)))
    (let ((init (car filters)))
      (do-choices (key (getkeys init))
	(store! args (glom "Filter." i ".Name") 
		(if (symbol? key) (downcase key) key))
	(do-choices (v (get init key) j)
	  (store! args (glom "Filter." i ".Value." (1+ j))
		  (get init key)))
	(set! i (+ i 1)))
      (set! filters (cdr filters))))
  (while (pair? filters)
    (cond  ((not (or (string? (car filters)) (symbol? (car filters))))
	    (irritant (car filters) |Bad filter|))
	   ((overlaps? (downcase (car filters)) resource-types)
	    (store! args (glom "Filter." i ".Name") "resource-type")
	    (do-choices (v (downcase (car filters)) j)
	      (store! args (glom "Filter." i ".Value." (1+ j)) v))
	    (set! filters (cdr filters))
	    (set! i (1+ i)))
	   ((has-prefix (car filters) resource-id-prefixes)
	    (store! args (glom "Filter." i ".Name") "resource-id")
	    (do-choices (v (car filters) j)
	      (store! args (glom "Filter." i ".Value." (1+ j)) v))
	    (set! filters (cdr filters)))
	   ((position #\. (car filters))
	    (store! args (car filters) (cadr filters))
	    (set! filters (cddr filters)))
	   (else
	    (store! args (glom "Filter." i ".Name") 
		    (if (symbol? (car filters)) 
			(downcase (car filters))
			(car filters)))
	    (do-choices (v (cadr filters) j)
	      (store! args (glom "Filter." i ".Value." (1+ j)) v))
	    (set! i (+ i 1))
	    (set! filters (cddr filters)))))
  (ec2/op action args))

(define (ec2/instances . filters)
  (let ((response (ec2/filtered "DescribeInstances" filters)))
    (deitemize (get (get (get (get response 'reservationset)
			      'item) 
			 'instancesset)
		    'item))))

(define (ec2/images . filters)
  (let ((response (ec2/filtered "DescribeImages" filters)))
    (deitemize (get (get response 'imagesset) 'item))))

(define (ec2/tags . filters)
  (let ((response (ec2/filtered "DescribeTags" filters)))
    (deitemize (get (get response 'tagset) 'item))))

(defambda (ec2/tag! ids keyvals)
  (let ((args #[]) (j 1))
    (do-choices (id ids i)
      (if (string? id)
	  (store! args (glom "ResourceId." (1+ i)) id)
	  (store! args (glom "ResourceId." (1+ i)) id)))
    (do-choices (keyval keyvals)
      (do-choices (key (getkeys keyval))
	(do-choices (value (get keyval key))
	  (store! args (glom "Tag." j ".Key") key)
	  (store! args (glom "Tag." j ".Value") value))))
    (ec2/op "CreateTags" args)))





