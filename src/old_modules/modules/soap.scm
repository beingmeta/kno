;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'soap)

;;; Wrappers and parsersfor SOAP requests

(use-module 'fdweb)

(module-export! '{emit-soap-xml soap/call})

(define (emit-soap-xml methodname args (opts '()))
  (let ((namespace (getopt opts 'namespace))
	(nsprefix (getopt opts 'nsprefix)))
    (xmlblock (if (and namespace nsprefix)
		  `(,(stringout nsprefix ":" methodname)
		    (,(stringout "xmlns:" nsprefix)
		     ,namespace))
		  (if namespace
		      `(,methodname (xmlns ,namespace))
		      `(,methodname)))
	(do-choices (key (getkeys args))
	  (let* ((keyname (if (symbol? key)
			      (downcase (symbol->string key))
			      key))
		 (eltname
		  (if (position #\: keyname) keyname
		      (if nsprefix (string-append nsprefix ":" keyname)
			  keyname))))
	    (xmlblock `(,eltname)
		(get args key)))))))

(define (soap/call uri methodname args (opts '()))
  (let* ((soapact (getopt opts 'soapaction))
	 (handle (or (getopt opts 'handle)
		     (if soapact
			 (curlopen 'header (stringout "SoapAction: " soapact))
			 (curlopen)))))
    (urlpostout handle uri "text/xml"
		(soapenvelope #f (emit-soap-xml methodname args opts)))))

;;; Test code

(define weatheruri
  "http://www.weather.gov/forecasts/xml/SOAP_server/ndfdXMLserver.php")
(define weather-soap-action
  "SoapAction: http://www.weather.gov/forecasts/xml/DWMLgen/wsdl/ndfdXML.wsdl#NDFDge")
(define weather-namespace
  "http://www.weather.gov/forecasts/xml/DWMLgen/wsdl/ndfdXML.wsdl")

(define weather-opts
  #[soapaction 
    "http://www.weather.gov/forecasts/xml/DWMLgen/wsdl/ndfdXML.wsdl#NDFDge"
    namespace
    "http://www.weather.gov/forecasts/xml/DWMLgen/wsdl/ndfdXML.wsdl"])

(define (get-weather-at lat long)
  (soap/call weatheruri "NDFDgen"
	     `#[latitude ,lat longitude ,long product "glance"]
	     weather-opts))



