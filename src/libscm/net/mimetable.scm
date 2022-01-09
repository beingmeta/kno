;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'net/mimetable)

(use-module '{texttools varconfig texttools})
(define %used_modules 'varconfig)

(module-export!
 '{*mimetable* *inv-mimetable*
   *default-mimetype* *default-charset*
   getsuffix
   ctype->suffix ctype->charset ctype->base
   path->ctype path->mimetype
   path->encoding
   mimetype/string?
   mimetype/text?})

(define *default-charset* #f)
(varconfig! mime:charset *default-charset* config:goodstring)

(define *default-mimetype* #f)
(varconfig! mime:default *default-mimetype* config:goodstring)

(define-init *inv-mimetable* (make-hashtable))

(define *override-data*
  '{("text/cache-manifest" "manifest")
    ("text/html" "html" "htm")
    ("text/config" "cfg")
    ("application/xhtml+xml" "xhtml")
    ;;("text/html" "xhtml")
    ("application/zip" "zip" "ZIP")
    ("text/plain" "text" "txt")
    ("image/jpeg" "jpeg" "jpg")
    ("image/png" "png")
    ("image/gif" "gif")
    ("image/x-icon" "ico")
    ("text/css" "css")
    ("image/svg+xml" "svg" "svgz")
    ("audio/aac" "aac")
    ("audio/mp4" "mp4" "m4a")
    ("audio/mpeg" "mp1" "mp3" "mpg" "mpeg")
    ("audio/ogg" "ogg" "oga")
    ("audio/wav" "wav")
    ("audio/webm" "webm")
    ("audio/mp4" "mp4" "m4v")
    ("audio/webm" "webm")
    ("audio/mpeg" "mp1" "mp2" "mp3" "mpg" "mpeg")
    ("text/javascript" "js" "javascript")
    ("application/javascript" "json" "jso")
    ("text/cache-manifest" "appcache")
    ("font/opentype" "otf")
    ("application/font-woff" "woff")
    ("font/ttf" "ttf")
    ("application/adobe-page-template+xml" "xpgt")
    ("application/x-dtbncx+xml" "ncx")
    ("application/dtype" "dtype")
    ("application/dtype+zip" "ztype")
    ("application/epub+zip" "epub")})

(define *mimetable-data*  ;; From Unix mimetable
  '{("text/csv" "csv")
    ("video/dl" "dl")		    
    ("video/dv" "dif" "dv")		    
    ("video/gl" "gl")		    
    ("audio/amr" "amr")		    
    ("image/ief" "ief")		    
    ("image/jp2" "jp2" "jpg2")	    
    ("image/jpm" "jpm")		    
    ("image/jpx" "jpx" "jpf")	    
    ("image/pcx" "pcx")		    
    ("text/h323" "323")		    
    ("text/iuls" "uls")		    
    ("text/x-sh" "sh")		    
    ("video/fli" "fli")		    
    ("video/ogg" "ogv")		    
    ("audio/flac" "flac")		    
    ("audio/midi" "mid" "midi" "kar")    
    ("audio/mpeg" "mpga" "mpega" "mp2" "mp3" "m4a") 
    ("image/tiff" "tiff" "tif")	    
    ("image/x-jg" "art")		    
    ("model/iges" "igs" "iges")	    
    ("model/mesh" "msh" "mesh" "silo")   
    ("model/vrml" "wrl" "vrml")	    
    ("text/plain" "asc" "txt" "text" "pot" "brf" "srt") 
    ("text/x-boo" "boo")		    
    ("text/x-csh" "csh")		    
    ("text/x-moc" "moc")		    
    ("text/x-sfv" "sfv")		    
    ("text/x-tcl" "tcl" "tk")	    
    ("text/x-tex" "tex" "ltx" "sty" "cls") 
    ("video/3gpp" "3gp")		      
    ("video/MP2T" "ts")		      
    ("audio/basic" "au" "snd")	      
    ("audio/x-gsm" "gsm")		      
    ("audio/x-sd2" "sd2")		      
    ("image/x-jng" "jng")		      
    ("image/x-rgb" "rgb")		      
    ("text/mathml" "mml")		      
    ("text/turtle" "ttl")		      
    ("text/x-chdr" "h")		      
    ("text/x-csrc" "c")		      
    ("text/x-diff" "diff" "patch")	      
    ("text/x-dsrc" "d")		      
    ("text/x-java" "java")		      
    ("text/x-perl" "pl" "pm")	      
    ("video/x-flv" "flv")		      
    ("video/x-mng" "mng")		      
    ("audio/amr-wb" "awb")		      
    ("audio/csound" "csd" "orc" "sco")     
    ("audio/x-aiff" "aif" "aiff" "aifc")   
    ("text/texmacs" "tm")		      
    ("text/x-scala" "scala")		      
    ("text/x-vcard" "vcf")		      
    ("audio/annodex" "axa")		      
    ("audio/mpegurl" "m3u")		      
    ("audio/prs.sid" "sid")		      
    ("audio/x-scpls" "pls")		      
    ("model/x3d+xml" "x3d")		      
    ("text/calendar" "ics" "icz")	      
    ("text/richtext" "rtx")		      
    ("text/x-bibtex" "bib")		      
    ("text/x-c++hdr" "h++" "hpp" "hxx" "hh") 
    ("text/x-c++src" "c++" "cpp" "cxx" "cc") 
    ("text/x-pascal" "p" "pas")		
    ("text/x-python" "py")		    
    ("text/x-setext" "etx")		    
    ("video/annodex" "axv")		    
    ("video/x-ms-wm" "wm")		    
    ("audio/x-ms-wax" "wax")		    
    ("audio/x-ms-wma" "wma")		    
    ("chemical/x-cdx" "cdx")		    
    ("chemical/x-cif" "cif")		    
    ("chemical/x-cml" "cml")		    
    ("chemical/x-ctx" "ctx")		    
    ("chemical/x-cxf" "cxf" "cef")	    
    ("chemical/x-hin" "hin")		    
    ("chemical/x-pdb" "pdb" "ent")	    
    ("chemical/x-vmd" "vmd")		    
    ("chemical/x-xyz" "xyz")		    
    ("image/vnd.djvu" "djvu" "djv")	    
    ("image/x-ms-bmp" "bmp")		    
    ("message/rfc822" "eml")		    
    ("model/x3d+vrml" "x3dv")	    
    ("text/scriptlet" "sct" "wsc")	    
    ("text/x-haskell" "hs")		    
    ("text/x-pcs-gcd" "gcd")		    
    ("video/x-la-asf" "lsf" "lsx")	    
    ("video/x-ms-asf" "asf" "asx")	    
    ("video/x-ms-wmv" "wmv")		    
    ("video/x-ms-wmx" "wmx")		    
    ("video/x-ms-wvx" "wvx")		    
    ("x-world/x-vrml" "vrm" "vrml" "wrl") 
    ("#chemical/x-mif" "mif")	     
    ("application/hta" "hta")	     
    ("application/m3g" "m3g")	     
    ("application/mxf" "mxf")	     
    ("application/oda" "oda")	     
    ("application/ogg" "ogx")	     
    ("application/pdf" "pdf")	     
    ("application/rar" "rar")	     
    ("application/rtf" "rtf")	     
    ("application/sla" "stl")	     
    ("application/xml" "xml" "xsd")	     
    ("audio/x-mpegurl" "m3u")	     
    ("chemical/x-cmdf" "cmdf")	     
    ("chemical/x-csml" "csml" "csm")	     
    ("chemical/x-mol2" "mol2")	     
    ("chemical/x-xtel" "xtel")	     
    ("image/x-xbitmap" "xbm")	     
    ("image/x-xpixmap" "xpm")	     
    ("text/x-lilypond" "ly")		     
    ("video/quicktime" "qt" "mov")	     
    ("video/x-msvideo" "avi")	     
    ("application/mbox" "mbox")	     
    ("application/x-sh" "sh")	     
    ("chemical/x-cache" "cac" "cache")    
    ("chemical/x-mmcif" "mcif")	     
    ("model/x3d+binary" "x3db")	     
    ("text/vnd.wap.wml" "wml")	     
    ("text/x-component" "htc")	     
    ("text/x-vcalendar" "vcs")	     
    ("video/x-matroska" "mpv" "mkv")	     
    ("application/dicom" "dcm")	     
    ("application/x-123" "wk")	     
    ("application/x-cab" "cab")	     
    ("application/x-cbr" "cbr")	     
    ("application/x-cbz" "cbz")	     
    ("application/x-cdf" "cdf" "cda")     
    ("application/x-csh" "csh")	     
    ("application/x-dms" "dms")	     
    ("application/x-dvi" "dvi")	     
    ("application/x-hdf" "hdf")	     
    ("application/x-hwp" "hwp")	     
    ("application/x-ica" "ica")	     
    ("application/x-jam" "jam")	     
    ("application/x-lha" "lha")	     
    ("application/x-lyx" "lyx")	     
    ("application/x-lzh" "lzh")	     
    ("application/x-lzx" "lzx")	     
    ("application/x-md5" "md5")	     
    ("application/x-mif" "mif")	     
    ("application/x-msi" "msi")	     
    ("application/x-nwc" "nwc")	     
    ("application/x-rdp" "rdp")	     
    ("application/x-sql" "sql")	     
    ("application/x-tar" "tar")	     
    ("application/x-tcl" "tcl")	     
    ("application/x-xcf" "xcf")	     
    ("audio/x-realaudio" "ra")	     
    ("chemical/x-cerius" "cer")	     
    ("chemical/x-chem3d" "c3d")	     
    ("chemical/x-rosdal" "ros")	     
    ("image/x-canon-cr2" "cr2")	     
    ("image/x-canon-crw" "crw")	     
    ("image/x-coreldraw" "cdr")	     
    ("image/x-epson-erf" "erf")	     
    ("image/x-nikon-nef" "nef")	     
    ("image/x-photoshop" "psd")	     
    ("video/vnd.mpegurl" "mxu")	     
    ("video/x-sgi-movie" "movie")	     
    ("x-epoc/x-sisx-app" "sisx")	     
    ("application/bbolin" "lin")	     
    ("application/msword" "doc" "dot")    
    ("application/x-cpio" "cpio")	     
    ("application/x-doom" "wad")	     
    ("application/x-font" "pfa" "pfb" "gsf" "pcf" "pcf.Z") 
    ("application/x-gtar" "gtar")	    
    ("application/x-info" "info")	    
    ("application/x-jmol" "jmz")	    
    ("application/x-koan" "skp" "skd" "skt" "skm") 
    ("application/x-qgis" "qgs" "shp" "shx") 
    ("application/x-ruby" "rb")	    
    ("application/x-sha1" "sha1")	    
    ("application/x-shar" "shar")	    
    ("application/x-xfig" "fig")	    
    ("chemical/x-alchemy" "alc")	    
    ("chemical/x-compass" "cpa")	    
    ("chemical/x-genbank" "gen")	    
    ("chemical/x-isostar" "istr" "ist")  
    ("chemical/x-mdl-tgf" "tgf")	    
    ("image/vnd.wap.wbmp" "wbmp")	    
    ("image/x-cmu-raster" "ras")	    
    ("application/annodex" "anx")	    
    ("application/dsptype" "tsp")	    
    ("application/java-vm" "class")	    
    ("application/onenote" "one" "onetoc2" "onetmp" "onepkg") 
    ("application/rdf+xml" "rdf") 
    ("application/x-bcpio" "bcpio") 
    ("application/x-kword" "kwd" "kwt") 
    ("application/x-latex" "latex")	   
    ("application/x-maker" "frm" "maker" "frame" "fm" "fb" "book" "fbdoc") 
    ("application/x-trash" "~" "%" "bak" "old" "sik") 
    ("application/x-troff" "t" "tr" "roff") 
    ("application/x-ustar" "ustar")	       
    ("application/x-wingz" "wz")	       
    ("chemical/x-chemdraw" "chm")	       
    ("chemical/x-jcamp-dx" "jdx" "dx")      
    ("chemical/x-kinemage" "kin")	       
    ("image/x-olympus-orf" "orf")	       
    ("image/x-xwindowdump" "xwd")	       
    ("application/atom+xml" "atom")	       
    ("application/cu-seeme" "cu")	       
    ("application/msaccess" "mdb")	       
    ("application/pgp-keys" "key")	       
    ("application/smil+xml" "smi" "smil")   
    ("application/vnd.smaf" "mmf")	       
    ("application/x-cdlink" "vcd")	       
    ("application/x-comsol" "mph")	       
    ("application/x-go-sgf" "sgf")	       
    ("application/x-iphone" "iii")	       
    ("application/x-kchart" "chrt")	       
    ("application/x-ms-wmd" "wmd")	       
    ("application/x-ms-wmz" "wmz")	       
    ("application/x-netcdf" "nc")	       
    ("application/x-object" "o")	       
    ("application/x-scilab" "sci" "sce")    
    ("application/x-sv4crc" "sv4crc")       
    ("application/x-tex-gf" "gf")	       
    ("application/x-tex-pk" "pk")	       
    ("application/xslt+xml" "xsl" "xslt")   
    ("application/xspf+xml" "xspf")	       
    ("audio/x-pn-realaudio" "ra" "rm" "ram") 
    ("chemical/x-cache-csf" "csf") 
    ("chemical/x-crossfire" "bsd") 
    ("chemical/x-molconn-Z" "b")   
    ("chemical/x-mopac-out" "moo") 
    ("chemical/x-mopac-vib" "mvb") 
    ("chemical/x-ncbi-asn1" "asn") 
    ("chemical/x-swissprot" "sw")  
    ("application/vnd.visio" "vsd") 
    ("application/x-abiword" "abw") 
    ("application/x-kspread" "ksp") 
    ("application/x-mpegURL" "m3u8") 
    ("application/x-rss+xml" "rss")	
    ("application/x-stuffit" "sit" "sitx") 
    ("application/x-sv4cpio" "sv4cpio")    
    ("application/x-texinfo" "texinfo" "texi") 
    ("chemical/x-mdl-rdfile" "rd") 
    ("chemical/x-mdl-sdfile" "sd" "sdf") 
    ("application/ecmascript" "es")	    
    ("application/pics-rules" "prf")	    
    ("application/postscript" "ps" "ai" "eps" "epsi" "epsf" "eps2" "eps3") 
    ("application/x-director" "dcr" "dir" "dxr") 
    ("application/x-freemind" "mm")	       
    ("application/x-gnumeric" "gnumeric")   
    ("application/x-troff-me" "me")	       
    ("application/x-troff-ms" "ms")	       
    ("chemical/x-macmolecule" "mcm")	       
    ("chemical/x-mdl-molfile" "mol")	       
    ("chemical/x-mdl-rxnfile" "rxn")	       
    ("chemical/x-mopac-graph" "gpt")	       
    ("chemical/x-mopac-input" "mop" "mopcrt" "mpc" "zmt") 
    ("text/vnd.wap.wmlscript" "wmls")       
    ("application/atomcat+xml" "atomcat")   
    ("application/mathematica" "nb" "nbp")  
    ("application/vnd.rim.cod" "cod")       
    ("application/x-chess-pgn" "pgn")       
    ("application/x-pkcs7-crl" "crl")       
    ("application/x-troff-man" "man")       
    ("application/x-xpinstall" "xpi")       
    ("chemical/x-galactic-spc" "spc")       
    ("chemical/x-gamess-input" "inp" "gam" "gamin") 
    ("chemical/x-gaussian-log" "gal")       
    ("image/x-corelphotopaint" "cpt")       
    ("image/x-portable-anymap" "pnm")       
    ("image/x-portable-bitmap" "pbm")       
    ("image/x-portable-pixmap" "ppm")       
    ("text/x-literate-haskell" "lhs")       
    ("x-conference/x-cooltalk" "ice")       
    ("#application/x-httpd-php" "phtml" "pht" "php") 
    ("application/andrew-inset" "ez")       
    ("application/atomserv+xml" "atomsrv")  
    ("application/davmount+xml" "davmount") 
    ("application/futuresplash" "spl")      
    ("application/java-archive" "jar")      
    ("application/mac-binhex40" "hqx")      
    ("application/octet-stream" "bin")      
    ("application/vnd.ms-excel" "xls" "xlb" "xlt") 
    ("application/vnd.wap.wmlc" "wmlc")     
    ("application/x-bittorrent" "torrent")  
    ("application/x-kpresenter" "kpr" "kpt") 
    ("chemical/x-cactvs-binary" "cbin" "cascii" "ctab") 
    ("chemical/x-gaussian-cube" "cub")      
    ("chemical/x-gcg8-sequence" "gcg")      
    ("image/x-coreldrawpattern" "pat")      
    ("image/x-portable-graymap" "pgm")      
    ("#application/x-httpd-php3" "php3")    
    ("#application/x-httpd-php4" "php4")    
    ("#application/x-httpd-php5" "php5")    
    ("application/pgp-encrypted" "pgp")     
    ("application/pgp-signature" "sig")     
    ("application/vnd.wap.wbxml" "wbxml")   
    ("application/x-python-code" "pyc" "pyo") 
    ("application/x-scilab-xcos" "xcos")    
    ("application/x-silverlight" "scr")     
    ("application/x-wais-source" "src")     
    ("chemical/x-gaussian-input" "gau" "gjc" "gjf") 
    ("chemical/x-ncbi-asn1-spec" "asn")     
    ("chemical/x-vamas-iso14976" "vms")     
    ("image/x-coreldrawtemplate" "cdt")     
    ("text/tab-separated-values" "tsv")     
    ("#application/x-httpd-eruby" "rhtml")  
    ("application/mac-compactpro" "cpt")    
    ("application/vnd.cinderella" "cdy")    
    ("application/x-futuresplash" "spl")    
    ("application/x-ganttproject" "gan")    
    ("application/x-killustrator" "kil")    
    ("application/x-x509-ca-cert" "crt")    
    ("chemical/x-ncbi-asn1-ascii" "prt" "ent") 
    ("#application/vnd.ms-pki.stl" "stl")   
    ("#chemical/x-daylight-smiles" "smi")   
    ("application/vnd.wordperfect" "wpd")   
    ("application/x-7z-compressed" "7z")    
    ("application/x-iso9660-image" "iso")   
    ("application/x-msdos-program" "com" "exe" "bat" "dll") 
    ("chemical/x-macromodel-input" "mmd" "mmod") 
    ("chemical/x-ncbi-asn1-binary" "val" "aso") 
    ("application/vnd.sun.xml.calc" "sxc")  
    ("application/vnd.sun.xml.draw" "sxd")  
    ("application/vnd.sun.xml.math" "sxm")  
    ("application/vnd.tcpdump.pcap" "cap" "pcap") 
    ("application/x-debian-package" "deb" "udeb") 
    ("application/x-java-jnlp-file" "jnlp") 
    ("application/x-oz-application" "oza")  
    ("application/vnd.ms-fontobject" "eot") 
    ("application/vnd.ms-pki.seccat" "cat") 
    ("application/vnd.ms-powerpoint" "ppt" "pps") 
    ("application/x-apple-diskimage" "dmg") 
    ("application/x-gtar-compressed" "tgz" "taz") 
    ("application/x-internet-signup" "ins" "isp") 
    ("application/x-quicktimeplayer" "qtl") 
    ("application/x-shockwave-flash" "swf" "swfl") 
    ("chemical/x-embl-dl-nucleotide" "emb" "embl") 
    ("application/vnd.ms-officetheme" "thmx") 
    ("application/vnd.sun.xml.writer" "sxw") 
    ("application/vnd.wap.wmlscriptc" "wmlsc") 
    ("application/vnd.wordperfect5.1" "wp5") 
    ("chemical/x-gaussian-checkpoint" "fch" "fchk") 
    ("#application/x-httpd-php-source" "phps") 
    ("application/vnd.mozilla.xul+xml" "xul") 
    ("application/vnd.sun.xml.impress" "sxi") 
    ("application/vnd.symbian.install" "sis") 
    ("application/x-pkcs7-certreqresp" "p7r") 
    ("application/vnd.google-earth.kmz" "kmz") 
    ("text/vnd.sun.j2me.app-descriptor" "jad") 
    ("application/vnd.stardivision.calc" "sdc") 
    ("application/vnd.stardivision.draw" "sda") 
    ("application/vnd.stardivision.math" "sdf") 
    ("application/x-graphing-calculator" "gcf") 
    ("application/x-ns-proxy-autoconfig" "pac" "dat") 
    ("application/java-serialized-object" "ser") 
    ("application/vnd.stardivision.chart" "sds") 
    ("application/vnd.stardivision.writer" "sdw") 
    ("application/vnd.google-earth.kml+xml" "kml") 
    ("application/vnd.stardivision.impress" "sdd") 
    ("application/x-redhat-package-manager" "rpm") 
    ("application/vnd.sun.xml.calc.template" "stc") 
    ("application/vnd.sun.xml.draw.template" "std") 
    ("application/vnd.sun.xml.writer.global" "sxg") 
    ("#application/x-httpd-php3-preprocessed" "php3p") 
    ("application/vnd.android.package-archive" "apk") 
    ("application/vnd.oasis.opendocument.text" "odt") 
    ("application/vnd.sun.xml.writer.template" "stw") 
    ("application/vnd.oasis.opendocument.chart" "odc") 
    ("application/vnd.oasis.opendocument.image" "odi") 
    ("application/vnd.sun.xml.impress.template" "sti") 
    ("application/vnd.oasis.opendocument.formula" "odf") 
    ("application/vnd.stardivision.writer-global" "sgl") 
    ("application/vnd.oasis.opendocument.database" "odb") 
    ("application/vnd.oasis.opendocument.graphics" "odg") 
    ("application/vnd.oasis.opendocument.text-web" "oth") 
    ("application/vnd.ms-excel.addin.macroEnabled.12" "xlam") 
    ("application/vnd.ms-excel.sheet.macroEnabled.12" "xlsm") 
    ("application/vnd.oasis.opendocument.spreadsheet" "ods") 
    ("application/vnd.oasis.opendocument.text-master" "odm") 
    ("application/vnd.oasis.opendocument.presentation" "odp") 
    ("application/vnd.ms-word.document.macroEnabled.12" "docm") 
    ("application/vnd.ms-word.template.macroEnabled.12" "dotm") 
    ("application/vnd.oasis.opendocument.text-template" "ott") 
    ("application/vnd.ms-excel.template.macroEnabled.12" "xltm") 
    ("application/vnd.ms-powerpoint.addin.macroEnabled.12" "ppam") 
    ("application/vnd.ms-powerpoint.slide.macroEnabled.12" "sldm") 
    ("application/vnd.oasis.opendocument.graphics-template" "otg") 
    ("application/vnd.ms-excel.sheet.binary.macroEnabled.12" "xlsb") 
    ("application/vnd.ms-powerpoint.template.macroEnabled.12" "potm") 
    ("application/vnd.ms-powerpoint.slideshow.macroEnabled.12" "ppsm") 
    ("application/vnd.oasis.opendocument.spreadsheet-template" "ots") 
    ("application/vnd.oasis.opendocument.presentation-template" "otp") 
    ("application/vnd.ms-powerpoint.presentation.macroEnabled.12" "pptm") 
    ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" "xlsx") 
    ("application/vnd.openxmlformats-officedocument.presentationml.slide" "sldx") 
    ("application/vnd.openxmlformats-officedocument.spreadsheetml.template" "xltx")
    ("application/vnd.openxmlformats-officedocument.presentationml.template"
     "potx")
    ("application/vnd.openxmlformats-officedocument.presentationml.slideshow"
     "ppsx")
    ("application/vnd.openxmlformats-officedocument.wordprocessingml.document"
     "docx")
    ("application/vnd.openxmlformats-officedocument.wordprocessingml.template"
     "dotx")
    ("application/vnd.openxmlformats-officedocument.presentationml.presentation"
     "pptx")
    })

(define *mimetable*
  (let ((table (make-hashtable)))
    (do-choices (map *override-data*)
      (let ((suffixes (if (pair? (cdr map)) (elts (cdr map)) (cdr map))))
	(store! *inv-mimetable* (car map)
		(pick-one (downcase (smallest suffixes length))))
	(store! table (choice suffixes (string-append "." suffixes))
		(car map))))
    (do-choices (map *mimetable-data*)
      (let ((suffixes (reject (if (pair? (cdr map)) (elts (cdr map)) (cdr map))
			      table)))
	(when (exists? suffixes)
	  (store! *inv-mimetable* (car map)
		  (pick-one (downcase (smallest suffixes length))))
	  (store! table (choice suffixes (string-append "." suffixes))
		  (car map)))))
    table))

(define (suffix->ctype suffix (typemap #f))
  (try (tryif typemap (get typemap suffix))
       (get *mimetable* suffix)))

(define (guess-ctype path (typemap #f))
  (if (string? path)
      (if (has-suffix path ".gz")
	  (suffix->ctype (path-suffix (slice path 0 -3)) typemap)
	  (if (has-suffix path ".Z")
	      (suffix->ctype (path-suffix (slice path 0 -2)) typemap)
	      (suffix->ctype (path-suffix path) typemap)))
      (if (and (pair? path) (string? (cdr path)))
	  (suffix->ctype (path-suffix (cdr path)) typemap)
	  (fail))))

(define (path->mimetype path (default-value {}) (typemap #f))
  (cond ((string? default-value))
	((table? default-value)
	 (set! typemap default-value)
	 (set! default-value {})))
  (let ((ctype (guess-ctype path typemap)))
    (or (and (exists? ctype) ctype (has-prefix ctype "text")
	     (and *default-charset* (not (search "charset" ctype))
		  (string-append ctype "; charset=" *default-charset*)))
	(and (exists? ctype) ctype)
	default-value)))
(define path->ctype path->mimetype)

(define (path->encoding path)
  (if (has-suffix path ".gz") "gzip"
      (if (has-suffix path ".Z") "compress"
	  #f)))

(define getsuffix path-suffix)

(define (ctype->suffix ctype (dot #f))
  (tryif (string? ctype)
    (glom dot
      (try (pick-one (smallest (get *inv-mimetable* ctype) length))
	   (and (position #\/ ctype) (slice ctype (1+  (position #\/ ctype))))))))

(define (ctype->base string)
  (downcase (slice string 0 (position #\; string))))

(define (ctype->charset string)
  (try (get (text->frames #("charset" (spaces*) "="
			    (spaces*) (label charset (not> {";" (eos)})))
			  string)
	    'charset)
       #f))

(define (mimetype/text? mimetype)
  (and (string? mimetype)
       (or (has-prefix mimetype "text")
	   (has-prefix mimetype
		       {"application/xml"
			"application/xhtml"
			"application/xml+xhtml"}))))

(define (mimetype/string? mimetype)
  (and (string? mimetype)
       (or (has-prefix mimetype "text")
	   (has-prefix mimetype
		       {"application/xml"
			"application/xhtml"
			"application/json"
			"application/javascript"
			"application/x-json"
			"application/x-javascript"}))))

