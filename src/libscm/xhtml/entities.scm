;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved

(in-module 'xhtml/entities)

(module-export! 'xhtml/entities)

(define xhtml/entities
  #["AElig" "\u00c6"
    "Aacgr" "\u0386"
    "Aacute" "\u00c1"
    "Abreve" "\u0102"
    "Acirc" "\u00c2"
    "Acy" "\u0410"
    "Agr" "\u0391"
    "Agrave" "\u00c0"
    "Alpha" "\u0391"
    "Amacr" "\u0100"
    "Aogon" "\u0104"
    "Aring" "\u00c5"
    "Atilde" "\u00c3"
    "Auml" "\u00c4"
    "Barwed" "\u2306"
    "Bcy" "\u0411"
    "Beta" "\u0392"
    "Bgr" "\u0392"
    "CHcy" "\u0427"
    "Cacute" "\u0106"
    "Cap" "\u22d2"
    "Ccaron" "\u010c"
    "Ccedil" "\u00c7"
    "Ccirc" "\u0108"
    "Cdot" "\u010a"
    "Chi" "\u03a7"
    "Cup" "\u22d3"
    "DJcy" "\u0402"
    "DScy" "\u0405"
    "DZcy" "\u040f"
    "Dagger" "\u2021"
    "Dcaron" "\u010e"
    "Dcy" "\u0414"
    "Delta" "\u0394"
    "Dgr" "\u0394"
    "Dot" "\u00a8"
    "DotDot" "\u20dc"
    "Dstrok" "\u0110"
    "EEacgr" "\u0389"
    "EEgr" "\u0397"
    "ENG" "\u014a"
    "ETH" "\u00d0"
    "Eacgr" "\u0388"
    "Eacute" "\u00c9"
    "Ecaron" "\u011a"
    "Ecirc" "\u00ca"
    "Ecy" "\u042d"
    "Edot" "\u0116"
    "Egr" "\u0395"
    "Egrave" "\u00c8"
    "Emacr" "\u0112"
    "Eogon" "\u0118"
    "Epsilon" "\u0395"
    "Eta" "\u0397"
    "Euml" "\u00cb"
    "Fcy" "\u0424"
    "GJcy" "\u0403"
    "Gamma" "\u0393"
    "Gbreve" "\u011e"
    "Gcedil" "\u0122"
    "Gcirc" "\u011c"
    "Gcy" "\u0413"
    "Gdot" "\u0120"
    "Gg" "\u22d9"
    "Ggr" "\u0393"
    "Gt" "\u226b"
    "HARDcy" "\u042a"
    "Hcirc" "\u0124"
    "Hstrok" "\u0126"
    "IEcy" "\u0415"
    "IJlig" "\u0132"
    "IOcy" "\u0401"
    "Iacgr" "\u038a"
    "Iacute" "\u00cd"
    "Icirc" "\u00ce"
    "Icy" "\u0418"
    "Idigr" "\u03aa"
    "Idot" "\u0130"
    "Igr" "\u0399"
    "Igrave" "\u00cc"
    "Imacr" "\u012a"
    "Iogon" "\u012e"
    "Iota" "\u0399"
    "Itilde" "\u0128"
    "Iukcy" "\u0406"
    "Iuml" "\u00cf"
    "Jcirc" "\u0134"
    "Jcy" "\u0419"
    "Jsercy" "\u0408"
    "Jukcy" "\u0404"
    "KHcy" "\u0425"
    "KHgr" "\u03a7"
    "KJcy" "\u040c"
    "Kappa" "\u039a"
    "Kcedil" "\u0136"
    "Kcy" "\u041a"
    "Kgr" "\u039a"
    "LJcy" "\u0409"
    "Lacute" "\u0139"
    "Lambda" "\u039b"
    "Larr" "\u219e"
    "Lcaron" "\u013d"
    "Lcedil" "\u013b"
    "Lcy" "\u041b"
    "Lgr" "\u039b"
    "Ll" "\u22d8"
    "Lmidot" "\u013f"
    "Lstrok" "\u0141"
    "Lt" "\u226a"
    "Mcy" "\u041c"
    "Mgr" "\u039c"
    "Mu" "\u039c"
    "NJcy" "\u040a"
    "Nacute" "\u0143"
    "Ncaron" "\u0147"
    "Ncedil" "\u0145"
    "Ncy" "\u041d"
    "Ngr" "\u039d"
    "Ntilde" "\u00d1"
    "Nu" "\u039d"
    "OElig" "\u0152"
    "OHacgr" "\u038f"
    "OHgr" "\u03a9"
    "Oacgr" "\u038c"
    "Oacute" "\u00d3"
    "Ocirc" "\u00d4"
    "Ocy" "\u041e"
    "Odblac" "\u0150"
    "Ogr" "\u039f"
    "Ograve" "\u00d2"
    "Omacr" "\u014c"
    "Omega" "\u03a9"
    "Omicron" "\u039f"
    "Oslash" "\u00d8"
    "Otilde" "\u00d5"
    "Ouml" "\u00d6"
    "PHgr" "\u03a6"
    "PSgr" "\u03a8"
    "Pcy" "\u041f"
    "Pgr" "\u03a0"
    "Phi" "\u03a6"
    "Pi" "\u03a0"
    "Prime" "\u2033"
    "Psi" "\u03a8"
    "Racute" "\u0154"
    "Rarr" "\u21a0"
    "Rcaron" "\u0158"
    "Rcedil" "\u0156"
    "Rcy" "\u0420"
    "Rgr" "\u03a1"
    "Rho" "\u03a1"
    "SHCHcy" "\u0429"
    "SHcy" "\u0428"
    "SOFTcy" "\u042c"
    "Sacute" "\u015a"
    "Scaron" "\u0160"
    "Scedil" "\u015e"
    "Scirc" "\u015c"
    "Scy" "\u0421"
    "Sgr" "\u03a3"
    "Sigma" "\u03a3"
    "Sub" "\u22d0"
    "Sup" "\u22d1"
    "THORN" "\u00de"
    "THgr" "\u0398"
    "TSHcy" "\u040b"
    "TScy" "\u0426"
    "Tau" "\u03a4"
    "Tcaron" "\u0164"
    "Tcedil" "\u0162"
    "Tcy" "\u0422"
    "Tgr" "\u03a4"
    "Theta" "\u0398"
    "Tstrok" "\u0166"
    "Uacgr" "\u038e"
    "Uacute" "\u00da"
    "Ubrcy" "\u040e"
    "Ubreve" "\u016c"
    "Ucirc" "\u00db"
    "Ucy" "\u0423"
    "Udblac" "\u0170"
    "Ugr" "\u03a5"
    "Ugrave" "\u00d9"
    "Umacr" "\u016a"
    "Uogon" "\u0172"
    "Upsi" "\u03a5"
    "Upsilon" "\u03a5"
    "Uring" "\u016e"
    "Utilde" "\u0168"
    "Uuml" "\u00dc"
    "Vcy" "\u0412"
    "Vdash" "\u22a9"
    "Verbar" "\u2016"
    "Vvdash" "\u22aa"
    "Wcirc" "\u0174"
    "Xgr" "\u039e"
    "Xi" "\u039e"
    "YAcy" "\u042f"
    "YIcy" "\u0407"
    "YUcy" "\u042e"
    "Yacute" "\u00dd"
    "Ycirc" "\u0176"
    "Ycy" "\u042b"
    "Yuml" "\u0178"
    "ZHcy" "\u0416"
    "Zacute" "\u0179"
    "Zcaron" "\u017d"
    "Zcy" "\u0417"
    "Zdot" "\u017b"
    "Zeta" "\u0396"
    "Zgr" "\u0396"
    "aacgr" "\u03ac"
    "aacute" "\u00e1"
    "abreve" "\u0103"
    "acirc" "\u00e2"
    "acute" "\u00b4"
    "acy" "\u0430"
    "aelig" "\u00e6"
    "agr" "\u03b1"
    "agrave" "\u00e0"
    "alefsym" "\u2135"
    "aleph" "\u2135"
    "alpha" "\u03b1"
    "amacr" "\u0101"
    "amalg" "\u2210"
    "amp" "\u0026"
    "and" "\u2227"
    "ang" "\u2220"
    "ang90" "\u221f"
    "angmsd" "\u2221"
    "angsph" "\u2222"
    "angst" "\u212b"
    "aogon" "\u0105"
    "ap" "\u2248"
    "ape" "\u224a"
    "apos" "\u0027"
    "aposmod" "\u02bc"
    "aring" "\u00e5"
    "ast" "\u002a"
    "asymp" "\u2248"
    "atilde" "\u00e3"
    "auml" "\u00e4"
    "b.Delta" "\u0394"
    "b.Gamma" "\u0393"
    "b.Lambda" "\u039b"
    "b.Omega" "\u03a9"
    "b.Phi" "\u03a6"
    "b.Pi" "\u03a0"
    "b.Psi" "\u03a8"
    "b.Sigma" "\u03a3"
    "b.Theta" "\u0398"
    "b.Upsi" "\u03a5"
    "b.Xi" "\u039e"
    "b.alpha" "\u03b1"
    "b.beta" "\u03b2"
    "b.chi" "\u03c7"
    "b.delta" "\u03b4"
    "b.epsi" "\u03b5"
    "b.epsis" "\u03b5"
    "b.epsiv" "\u03b5"
    "b.eta" "\u03b7"
    "b.gamma" "\u03b3"
    "b.gammad" "\u03dc"
    "b.iota" "\u03b9"
    "b.kappa" "\u03ba"
    "b.kappav" "\u03f0"
    "b.lambda" "\u03bb"
    "b.mu" "\u03bc"
    "b.nu" "\u03bd"
    "b.omega" "\u03ce"
    "b.phis" "\u03c6"
    "b.phiv" "\u03d5"
    "b.pi" "\u03c0"
    "b.piv" "\u03d6"
    "b.psi" "\u03c8"
    "b.rho" "\u03c1"
    "b.rhov" "\u03f1"
    "b.sigma" "\u03c3"
    "b.sigmav" "\u03c2"
    "b.tau" "\u03c4"
    "b.thetas" "\u03b8"
    "b.thetav" "\u03d1"
    "b.upsi" "\u03c5"
    "b.xi" "\u03be"
    "b.zeta" "\u03b6"
    "barwed" "\u22bc"
    "bcong" "\u224c"
    "bcy" "\u0431"
    "bdquo" "\u201e"
    "becaus" "\u2235"
    "bepsi" "\u220d"
    "bernou" "\u212c"
    "beta" "\u03b2"
    "beth" "\u2136"
    "bgr" "\u03b2"
    "blackstar" "\u2726"
    "blank" "\u2423"
    "blk12" "\u2592"
    "blk14" "\u2591"
    "blk34" "\u2593"
    "block" "\u2588"
    "bottom" "\u22a5"
    "bowtie" "\u22c8"
    "boxDL" "\u2557"
    "boxDR" "\u2554"
    "boxDl" "\u2556"
    "boxDr" "\u2553"
    "boxH" "\u2550"
    "boxHD" "\u2566"
    "boxHU" "\u2569"
    "boxHd" "\u2564"
    "boxHu" "\u2567"
    "boxUL" "\u255d"
    "boxUR" "\u255a"
    "boxUl" "\u255c"
    "boxUr" "\u2559"
    "boxV" "\u2551"
    "boxVH" "\u256c"
    "boxVL" "\u2563"
    "boxVR" "\u2560"
    "boxVh" "\u256b"
    "boxVl" "\u2562"
    "boxVr" "\u255f"
    "boxdL" "\u2555"
    "boxdR" "\u2552"
    "boxdl" "\u2510"
    "boxdr" "\u250c"
    "boxh" "\u2500"
    "boxhD" "\u2565"
    "boxhU" "\u2568"
    "boxhd" "\u252c"
    "boxhu" "\u2534"
    "boxuL" "\u255b"
    "boxuR" "\u2558"
    "boxul" "\u2518"
    "boxur" "\u2514"
    "boxv" "\u2502"
    "boxvH" "\u256a"
    "boxvL" "\u2561"
    "boxvR" "\u255e"
    "boxvh" "\u253c"
    "boxvl" "\u2524"
    "boxvr" "\u251c"
    "bprime" "\u2035"
    "breve" "\u02d8"
    "brvbar" "\u00a6"
    "bsim" "\u223d"
    "bsime" "\u22cd"
    "bsol" "\u005c"
    "bull" "\u2022"
    "bump" "\u224e"
    "bumpe" "\u224f"
    "cacute" "\u0107"
    "cap" "\u2229"
    "caret" "\u2041"
    "caron" "\u02c7"
    "ccaron" "\u010d"
    "ccedil" "\u00e7"
    "ccirc" "\u0109"
    "cdot" "\u010b"
    "cedil" "\u00b8"
    "cent" "\u00a2"
    "chcy" "\u0447"
    "check" "\u2713"
    "chi" "\u03c7"
    "cir" "\u25cb"
    "circ" "\u02c6"
    "cire" "\u2257"
    "clubs" "\u2663"
    "colon" "\u003a"
    "colone" "\u2254"
    "comma" "\u002c"
    "commat" "\u0040"
    "comp" "\u2201"
    "compfn" "\u2218"
    "cong" "\u2245"
    "conint" "\u222e"
    "coprod" "\u2210"
    "copy" "\u00a9"
    "copysr" "\u2117"
    "crarr" "\u21b5"
    "cross" "\u2717"
    "cuepr" "\u22de"
    "cuesc" "\u22df"
    "cularr" "\u21b6"
    "cup" "\u222a"
    "cupre" "\u227c"
    "curarr" "\u21b7"
    "curren" "\u00a4"
    "cuvee" "\u22ce"
    "cuwed" "\u22cf"
    "dArr" "\u21d3"
    "dagger" "\u2020"
    "daleth" "\u2138"
    "darr" "\u2193"
    "darr2" "\u21ca"
    "dash" "\u2010"
    "dashv" "\u22a3"
    "dblac" "\u02dd"
    "dcaron" "\u010f"
    "dcy" "\u0434"
    "deg" "\u00b0"
    "delta" "\u03b4"
    "dgr" "\u03b4"
    "dharl" "\u21c3"
    "dharr" "\u21c2"
    "diam" "\u22c4"
    "diams" "\u2666"
    "die" "\u00a8"
    "divide" "\u00f7"
    "divonx" "\u22c7"
    "djcy" "\u0452"
    "dlarr" "\u2199"
    "dlcorn" "\u231e"
    "dlcrop" "\u230d"
    "dollar" "\u0024"
    "dot" "\u02d9"
    "drarr" "\u2198"
    "drcorn" "\u231f"
    "drcrop" "\u230c"
    "dscy" "\u0455"
    "dstrok" "\u0111"
    "dtri" "\u25bf"
    "dtrif" "\u25be"
    "dzcy" "\u045f"
    "eDot" "\u2251"
    "eacgr" "\u03ad"
    "eacute" "\u00e9"
    "ecaron" "\u011b"
    "ecir" "\u2256"
    "ecirc" "\u00ea"
    "ecolon" "\u2255"
    "ecy" "\u044d"
    "edot" "\u0117"
    "eeacgr" "\u03ae"
    "eegr" "\u03b7"
    "efDot" "\u2252"
    "egr" "\u03b5"
    "egrave" "\u00e8"
    "egs" "\u22dd"
    "ell" "\u2113"
    "els" "\u22dc"
    "emacr" "\u0113"
    "empty" "\u2205"
    "emsp" "\u2003"
    "emsp13" "\u2004"
    "emsp14" "\u2005"
    "eng" "\u014b"
    "ensp" "\u2002"
    "eogon" "\u0119"
    "epsi" "\u03b5"
    "epsilon" "\u03b5"
    "epsis" "\u220a"
    "epsiv" "\u03bf"
    "equals" "\u003d"
    "equiv" "\u2261"
    "erDot" "\u2253"
    "esdot" "\u2250"
    "eta" "\u03b7"
    "eth" "\u00f0"
    "euml" "\u00eb"
    "euro" "\u20ac"
    "excl" "\u0021"
    "exist" "\u2203"
    "fcy" "\u0444"
    "female" "\u2640"
    "ffilig" "\ufb03"
    "fflig" "\ufb00"
    "ffllig" "\ufb04"
    "filig" "\ufb01"
    "flat" "\u266d"
    "fllig" "\ufb02"
    "fnof" "\u0192"
    "forall" "\u2200"
    "fork" "\u22d4"
    "frac12" "\u00bd"
    "frac13" "\u2153"
    "frac14" "\u00bc"
    "frac15" "\u2155"
    "frac16" "\u2159"
    "frac18" "\u215b"
    "frac23" "\u2154"
    "frac25" "\u2156"
    "frac34" "\u00be"
    "frac35" "\u2157"
    "frac38" "\u215c"
    "frac45" "\u2158"
    "frac56" "\u215a"
    "frac58" "\u215d"
    "frac78" "\u215e"
    "frasl" "\u2044"
    "frown" "\u2322"
    "gE" "\u2267"
    "gEl" "\u2a8c"
    "gacute" "\u01f5"
    "gamma" "\u03b3"
    "gammad" "\u03dc"
    "gap" "\u2a86"
    "gbreve" "\u011f"
    "gcedil" "\u0123"
    "gcirc" "\u011d"
    "gcy" "\u0433"
    "gdot" "\u0121"
    "ge" "\u2265"
    "gel" "\u22db"
    "ges" "\u2265"
    "ggr" "\u03b3"
    "gimel" "\u2137"
    "gjcy" "\u0453"
    "gl" "\u2277"
    "gnE" "\u2269"
    "gnap" "\u2a8a"
    "gne" "\u2269"
    "gnsim" "\u22e7"
    "grave" "\u0060"
    "gsdot" "\u22d7"
    "gsim" "\u2273"
    "gt" "\u003e"
    "gvnE" "\u2269"
    "hArr" "\u21d4"
    "hairsp" "\u200a"
    "half" "\u00bd"
    "hamilt" "\u210b"
    "hardcy" "\u044a"
    "harr" "\u2194"
    "harrw" "\u21ad"
    "hcirc" "\u0125"
    "hearts" "\u2665"
    "hellip" "\u2026"
    "horbar" "\u2015"
    "hstrok" "\u0127"
    "hybull" "\u2043"
    "hyphen" "\u002d"
    "iacgr" "\u03af"
    "iacute" "\u00ed"
    "icirc" "\u00ee"
    "icy" "\u0438"
    "idiagr" "\u0390"
    "idigr" "\u03ca"
    "iecy" "\u0435"
    "iexcl" "\u00a1"
    "iff" "\u21d4"
    "igr" "\u03b9"
    "igrave" "\u00ec"
    "ijlig" "\u0133"
    "imacr" "\u012b"
    "image" "\u2111"
    "incare" "\u2105"
    "infin" "\u221e"
    "inodot" "\u0131"
    "int" "\u222b"
    "intcal" "\u22ba"
    "iocy" "\u0451"
    "iogon" "\u012f"
    "iota" "\u03b9"
    "iquest" "\u00bf"
    "isin" "\u2208"
    "itilde" "\u0129"
    "iukcy" "\u0456"
    "iuml" "\u00ef"
    "jcirc" "\u0135"
    "jcy" "\u0439"
    "jsercy" "\u0458"
    "jukcy" "\u0454"
    "kappa" "\u03ba"
    "kappav" "\u03f0"
    "kcedil" "\u0137"
    "kcy" "\u043a"
    "kgr" "\u03ba"
    "kgreen" "\u0138"
    "khcy" "\u0445"
    "khgr" "\u03c7"
    "kjcy" "\u045c"
    "lAarr" "\u21da"
    "lArr" "\u21d0"
    "lE" "\u2266"
    "lEg" "\u2a8b"
    "lacute" "\u013a"
    "lagran" "\u2112"
    "lambda" "\u03bb"
    "lang" "\u2329"
    "lap" "\u2a85"
    "laquo" "\u00ab"
    "larr" "\u2190"
    "larr2" "\u21c7"
    "larrhk" "\u21a9"
    "larrlp" "\u21ab"
    "larrtl" "\u21a2"
    "lcaron" "\u013e"
    "lcedil" "\u013c"
    "lceil" "\u2308"
    "lcub" "\u007b"
    "lcy" "\u043b"
    "ldot" "\u22d6"
    "ldquo" "\u201c"
    "ldquor" "\u201e"
    "le" "\u2264"
    "leg" "\u22da"
    "les" "\u2264"
    "lfloor" "\u230a"
    "lg" "\u2276"
    "lgr" "\u03bb"
    "lhard" "\u21bd"
    "lharu" "\u21bc"
    "lhblk" "\u2584"
    "ljcy" "\u0459"
    "lmidot" "\u0140"
    "lnE" "\u2268"
    "lnap" "\u2a89"
    "lne" "\u2268"
    "lnsim" "\u22e6"
    "lowast" "\u2217"
    "lowbar" "\u005f"
    "loz" "\u25ca"
    "lozf" "\u2726"
    "lpar" "\u0028"
    "lrarr2" "\u21c6"
    "lrhar2" "\u21cb"
    "lrm" "\u200e"
    "lsaquo" "\u2039"
    "lsh" "\u21b0"
    "lsim" "\u2272"
    "lsqb" "\u005b"
    "lsquo" "\u2018"
    "lsquor" "\u201a"
    "lstrok" "\u0142"
    "lt" "\u003c"
    "lthree" "\u22cb"
    "ltimes" "\u22c9"
    "ltri" "\u25c3"
    "ltrie" "\u22b4"
    "ltrif" "\u25c2"
    "lvnE" "\u2268"
    "macr" "\u00af"
    "male" "\u2642"
    "malt" "\u2720"
    "map" "\u21a6"
    "marker" "\u25ae"
    "mcy" "\u043c"
    "mdash" "\u2014"
    "mgr" "\u03bc"
    "micro" "\u00b5"
    "mid" "\u2223"
    "middot" "\u00b7"
    "minus" "\u2212"
    "minusb" "\u229f"
    "mldr" "\u2026"
    "mnplus" "\u2213"
    "models" "\u22a7"
    "mu" "\u03bc"
    "mumap" "\u22b8"
    "nVDash" "\u22af"
    "nVdash" "\u22ae"
    "nabla" "\u2207"
    "nacute" "\u0144"
    "nap" "\u2249"
    "napos" "\u0149"
    "natur" "\u266e"
    "nbsp" "\u00a0"
    "ncaron" "\u0148"
    "ncedil" "\u0146"
    "ncong" "\u2247"
    "ncy" "\u043d"
    "ndash" "\u2013"
    "ne" "\u2260"
    "nearr" "\u2197"
    "nequiv" "\u2262"
    "nexist" "\u2204"
    "nge" "\u2271"
    "nges" "\u2271"
    "ngr" "\u03bd"
    "ngt" "\u226f"
    "nhArr" "\u21ce"
    "nharr" "\u21ae"
    "ni" "\u220b"
    "njcy" "\u045a"
    "nlArr" "\u21cd"
    "nlarr" "\u219a"
    "nldr" "\u2025"
    "nle" "\u2270"
    "nles" "\u2270"
    "nlt" "\u226e"
    "nltri" "\u22ea"
    "nltrie" "\u22ec"
    "nmid" "\u2224"
    "not" "\u00ac"
    "notin" "\u2209"
    "npar" "\u2226"
    "npr" "\u2280"
    "npre" "\u22e0"
    "nrArr" "\u21cf"
    "nrarr" "\u219b"
    "nrtri" "\u22eb"
    "nrtrie" "\u22ed"
    "nsc" "\u2281"
    "nsce" "\u22e1"
    "nsim" "\u2241"
    "nsime" "\u2244"
    "nsmid" "\u2224"
    "nspar" "\u2226"
    "nsub" "\u2284"
    "nsubE" "\u2288"
    "nsube" "\u2288"
    "nsup" "\u2285"
    "nsupE" "\u2289"
    "nsupe" "\u2289"
    "ntilde" "\u00f1"
    "nu" "\u03bd"
    "num" "\u0023"
    "numero" "\u2116"
    "numsp" "\u2007"
    "nvDash" "\u22ad"
    "nvdash" "\u22ac"
    "oS" "\u24c8"
    "oacgr" "\u03cc"
    "oacute" "\u00f3"
    "oast" "\u229b"
    "ocir" "\u229a"
    "ocirc" "\u00f4"
    "ocy" "\u043e"
    "odash" "\u229d"
    "odblac" "\u0151"
    "odot" "\u2299"
    "oelig" "\u0153"
    "ogon" "\u02db"
    "ogr" "\u03bf"
    "ograve" "\u00f2"
    "ohacgr" "\u03ce"
    "ohgr" "\u03c9"
    "ohm" "\u2126"
    "olarr" "\u21ba"
    "oline" "\u203e"
    "omacr" "\u014d"
    "omega" "\u03c9"
    "omicron" "\u03bf"
    "ominus" "\u2296"
    "oplus" "\u2295"
    "or" "\u2228"
    "orarr" "\u21bb"
    "order" "\u2134"
    "ordf" "\u00aa"
    "ordm" "\u00ba"
    "oslash" "\u00f8"
    "osol" "\u2298"
    "otilde" "\u00f5"
    "otimes" "\u2297"
    "ouml" "\u00f6"
    "par" "\u2225"
    "para" "\u00b6"
    "part" "\u2202"
    "pcy" "\u043f"
    "percnt" "\u0025"
    "period" "\u002e"
    "permil" "\u2030"
    "perp" "\u22a5"
    "pgr" "\u03c0"
    "phgr" "\u03c6"
    "phi" "\u03c6"
    "phis" "\u03c6"
    "phiv" "\u03d5"
    "phmmat" "\u2133"
    "phone" "\u260e"
    "pi" "\u03c0"
    "piv" "\u03d6"
    "planck" "\u210f"
    "plus" "\u002b"
    "plusb" "\u229e"
    "plusdo" "\u2214"
    "plusmn" "\u00b1"
    "pound" "\u00a3"
    "pr" "\u227a"
    "prap" "\u2ab7"
    "pre" "\u227c"
    "prime" "\u2032"
    "prnE" "\u2ab5"
    "prnap" "\u2ab9"
    "prnsim" "\u22e8"
    "prod" "\u220f"
    "prop" "\u221d"
    "prsim" "\u227e"
    "psgr" "\u03c8"
    "psi" "\u03c8"
    "puncsp" "\u2008"
    "quest" "\u003f"
    "quot" "\u0022"
    "rAarr" "\u21db"
    "rArr" "\u21d2"
    "racute" "\u0155"
    "radic" "\u221a"
    "rang" "\u232a"
    "raquo" "\u00bb"
    "rarr" "\u2192"
    "rarr2" "\u21c9"
    "rarrhk" "\u21aa"
    "rarrlp" "\u21ac"
    "rarrtl" "\u21a3"
    "rarrw" "\u219d"
    "rcaron" "\u0159"
    "rcedil" "\u0157"
    "rceil" "\u2309"
    "rcub" "\u007d"
    "rcy" "\u0440"
    "rdquo" "\u201d"
    "rdquor" "\u201c"
    "real" "\u211c"
    "rect" "\u25ad"
    "reg" "\u00ae"
    "rfloor" "\u230b"
    "rgr" "\u03c1"
    "rhard" "\u21c1"
    "rharu" "\u21c0"
    "rho" "\u03c1"
    "rhov" "\u03f1"
    "ring" "\u02da"
    "rlarr2" "\u21c4"
    "rlhar2" "\u21cc"
    "rlm" "\u200f"
    "rpar" "\u0029"
    "rpargt" "\u2994"
    "rsaquo" "\u203a"
    "rsh" "\u21b1"
    "rsqb" "\u005d"
    "rsquo" "\u2019"
    "rsquor" "\u2018"
    "rthree" "\u22cc"
    "rtimes" "\u22ca"
    "rtri" "\u25b9"
    "rtrie" "\u22b5"
    "rtrif" "\u25b8"
    "rx" "\u211e"
    "sacute" "\u015b"
    "samalg" "\u2210"
    "sbquo" "\u201a"
    "sbsol" "\u005c"
    "sc" "\u227b"
    "scap" "\u2ab8"
    "scaron" "\u0161"
    "sccue" "\u227d"
    "sce" "\u227d"
    "scedil" "\u015f"
    "scirc" "\u015d"
    "scnE" "\u2ab6"
    "scnap" "\u2aba"
    "scnsim" "\u22e9"
    "scsim" "\u227f"
    "scy" "\u0441"
    "sdot" "\u22c5"
    "sdotb" "\u22a1"
    "sect" "\u00a7"
    "semi" "\u003b"
    "setmn" "\u2216"
    "sext" "\u2736"
    "sfgr" "\u03c2"
    "sfrown" "\u2322"
    "sgr" "\u03c3"
    "sharp" "\u266f"
    "shchcy" "\u0449"
    "shcy" "\u0448"
    "shy" "\u00ad"
    "sigma" "\u03c3"
    "sigmaf" "\u03c2"
    "sigmav" "\u03c2"
    "sim" "\u223c"
    "sime" "\u2243"
    "smid" "\u2223"
    "smile" "\u2323"
    "softcy" "\u044c"
    "sol" "\u002f"
    "spades" "\u2660"
    "spar" "\u2225"
    "sqcap" "\u2293"
    "sqcup" "\u2294"
    "sqsub" "\u228f"
    "sqsube" "\u2291"
    "sqsup" "\u2290"
    "sqsupe" "\u2292"
    "squ" "\u25a1"
    "square" "\u25a1"
    "squf" "\u25aa"
    "ssetmn" "\u2216"
    "ssmile" "\u2323"
    "sstarf" "\u22c6"
    "star" "\u2606"
    "starf" "\u2605"
    "sub" "\u2282"
    "subE" "\u2286"
    "sube" "\u2286"
    "subnE" "\u228a"
    "subne" "\u228a"
    "sum" "\u2211"
    "sung" "\u266a"
    "sup" "\u2283"
    "sup1" "\u00b9"
    "sup2" "\u00b2"
    "sup3" "\u00b3"
    "supE" "\u2287"
    "supe" "\u2287"
    "supnE" "\u228b"
    "supne" "\u228b"
    "szlig" "\u00df"
    "target" "\u2316"
    "tau" "\u03c4"
    "tcaron" "\u0165"
    "tcedil" "\u0163"
    "tcy" "\u0442"
    "tdot" "\u20db"
    "telrec" "\u2315"
    "tgr" "\u03c4"
    "there4" "\u2234"
    "theta" "\u03b8"
    "thetas" "\u03b8"
    "thetasym" "\u03d1"
    "thetav" "\u03d1"
    "thgr" "\u03b8"
    "thinsp" "\u2009"
    "thkap" "\u2248"
    "thksim" "\u223c"
    "thorn" "\u00fe"
    "tilde" "\u02dc"
    "times" "\u00d7"
    "timesb" "\u22a0"
    "top" "\u22a4"
    "tprime" "\u2034"
    "trade" "\u2122"
    "trie" "\u225c"
    "tscy" "\u0446"
    "tshcy" "\u045b"
    "tstrok" "\u0167"
    "twixt" "\u226c"
    "uArr" "\u21d1"
    "uacgr" "\u03cd"
    "uacute" "\u00fa"
    "uarr" "\u2191"
    "uarr2" "\u21c8"
    "ubrcy" "\u045e"
    "ubreve" "\u016d"
    "ucirc" "\u00fb"
    "ucy" "\u0443"
    "udblac" "\u0171"
    "udiagr" "\u03b0"
    "udigr" "\u03cb"
    "ugr" "\u03c5"
    "ugrave" "\u00f9"
    "uharl" "\u21bf"
    "uharr" "\u21be"
    "uhblk" "\u2580"
    "ulcorn" "\u231c"
    "ulcrop" "\u230f"
    "umacr" "\u016b"
    "uml" "\u00a8"
    "uogon" "\u0173"
    "uplus" "\u228e"
    "upsi" "\u03c5"
    "upsih" "\u03d2"
    "upsilon" "\u03c5"
    "urcorn" "\u231d"
    "urcrop" "\u230e"
    "uring" "\u016f"
    "utilde" "\u0169"
    "utri" "\u25b5"
    "utrif" "\u25b4"
    "uuml" "\u00fc"
    "vArr" "\u21d5"
    "vDash" "\u22a8"
    "varr" "\u2195"
    "vcy" "\u0432"
    "vdash" "\u22a2"
    "veebar" "\u22bb"
    "vellip" "\u22ee"
    "verbar" "\u007c"
    "vltri" "\u22b2"
    "vprime" "\u2032"
    "vprop" "\u221d"
    "vrtri" "\u22b3"
    "vsubnE" "\u228a"
    "vsubne" "\u228a"
    "vsupnE" "\u228b"
    "vsupne" "\u228b"
    "wcirc" "\u0175"
    "wedgeq" "\u2259"
    "weierp" "\u2118"
    "whitestar" "\u2727"
    "wreath" "\u2240"
    "xcirc" "\u25cb"
    "xdtri" "\u25bd"
    "xgr" "\u03be"
    "xhArr" "\u2194"
    "xharr" "\u2194"
    "xi" "\u03be"
    "xlArr" "\u21d0"
    "xrArr" "\u21d2"
    "xutri" "\u25b3"
    "yacute" "\u00fd"
    "yacy" "\u044f"
    "ycirc" "\u0177"
    "ycy" "\u044b"
    "yen" "\u00a5"
    "yicy" "\u0457"
    "yucy" "\u044e"
    "yuml" "\u00ff"
    "zacute" "\u017a"
    "zcaron" "\u017e"
    "zcy" "\u0437"
    "zdot" "\u017c"
    "zeta" "\u03b6"
    "zgr" "\u03b6"
    "zhcy" "\u0436"
    "zwj" "\u200d"
    "zwnj" "\u200c"])

