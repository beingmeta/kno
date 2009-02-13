TARGET=$1
SOURCE=${2:-http://data.beingmeta.com.s3.amazonaws.com/brico}
cd $TARGET
wget $SOURCE/absfreq.table
wget $SOURCE/analytics.index
wget $SOURCE/brico.db
wget $SOURCE/brico.pool
wget $SOURCE/core.index
wget $SOURCE/dtermcache.index
wget $SOURCE/english.index
wget $SOURCE/enplus.index
wget $SOURCE/entails.index
wget $SOURCE/latlong.index
wget $SOURCE/lattice.index
wget $SOURCE/links.index
wget $SOURCE/misc.index
wget $SOURCE/names.pool
wget $SOURCE/parts.index
wget $SOURCE/places.pool
wget $SOURCE/refs.index
wget $SOURCE/slots.index
wget $SOURCE/wordforms.index
wget $SOURCE/xbrico.pool
wget $SOURCE/xplus.index
wget $SOURCE/xwords.index
