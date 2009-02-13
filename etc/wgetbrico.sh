TARGET=$1
SOURCE=${2:-http://data.beingmeta.com.s3.amazonaws.com/brico}
cd $TARGET
wget -N  $SOURCE/absfreq.table
wget -N  $SOURCE/analytics.index
wget -N  $SOURCE/brico.db
wget -N  $SOURCE/brico.pool
wget -N  $SOURCE/core.index
wget -N  $SOURCE/dtermcache.index
wget -N  $SOURCE/english.index
wget -N  $SOURCE/enplus.index
wget -N  $SOURCE/entails.index
wget -N  $SOURCE/latlong.index
wget -N  $SOURCE/lattice.index
wget -N  $SOURCE/links.index
wget -N  $SOURCE/misc.index
wget -N  $SOURCE/names.pool
wget -N  $SOURCE/parts.index
wget -N  $SOURCE/places.pool
wget -N  $SOURCE/refs.index
wget -N  $SOURCE/slots.index
wget -N  $SOURCE/wordforms.index
wget -N  $SOURCE/xbrico.pool
wget -N  $SOURCE/xplus.index
wget -N  $SOURCE/xwords.index
