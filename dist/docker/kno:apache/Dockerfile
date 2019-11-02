FROM kno:latest
MAINTAINER repoman@beingmeta.com
RUN apt-get -y update
RUN apt-get -y dist-upgrade
RUN apt-get -y install apache2 libapache2-mod-knocgi
RUN a2enmod ssl rewrite status proxy proxy_http info headers knocgi alias auth_basic auth_digest mime
