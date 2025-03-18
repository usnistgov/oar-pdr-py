#########################################################################
#
# MIDAS Authoring web service suite
#
# This container launches the MIDAS web services via scripts/midas-uwsgi.py
#
#########################################################################
FROM oar-pdr-py/pyenv

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod a+rx /usr/local/bin/entrypoint.sh

VOLUME /dev/oar-pdr-py
VOLUME /app/dist

RUN mkdir -p /dev/oar-pdr-py /app && chmod a+rwx /app
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
RUN apt-get update
RUN apt-get install -y nodejs
RUN apt-get install -y lsof
RUN apt-get install -y vim
RUN npm install -g wscat
WORKDIR /dev/oar-pdr-py

ENV PYTHONPATH /dev/oar-pdr-py/python/dist/pdr/lib/python:/app/dist/pdr/lib/python

ARG devuser=developer
USER $devuser
EXPOSE 9091
EXPOSE 8765
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
