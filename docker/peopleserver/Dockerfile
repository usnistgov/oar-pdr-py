#########################################################################
#
# Programmatic Data Publishing (PDP) web service
#
# This container launches the PDP web service via scripts/pdp-uwsgi.py
#
#########################################################################
FROM oar-pdr-py/pyenv

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod a+rx /usr/local/bin/entrypoint.sh

VOLUME /dev/oar-pdr-py
VOLUME /app/dist

RUN mkdir -p /dev/oar-pdr-py /app /data/nsd && chmod a+rwx /app
WORKDIR /dev/oar-pdr-py

ENV PYTHONPATH /app/dist/pdr/lib/python

ARG devuser=developer
USER $devuser
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
