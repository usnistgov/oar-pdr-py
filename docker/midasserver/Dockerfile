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
WORKDIR /dev/oar-pdr-py

ENV PYTHONPATH /dev/oar-pdr-py/python/dist/pdr/lib/python:/app/dist/pdr/lib/python

ARG devuser=developer
USER $devuser
EXPOSE 9091
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
