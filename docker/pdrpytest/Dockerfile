#########################################################################
#
# Build/test container for the OAR PDR python library
#
# The purpose of this Docker container is two-fold:
#  * produce a distribution bundle that can be added into a deployment
#    container (e.g. part of oar-docker).  Normally this is saved as
#    dist/pdrlib.zip.
#  * Execute all unit and integration tests.
# 
# This Dockerfile creates a Linux container with all the necessary
# dependencies sufficient to do these two things.  
#
#########################################################################
FROM oar-pdr-py/pyenv

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod a+rx /usr/local/bin/entrypoint.sh

VOLUME /dev/oar-pdr-py
VOLUME /app/dist

RUN mkdir -p /dev/oar-pdr-py /app && chmod a+rwx /app
WORKDIR /dev/oar-pdr-py

ENV PYTHONPATH /dev/oar-pdr-py/python/build/lib

ARG devuser=developer
USER $devuser
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
