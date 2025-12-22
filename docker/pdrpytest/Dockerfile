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
COPY runtests.sh /usr/local/bin/runtests.sh
RUN chmod a+rx /usr/local/bin/runtests.sh

VOLUME /dev/oar-pdr-py
VOLUME /app/dist

RUN mkdir -p /dev/oar-pdr-py /app && chmod a+rwx /app
WORKDIR /dev/oar-pdr-py

RUN pip install preppy==5.0.1 trml2pdf==0.6
RUN pip install websockets==10.4 json-spec

ENV PYTHONPATH /dev/oar-pdr-py/python/build/lib
ENV MONGO_TESTDB_URL mongodb://localhost/testdb

ARG devuser=developer
ENV DEV_USER $devuser
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
