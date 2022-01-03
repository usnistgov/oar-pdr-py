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

FROM oar-metadata/ejsonschema

RUN apt-get update && apt-get install -y python-yaml curl wget less sudo zip \
                                         p7zip-full ca-certificates git
RUN pip install --upgrade pip setuptools
RUN pip install funcsigs 'bagit>=1.6.3,<2.0' 'fs>=2.0.21' mako

# install multibag from source
RUN multibag_ver=0.3 && \
    curl -L -o multibag-py.zip \
    https://github.com/usnistgov/multibag-py/archive/$multibag_ver.zip && \
    unzip -oq multibag-py.zip && \
    cd multibag-py-$multibag_ver && \
    echo __version__ = $multibag_ver >> multibag/__init__.py && \
    python setup.py install --install-purelib=/usr/local/lib/python3.8/dist-packages

# Create the user that build/test operations should run as.  Normally,
# this is set to match identity information of the host user that is
# launching the container.
#
RUN sed --in-place -e '/CREATE_MAIL_SPOOL/ s/=yes/=no/' /etc/default/useradd
ARG devuser=developer
ARG devuid=1000
RUN grep -qs :${devuid}: /etc/group || \
    groupadd --gid $devuid $devuser
RUN grep -Pqs ":${devuid}:\d+:" /etc/passwd || \
    useradd -m --comment "OAR Developer" --shell /bin/bash \
            --gid $devuid --uid $devuid $devuser
RUN echo $devuser "ALL=(ALL)" NOPASSWD:ALL > /etc/sudoers.d/90-developers

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod a+rx /usr/local/bin/entrypoint.sh

VOLUME /dev/oar-pdr-py
VOLUME /app/dist

RUN mkdir -p /dev/oar-pdr-py /app && chmod a+rwx /app
WORKDIR /dev/oar-pdr-py

ENV PYTHONPATH /dev/oar-pdr-py/python/build/lib

USER $devuser
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
