#########################################################################
#
# PDR Python base environment
# 
# This container provides the common python environment required by
# other containers needed in the docker directory
# 
# This Dockerfile creates a Linux container with Python 3 installed 
# along with all required python libraries.
#
#########################################################################
FROM oar-metadata/mdenv

RUN apt-get update && apt-get install -y python3-yaml curl wget less sudo zip \
                                         p7zip-full ca-certificates git
# RUN pip install --upgrade pip setuptools
RUN pip install funcsigs 'bagit>=1.6.3,<2.0' 'fs>=2.0.21' jsonpatch mako pyjwt \
                jsonpath_ng lxml webdavclient3 websockets

# install multibag from source
RUN multibag_ver=0.5 && \
    curl -L -o multibag-py.zip \
    https://github.com/usnistgov/multibag-py/archive/$multibag_ver.zip && \
    unzip -oq multibag-py.zip && \
    cd multibag-py-$multibag_ver && \
    echo __version__ = $multibag_ver >> multibag/__init__.py && \
    python setup.py install --install-purelib=/usr/local/lib/python3.10/dist-packages

# Create the user that container operations should run as.  Normally,
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

CMD ["bash"]

