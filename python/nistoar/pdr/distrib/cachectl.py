"""
A client for the Distribution Service's Cache Management Control API.

The Distribution Service can support a cache for fast delivery of files (i.e. objects) from datasets.  
Such a cache is useful when the availability of storage from which to deliver objects is smaller than 
the total contents of the archive.  When a desired object is not in the cache, the object must be 
retrieved from long-term storage and unpacked from is Archive Information Package (AIP), which can be
slow and costly.  The Distribution Service provides an API for inspecting and managing the contents 
of the cache.  
"""
from typing import List, Mapping

class CacheCtlClient:
    """
    A client for the Distribution Service's Cache Management Control API.

    See the :py:mod:`module docmentation<nistoar.pdr.distrib.cachectl>` for more information on the 
    purpose of the data cache.  The cache stores data into set of storage volumes with limited 
    capacities.  Data objects from a dataset can be distributed across the different volumes according
    to an internal strategy.  A cache database keeps track of in which volumes objects can be found.  
    When a volume fills up, objects will be deleted up as needed; the database can remember information 
    about objects that no longer exist in the cache.  If an object currently exists in the cache, it is 
    referred to as "cached".  

    The cache manager includes a monitor process that periodically runs integrity checks on objects 
    in the cache to ensure that objects register to be cached are actually cached, and it recalculates 
    their checksums to make sure they still match their registered checksums.
    """

    def __init__(self, endpoint: str, authkey: str=None):
        """
        create the client

        :param str endpoint:  the service's endpoint URL; for the PDR, this URL ends with "/cache"
        :param str  authkey:  an authentication key; if not provided, it will be assumed that none
                              is needed.
        """
        self.ep = endpoint.rstrip('/')
        self.authhdr = { }
        if authkey:
            self.authhdr["Authorization"] = "Bearer " + authkey

        self._cli = RESTServiceClient(self.ep, self.authhdr)

    @classpath
    def from_config(cls, config: Mapping):
        try:
            return cls(config['service_endpoint'], config.get('auth_key'))
        except KeyError as ex:
            raise ConfigurationException("Missing required config parameter: "+str(ex)) from ex

    def is_up(self) -> bool:
        """
        return True if the cache manager is up and operating. If an error occurs either 
        during communication with the service or as a result of an error response, 
        False is returned.
        """
        return self._cli.is_available("/")

    def volumes(self) -> List[Mapping]:
        """
        return a list of descriptions of volumes currently in use.
        """
        return self._cli.get_json("/volumes/")

    def volume_names(self) -> List[str]:
        """
        return a list of the names of the volumes currently in use
        """
        return [d.get('name') for d in self.volumes()]

    def describe_volume(self, name) -> Mapping:
        """
        return a description of a volume with the given name
        """
        return self._cli.get_json("/volumes/"+name)

    def datasets(self) -> List[Mapping]:
        """
        return a list of descriptions of datasets that have objects known to the cache manager.  
        Some of the objects are currently in the cache, while others no longer are.  This list 
        may be quite large.
        """
        return self._cli.get_json("/objects/")

    def objects_for(self, aipid: str, select: str="files") -> List[Mapping]:
        """
        return a list of descriptions of known objects belonging to a dataset with the given identifier.

        :param str  aipid:  the identifier for the dataset that objects are requested for
        :param str select:  a label for specifying a subset of the objects to return.  ``files``
                            (default) will return all files known to the cache, which can 
                            include some that are no longer in the cache.  ``cached`` will return 
                            only those that currently exist in the cache.  ``checked`` will 
                            return only those that are subject to integrity checking, which excludes
                            files that cannot be removed from the cache.  See also 
                            :py:meth:`cached_objects_for` and :py:meth:`checked_objects_for`
        """
        if not select:
            select = "files"
        if select not in ["files", "cached", "checked"]:
            raise ValueError("objects_for(): invalid select value: "+select)
        return self._cli.get_json(f"/objects/{aipid}/:{select}")

    def cached_objects_for(self, aipid) -> List[Mapping]:
        """
        return a list of the file objects for a dataset that are currently cached.  

        This is equivalent to :py:meth:`objects_for(aipid, 'cached')<cached_objects>`.
        """
        return self.objects_for(aipid, 'cached')

    def checked_objects_for(self, aipid) -> List[Mapping]:
        """
        return a list of the file objects for a dataset that are currently cached.  

        This is equivalent to :py:meth:`objects_for(aipid, 'checked')<cached_objects>`.
        """
        return self.objects_for(aipid, 'checked')

    def describe_object(self, aipid: str, objname: str) -> List[Mapping]:
        """
        return object descriptions for a specified object from a dataset.  While multiple descriptions
        may be returned, representing its presence (past or current) in different volumes, normally only
        one of them will currently is cached (i.e. actually exists presently in its volume).  
        """
        return self._cli.get_json(f"/objects/{aipid}/{objname}")

    def ensure_cached(self, aipid: str, objname: str=None, version: str=None,
                      recache: bool=False) -> List[Mapping]:
        """
        ensure a given dataset, or object from a dataset, is cached.  In general, caching is 
        asynchronous; this request will place the dataset or specific object onto a caching 
        queue.  

        :param str    aipid:  the AIP ID for the dataset to be cached
        :param str  objname:  the name of an object from the dataset to be cached; if None, all 
                              objects from the dataset will be cached.  
        :param str  version:  the version of the data set to cache.  If not provided, only the 
                              latest version will be subject to caching.
        :param bool recache:  if True and an object currently exists in the cache, it will be deleted 
                              and reloaded from long-term storage.  This is desired, for example, if 
                              the dataset has been recently updated.  
        :return:  a list of descriptions similar to that returned by :py:meth:`objects_for` which will
                  indicate the current cached status of each of the files in the dataset.
                  :rtype: List[Mapping]
        """
        which = f"/objects/{aipid}/"
        if objname:
            which += f"{objname}/"

        params = []
        if version:
            param.append(f"version={version}")
        if recache:
            param.append("recache=1")
        if params:
            params = "?" + "&".join(params)
        
        return self._cli.get_json(f"{which}:cached{params}", "PUT")
        

    def uncache(self, aipid, objname=None):
        """
        remove all objects or a particular object from a specified dataset from the cache.

        :param str    aipid:  the AIP ID for the dataset to be uncached
        :param str  objname:  the name of an object from the dataset to be uncached; if None, all 
                              objects from the dataset will be uncached.  
        """
        which = f"/objects/{aipid}/"
        if objname:
            which += f"{objname}/"
        which += ":cached"

        status, reason = self._cli.get_status(which, 'DELETE')
        if status >= 500:
            raise DistribServerError(status, "Unexpected Server failure: "+reason)
        elif status < 200 or status >= 300:
            raise DistribServerError(status, f"Unexpected server response: {status} {reason}")

        return True

    def queue_status(self) -> Mapping:
        """
        return status information about the caching queue. 

        The caching queue is the queue of data items waiting to be cached.  The output dictionary 
        will contain the following properties:

        ``status``
             (str) either ``running`` or ``not running``
        ``current``
             (str) the dataset or object currently being cached
        ``waiting``
             (list) a list of the datasets or objects waiting to be cached
        """
        return self._cli.get_json("/queue/")

    def trigger_caching(self):
        """
        send a signal to start processing any caching requests currently in the queue.  Normally, 
        this happens automatically when something is added to the queue; however if the caching 
        thread dies for some reason, this method will restart procesing.  
        """
        relurl = "/queue/"
        message = self._cli.get_text(relurl, "PUT")

        return "already" not in message.lower()        

    def monitor_status(self):
        """
        return a description of the most recent cache monitor activity.
        """
        return self._cli.get_json("/monitor/")

    def monitor_is_running(self) -> bool:
        """
        return True if the monitor is currently running
        """
        answer = self._cli.get_text("/monitor/running")
        if isinstance(answer, str):
            return answer.lower() == "true"

        return False

    def ensure_monitoring(self, repeat=True) -> bool:
        """
        trigger the start of the monitor thread if it is not already running.

        :param bool repeat:  if True (default), have the monitor run on its normal periodic schedule.
                             If False, it will run once and exit.
        :return: False if the monitor was already running or True if it was started
                 :rtype: bool
        """
        relurl = "/monitor/running?repeat="+str(repeat)
        message = self._cli.get_text(relurl, "PUT")

        return "already" not in message.lower()

    def stop_monitoring(self):
        """
        stop the monitoring thread.
        """
        status, reason = get_status("/monitor/running", "DELETE")

        if status >= 500:
            raise DistribServerError(status, "Unexpected Server failure: "+reason)
        elif status < 200 or status >= 300:
            raise DistribServerError(status, f"Unexpected server response: {status} {reason}")

        return True

        
