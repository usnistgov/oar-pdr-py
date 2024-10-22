# MIDAS Web Services (for development mode)

The intention of this Docker container is to run a fully functional MIDAS web service 
suite primarily for development purposes.  It can be launched via the
[`midasserver` script](../scripts/midasserver) in the [`scripts` directory](../scripts).

By default, the server operates with a storage backend in which records are stored in JSON
files beneath a specified data directory.  This makes it easy to inspect the current contents
of the stored records during development.  However, the server can optionally be run using a
MongoDB backend.

## Prerequisites for running the server

To run this server "out of the box" requires:

  * Python 3 (>= 3.10.X)
  * Docker Engine with command-line tools
    * if you want to use the option MongoDB backend, you will also need the [docker compose
      plugin](https://docs.docker.com/get-started/08_using_compose/).  This is included with
      Docker Desktop (typical for Mac installs); to check to see if you already have it, type,
      `docker compose version`.
  * The [oar-pdr-py repository](https://github.com/usnistgov/oar-pdr-py) cloned onto your machine.

It is not necessary to build the python software separately; this can be done on-the-fly the first
time your script run the server script.

## Starting and stopping the server

To run the server, you should open a terminal and change into your local clone of the `oar-pdr-py`
repository.  The server is launched using the `midasserver` script located in the `scripts`
directory.  Note when you run the script for the first time, it will automatically build all of
the python code and docker images (producing a lot of output to the screen); these are not rebuilt
by default on subsequent executions.

To start the server, you provide the name of the directory where you want the backend data written.
For example, you can type:

```bash
scripts/midasserver midasdata --bg
```

This will create a `midasdata` subdirectory in the current directory.  The actual record data will
be stored under `./midasdata/dbfiles`.  

The server runs in a docker container in the shell's forground; that is, you will not get your shell
prompt back.  This allows you to see messages logging each call made to the service.  To stop the
server, you need to open another terminal, change into the `oar-pdr-py` repository directory, and
type:

```bash
scripts/midasserver stop
```

More information about `midasserver` command-line options can be viewed via the `-h` option:

```bash
scripts/midasserver -h
```

### Launching with a MongoDB backend

The server can be optionally switched to store its records in a MongoDB database backend with the
`-M` command-line option:

```bash
scripts/midasserver -M midasdata
```

In addition to starting a MongoDB database server in a Docker container, it also launches a server
frontend (called mongo-express) that allows you to explore the database contents via a web browser.  
To view, open the URL, `http://localhost:8081/`.

To stop the server, be sure to also provide the `-M` option to ensure that the Mongo database gets
shutdown as well:

```bash
scripts/midasserver -M stop
```

### Launching with staff directory APIs included

If the server is started with the `-P` (or `--add-people-service`) option, the staff directory
service APIs will be included among the MIDAS APIs.

```bash
scripts/midasserver -M -P midasdata
```

These will be available under the `http://localhost:9091/nsd` endpoint.  See the
[peopleservice README documentation](../peopleserver/README.md) for details about using these
APIs. 

## Using the service

The base URLs for the MIDAS services are as follows:

  - [`http://localhost:9091/midas/dmp/mdm1`](http://localhost:9091/midas/dmp/mdm1) -- the Data
    Management Plan (DMP) project service
  - `http://localhost:9091/midas/dap/mds3` -- the Digital Asset Publication (DAP) project service
  - `http://localhost:9091/midas/groups`   -- the MIDAS user group service

Note that when you start the service, you also have access to online API documentation.  To view,
open the URL,
[`http://localhost:9091/docs/dmpsvc-elements.html`](http://localhost:9091/docs/dmpsvc-elements.html).

At this time, all request and response messages are JSON documents with a content type of
"application/json".  This content type is assumed by default, so "Accept" and "Content-Header"
HTTP headers are not required. 

### Creating a new DMP project

Creating a new DMP project record is done by POSTing to the service's base URL.  The request
body must be a JSON object which can contain the following properties:

  - `name` -- (Required)  The user-supplied mnemonic name to assign to the new record.  This name
    is intended only for display purposes; it is not part of the DMP data content.
  - `data` -- (Optional)  A JSON object containing the data content.  Each project type will
    enforce its own schema for this object.  The data included here is not expected to be complete.
  - `meta` -- (Optional)  A JSON object containing metadata hints that help the server manage
    the record.  This data will not be part of the public DMP data content.  Creation is the only
    time the client can directly add information to this object; although the server may update
    the information triggered by other user requests.  Unrecognized data in this object may be
    ignored.  

For example, an initial record might be created with:

```bash
curl -X POST --data '{"name": "CoTEM", "data": {"title": "Microscopy of Cobalt Samples"}}' \
     http://localhost:9091/midas/dmp/mdm1
```

If the creation request is successful, the request will return a 201 status and a JSON document
containing the full, newly created record:

```json
{
  "id": "mdm1:0003",
  "name": "CoTEM",
  "acls": {
    "read": [
      "anonymous"
    ],
    "write": [
      "anonymous"
    ],
    "admin": [
      "anonymous"
    ],
    "delete": [
      "anonymous"
    ]
  },
  "owner": "anonymous",
  "data": {
    "title": "Microscopy of Cobalt Samples"
  },
  "meta": {},
  "curators": [],
  "created": 1669560885.988901,
  "createdDate": "2022-11-27T09:54:45",
  "lastModified": 1669560885.988901,
  "lastModifiedDate": "2022-11-27T09:54:45",
  "deactivated": null,
  "type": "dmp"
}
```

Clients should note the value of the `id` property in order to make further updates.

### Updating a record's data content

Typically in the life of project record, after the client creates a new record, it will incrementally
update its data content as the user manipulates the client interface.  Updates can be made using either
PUT or PATCH requests.  PATCH is perhaps more typical: the input can contain partial data that will be
merged with the data that is already saved on the server.  With PUT, the input can also contain partial
data; however, it will completely replace the data that was already saved on the server, deleting all
previous data properties regardless of whether they are included in the input.

In this example, we use PATCH to add more data to the record.  Note that the URL includes the record's
`id` value, followed by `/data`.  

```bash
curl -X PATCH --data '{"expectedDataSize": "2 TB"}' http://localhost:9091/midas/dmp/mdm1/mdm1:0003/data
```

Because the URL used above specifically accesses the "data" part of the record, only the updated data
object is returned:

```
{
  "title": "Microscopy of Cobalt Samples",
  "expectedDataSize": "2 TB"
}
```

### Getting record contents (without updating)

GET requests can be made to against different resource URLs to get full records or portions of a record.
In particular:

  - `http://localhost:9091/midas/dmp/mdm1` -- returns a list of records that the requesting user is
    allowed to read
  - `http://localhost:9091/midas/dmp/mdm1/`_id_ -- returns the full record that has an identifier given
    by _id_.  The format is the same as that returned by the create request illustrated above.
  - `http://localhost:9091/midas/dmp/mdm1/`_id_`/data` -- returns just the data contents for the record
    with the identifier given by _id_.
  - `http://localhost:9091/midas/dmp/mdm1/`_id_`/name` -- returns just the mnemonic name assigned to the
    record by the user
  - `http://localhost:9091/midas/dmp/mdm1/`_id_`/owner` -- returns just the identifier of the user that 
    owns (and usually created) the record
  - `http://localhost:9091/midas/dmp/mdm1/`_id_`/acls` -- returns just access control lists attached to 
    the record
  - `http://localhost:9091/midas/dmp/mdm1/`_id_`/meta` -- returns just the custom metadata attached to record

### Other operations of note

The service provides other operations that a client can provide support for:

  - **Changing the mnemonic name** - a new name can be assigned to the record via a PUT request on the
    `/name` sub-resource of a record.
  - **Add/Remove permissions for other users** - the `/acls` sub-resource endpoints allow one to manipulate
    permissions given to other users.
  - **Create user groups**<sup>*</sup> -- the `/midas/groups` endpoint alls a user to create and manage 
    their own user groups that can be applied to a records ACLs.

For more information, consult the [API documentation](https://localhost:9091/docs/dmpsvc-elements.html).  

<sup>*</sup>_Not implemented yet._


