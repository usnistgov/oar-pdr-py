# Staff Directory Service (for development mode)

The intention of this Docker container is a to run a fully functional Staff Directory service
suite for development purposes.  It supports two APIs to the directory data: one (`nsd1`)
that is meant to be partially compliant with the NIST Staff Directory service (NSD) and another
(`oar1`) that is optimized for use with the OAR/MIDAS application.  This can be launched via
the [`peopleserver` script](../scripts/peopleserver) in the {`scripts` directory](../scripts).

We note that the [`midasserver`](../midas-server) can now be configured to host the directory
APIs along with the DBIO ones.  This `peopleserver` represents a stand-alone configuration.

## Prerequisites for running the server

To run this server "out of the box" requires:

  * Python 3 (>= 3.10.X)
  * Docker Engine with command-line tools
    * if you want to use the option MongoDB backend, you will also need the [docker compose
      plugin](https://docs.docker.com/get-started/08_using_compose/).  This is included with
      Docker Desktop (typical for Mac installs); to check to see if you already have it, type,
      `docker compose version`.
  * The [oar-pdr-py repository](https://github.com/usnistgov/oar-pdr-py) cloned onto your machine.

## Starting and stopping the server

To run the server, you should open a terminal and change into you local clone of the `oar-pdr-py`
repository.  The server is launched using the `peopleserver` script located in the `scripts`
directory.  Note when you run the script for the first time, it will automatically build all of
the python code and docker images (producing a lot of output to the screen); these are not rebuilt
by default on subsequent executions.

To start the server, you provide the name of the directory where you want the backend data written.
For example, you can type:

```bash
scripts/peopleserver --bg start
```

The `--bg` option causes the server to run in the background; thus, you will get your terminal
prompt back (allowing you to issue a stop command later).

By default, the server will load its database with a handful of fake test data records--enough
to facilitate testing and development.  However, you can load alternative records (like those
retrieved from the NSD service at NIST) using the `-d` option which identifies a directory
where the data records will be found; for example:

```bash
scripts/peopleserver --bg -d docker/peopleserver/data start
```

The start-up script will look for two files in that directory, `people.json` and `orgs.json`
(by default, these can be changed via the configuration file,
`docker/peopleserver/people_conf.yml`).  These contain the person and organization records,
respectivel, formatted in JSON as an array of objects.

For more start-up options, see the help documentation by typing,

```bash
scripts/peopleserver -h
```

To stop the server, type:

```bash
scripts/peopleserver -bg stop
```

## Accessing the APIs

Once the server is started, the internal database is loaded with records and is ready to use.
The data can be accessed via either of its two APIs at `http://localhost:9092/nsd1` and
`http://localhost:9092/oar1`, respectively.  Accessing either of these URLs exactly returns a
status message indicating whether is it is running properly:

```bash
# curl http://localhost:9092/nsd1
{
  "status": "ready",
  "message": "Ready with 8 organizations and 4 people",
  "person_count": 4,
  "org_count": 8
}
```

### Overview of the `nsd1` Interface

This interface is a partial implementation of the NIST Directory Service API; in particular,
it features these endpoints (that should be prefixed with `http://localhost:9092/nsd1`:

  * `/People/` -- accepts POST request for searching for person records.
  * `/NISTOU`  -- returns all NIST OU records
  * `/NISTDivision`  -- returns all NIST division records
  * `/NISTGroup`  -- returns all NIST group records

For more details on how to query these endpoints, consult the NIST SD Swagger page.  

### Overview of the `oar` Interface

Access to the people and organization records is done via the following endpoints (which
should be prefixed with `http://localhost:9092/oar1`):

   * `/people` -- for accessing, searching, or indexing person records
      * `/people/`_{id}_ -- returns a single person record whose database identifier (a
                            number) matches _{id}_.
      * `/people/select` -- for selecting a subset of person record based on search query
                            parameters (see searching below)
      * `/people/index`  -- for returning an index of matching selected person records
                            based on search query parameters
   * `/orgs` -- for accessing, searching, or indexing organization records, regardless of type
      * `/orgs/`_{id}_ -- returns a single organization record whose database identifier (a
                            number) matches _{id}_.
      * `/orgs/select` -- for selecting a subset of organization records based on search query
                            parameters (see searching below)
      * `/orgs/index`  -- for returning an index of matching selected organization records
                            based on search query parameters (see indexing below)
      * `/orgs/OU`     -- for accessing specifically organization records that represent different
                            Organizational Units (OU)
      * `/orgs/Div`    -- for accessing specifically organization records that represent different
                            Divisions
      * `/orgs/Group`  -- for accessing specifically organization records that represent different
                            Groups
         * `/orgs/Group/`_{id}_ -- returns a single organization record whose database identifier (a
                                   number) matches _{id}_.  (This endpoint exists for OU and Div
                                   endpoints as well.)
         * `/orgs/Group/select` -- for selecting a subset of organization records based on search query
                                   parameters (see searching below; this endpoint exists for OU and Div
                                   endpoints as well.)
         * `/orgs/Group/index`  -- for returning an index of matching selected organization records
                                   based on search query parameters (see indexing below; this endpoint 
                                   exists for OU and Div endpoints as well.)

#### Selecting records based on search criteria

The primary way to search for selected records based on search query is to URL query
parameters in a GET request, either to the main record endpoint (`/oar1/people` or
`/oar1/orgs`) or to its `/select` sub resource.  A search constraint agains a particular field
in the record is done with a query parameter of the form, `with_`_{field}_`=`_{testvalue}_,
where _{field}_ is the name of the field in the record to constrain.  This constraint will
restrict the returned records to those whose _{field}_ property either matches _{value}_ (if
it is not a string-type field) or at least starts with the value as a substring.  If the same
"with" parameter is given multiple times the constraints will be logically OR-ed together.
Different "with" parameters (i.e. with different _{field}_ suffixes in the parameter name) are
logically AND-ed together.

In the example below, this query returns all people whose first name begins with "phil" (the
match is case-insensitive):

```bash
curl http://localhost:9092/oar1/people?with_firstName=phil
```

Searchable endpoints also support the `like` parameter, but this is mainly used for indexing
(see next section), so we explain it there.

#### Creating indexes based on search criteria

With an `index` endpoint, one can get special compact rendering of matching records that is
useful for feeding suggestions into front-end forms presented to the user within a web page.
Creating an index is must like selecting records as the same query parameters can be used.
The most useful one is the `like` parameter.

The value of a `like` parameter is text, typically just a few characters.  The returned index
will reference all record in which any one of a selected set of record fields start with that
string.  When creating an index of people records (i.e. sending a GET to
`/oar1/people/index`), the index will point to people records where either the `firstName` or
`lastName` begins with the `like` parameter value.  If creating an index of organizations, the
returned index refers to organizations where the organization name (`orG_Name`), its
abbreviation (`orG_ACRNM`), _or_ its number (`orG_CD`, like "641") starts with the `like`
value.  If a more than one `like` parameter is provided, the index will be a combination of
the records matching the either of the values.

The returned index is a JSON object with the following structure:

  * each key in the object is value of one of the selected fields that was matched.
  * each value is an object containing references to records containing that value in one of
    the selecect fields.  In that inner object:
     * the key is the database identifier (a number) for the matched record.  This key can be
       used to look up the full record using the appropriate _{id}_ endpoint noted above.  
     * the value is a string representat of the record.  If it is a person record, it will be
       person's name in the form _lastName_, _firstName_.  This string is intended for display
       to the user as a suggestion for a matching record.

A front-end application can use this an index to quickly offer suggestions as the user begins
typing a name into an input form.  The app will usually collect the first two or three
characters typed by the user before requesting an index by passing those characters into the
`like` parameter.  The app loads the returned index into memory.  Then as the user types more,
the app will select out all entries where the (outer) keys start with the characters typed so
far.  All of the records those keys reference represent suggestions for records that match
what was typed.  The app displays the inner repreesntative values as suggestions.  When the
user selects one of the suggestions, the app uses the corresponding identifier to look up the
first record.





