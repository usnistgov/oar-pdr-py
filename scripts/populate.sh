#! /bin/bash
#
# This script installs allow to create or clear daps and dps
# -m describe the model you want to touch (dap or dmp)
# -a describe the action you want to do (create or clear)
# -n is the number of entry you want to create
# ./populate.sh -m dap -a create -n 10
# ./populate.sh -m dmp -a create -n 7
# ./populate.sh -m dap -a clear
# ./populate.sh -m dmp -a clear
#

Help()
{

    echo " This script installs allow to create or clear daps and dps"
    echo
    echo " -m describe the model you want to touch (dap or dmp)"
    echo " -a describe the action you want to do (create or clear)"
    echo " -n is the number of entry you want to create"
    echo " example : ./populate.sh -m dmp -a create -n 7 "
    echo " would create 7 dmps "
    echo " example : ./populate.sh -m dap -a clear"
    echo " would clear all daps "
}


create_dmp(){
    mkdir ../midasdata/dbio/dbfiles/dmp/
    for((i=1;i<=$1;i++))
do
data='{ "id": "mdm1:0'$i'",
    "name": "Test '$i'",
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
    "deactivated": null,
    "status": {
        "created": 1689021185.5037804,
        "state": "edit",
        "action": "create",
        "since": 1689021185.5038593,
        "modified": 1689021185.5050585,
        "message": "draft created"
    },
    "data": {
        "title": "Superconductor Metrology",
        "startDate": "2021-07-08 19:03:27",
        "endDate": "",
        "dmpSearchable": "Y",
        "funding": {
            "grant_source": "Grant Number",
            "grant_id": ""
        },
        "projectDescription": "Investigate various aspects of metrology for superconductors including: the comparison of unified scaling models, the computation of a samples irreversible strain limit, and investigate the correlation among measurements at various temperatures and fields. The volume of data anticipated for this project is in the range of 25 - 50 MB. Collaborators include J. Ekin (687 guest researcher), L. Goodrich (CU), and N. Cheggour (CU).",
        "organizations": [
            {
                "ORG_ID": 776,
                "name": "Statistical Engineering Division"
            }
        ],
        "primary_NIST_contact": {
            "firstName": "Jolene D.",
            "lastName": "Splett"
        },
        "contributors": [
            {
                "contributor": {
                    "firstName": "Jolene D.",
                    "lastName": "Splett"
                },
                "e_mail": "jolene.splett@nist.gov",
                "instituion": "NIST",
                "role": "Principal Investigator"
            }
        ],
        "keyWords": [
            "superconductor metrology"
        ],
        "dataStorage": [],
        "dataSize": null,
        "sizeUnit": "GB",
        "softwareDevelopment": {
            "development": "no",
            "softwareUse": "",
            "softwareDatabase": "",
            "softwareWebsite": ""
        },
        "technicalResources": [],
        "ethical_issues": {
            "ethical_issues_exist": "no",
            "ethical_issues_description": "",
            "ethical_issues_report": "",
            "dmp_PII": "no"
        },
        "dataDescription": "No additional requirements Not available to the public",
        "dataCategories": [
            "Derived Data",
            "Working Data"
        ],
        "preservationDescription": "Working Data; Derived Data; : Files types for this project include: text, Excel, Word, and PDF.",
        "pathsURLs": []
    },
    "meta": {},
    "curators": []
}
'
touch ../midasdata/dbio/dbfiles/dmp/mdm1:00"$i".json
echo $data > ../midasdata/dbio/dbfiles/dmp/mdm1:00"$i".json
done
}

create_dap(){
    mkdir ../midas/data/midas/dbio/dbfiles/dap/
    for((i=1;i<=$1;i++))
do
data='{
    "id": "mds3:0'$i'",
    "name": "test'$i'",
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
    "deactivated": null,
    "status": {
      "created": 1679938411.5561934,
      "state": "edit",
      "action": "create",
      "since": 1679938411.5563061,
      "modified": 1679938411.5659344,
      "message": "",
      "createdDate": "2023-03-27T17:33:31",
      "modifiedDate": "2023-03-27T17:33:31",
      "sinceDate": "2023-03-27T17:33:31"
    },
    "data": {
      "@id": "ark:/88434/mds3-0005",
      "title": "",
      "_schema": "https://data.nist.gov/od/dm/nerdm-schema/v0.7#",
      "@type": [
        "nrdp:PublicDataResource",
        "dcat:Resource"
      ],
      "doi": "doi:10.18434/mds3-0005",
      "author_count": 1,
      "file_count": 0,
      "nonfile_count": 1,
      "reference_count": 0
    },
    "meta": {
      "resourceType": "website",
      "creatorisContact": true,
      "willUpload": false,
      "assocPageType": "supplement"
    },
    "curators": [],
    "type": "dap"
  }'

touch ../midasdata/dbio/dbfiles/dap/mds3:00"$i".json
echo $data > ../midasdata/dbio/dbfiles/dap/"$i".json
done
}

while getopts m:a:n:h: flag 
do
    case "${flag}" in 
        m) model=${OPTARG};;
        a) action=${OPTARG};;
        n) number=${OPTARG};;
    esac
done
case $model in
    dmp)    
        case $action in
        # clear all existing dmp
            clear) `rm -rf ../midasdata/dbio/dbfiles/dmp/*.json`;;
        # create n dmps
            create) `create_dmp $number`;;

        esac;;
    dap)
        case $action in 
        # clear all existing dap
            clear) `rm -rf ../midasdata/dbio/dbfiles/dap/*.json`;;
        # create n daps 
            create) `create_dap $number`;;
        esac;;
esac