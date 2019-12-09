#!/bin/bash

username=
password=
catalogue=
dataset=
filename=
keycloak_server=https://www.ppe-aws.europeandataportal.eu
keycloak_token_path=/auth/realms/edp/protocol/openid-connect/token
keycloak_resource_path=/auth/realms/edp/authz/protection/resource_set
server=https://piveau-hub-edp2.okd.fokus.fraunhofer.de
TOKEN=
RTP=
KEYCLOAK_TOKEN=
RESOURCE_ID=
use=$1 shift
while [ "$1" != "" ]; do
        case $1 in
                -d | --dataset ) shift
                        dataset=$1
                        ;;
                -c | --catalogue ) shift
                        catalogue=$1
                        ;;
                -u | --username ) shift
                        username=$1
                        ;;
                -p | --password ) shift
                        password=$1
                        ;;
                -f | --filename ) shift
                        filename=$1
                        ;;
                -s | --server ) shift
                        server=$1
                        ;;
                *) usage
                exit 1
        esac
        shift
done

if [ "$use" = "create" ]
then
    TOKEN=$(curl --data "grant_type=password&client_id=edp-ui&username=${username}&password=${password}" ${keycloak_server}${keycloak_token_path} | sed 's/.*access_token":"//g' | sed 's/".*//g')
    echo "\n"

    RTP=$(curl -X POST ${keycloak_server}${keycloak_token_path} -H "Authorization: Bearer $TOKEN" --data "grant_type=urn:ietf:params:oauth:grant-type:uma-ticket" --data "audience=piveau-hub" | sed 's/.*access_token":"//g' | sed 's/".*//g
')
    echo "\n"

    curl -X PUT ${server}/datasets/${dataset}?catalogue=${catalogue} -H 'Authorization: Bearer '$RTP -H 'Content-Type: text/turtle' --data-binary '@./'${filename}
    echo "\n"
fi

if [ "$use" = "delete" ]
then

    TOKEN=$(curl --data "grant_type=password&client_id=edp-ui&username=${username}&password=${password}" ${keycloak_server}${keycloak_token_path} | sed 's/.*access_token":"//g' | sed 's/".*//g')
    echo "\n"

    RTP=$(curl -X POST ${keycloak_server}${keycloak_token_path} -H "Authorization: Bearer $TOKEN" --data "grant_type=urn:ietf:params:oauth:grant-type:uma-ticket" --data "audience=piveau-hub" | sed 's/.*access_token":"//g' | sed 's/".*//g
')
    echo "\n"

    curl -X DELETE ${server}/datasets/${dataset}?catalogue=${catalogue} -H 'Authorization: Bearer '$RTP
    echo "\n"
fi