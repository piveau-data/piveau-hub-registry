#!/bin/bash

username=
password=
catalogue=
filename=
keycloak_server=https://www.ppe-aws.europeandataportal.eu
keycloak_token_path=/auth/realms/edp/protocol/openid-connect/token
keycloak_resource_path=/auth/realms/edp/authz/protection/resource_set
TOKEN=
RTP=
KEYCLOAK_TOKEN=
RESOURCE_ID=
client_secret=96603d23-2219-4780-b72a-f1ec66a5a6e1
client_id=piveau-hub
use=$1 shift
while [ "$1" != "" ]; do
        case $1 in
                -c | --catalogue ) shift
                        catalogue=$1
                        ;;
                -u | --username ) shift
                        username=$1
                        ;;
                -p | --password ) shift
                        password=$1
                        ;;
                *) usage
                exit 1
        esac
        shift
done

if [ "$use" = "create" ]
then
    KEYCLOAK_TOKEN=$(curl --data "grant_type=client_credentials&client_id=${client_id}&client_secret=${client_secret}" ${keycloak_server}${keycloak_token_path}  | sed 's/.*access_token":"//g' | sed 's/".*//g')
    echo "\n"

    curl -X POST ${keycloak_server}${keycloak_resource_path} -H 'Authorization: Bearer '$KEYCLOAK_TOKEN -H 'Content-Type: application/json' -d "{\"name\":\"https://europeandataportal.eu/id/catalogue/${catalogue}\",\"type\":\"urn:piveau-h
ub:resources:catalog\",\"owner\":\"${username}\",\"ownerManagedAccess\":true,\"displayName\":\"https://europeandataportal.eu/id/catalogue/${catalogue}\",\"attributes\":{},\"uris\":[\"https://europeandataportal.eu/id/catalogue/${catalgoue
}\"],\"resource_scopes\":[{\"name\":\"update\"},{\"name\":\"delete\"}],\"scopes\":[{\"name\":\"update\"},{\"name\":\"delete\"}]}"
fi

if [ "$use" = "delete" ]
then

    KEYCLOAK_TOKEN=$(curl --data "grant_type=client_credentials&client_id=${client_id}&client_secret=${client_secret}" ${keycloak_server}${keycloak_token_path} | sed 's/.*access_token":"//g' | sed 's/".*//g')
    echo "\n"

    RESOURCE_ID=$(curl -X GET ${keycloak_server}${keycloak_resource_path}?name=${catalogue} -H "Authorization: Bearer $KEYCLOAK_TOKEN" | sed 's/\["//g' | sed 's/"\]//g' | sed 's/","/ /g')

    echo "\n"
    #echo $RESOURCE_ID

    curl -X DELETE ${keycloak_server}${keycloak_resource_path}/$RESOURCE_ID -H "Authorization: Bearer $KEYCLOAK_TOKEN"
    echo "\n"
fi

if [ "$use" = "RTP" ]
then
    TOKEN=$(curl --data "grant_type=password&client_id=edp-ui&username=${username}&password=${password}" ${keycloak_server}${keycloak_token_path} | sed 's/.*access_token":"//g' | sed 's/".*//g')
    
     RTP=$(curl -X POST ${keycloak_server}${keycloak_token_path} -H "Authorization: Bearer $TOKEN" --data "grant_type=urn:ietf:params:oauth:grant-type:uma-ticket" --data "audience=piveau-hub" | sed 's/.*access_token":"//g' | sed 's/".*//g
')
    echo $RTP
fi