#!/bin/bash
set -e

echo "Avvio iniezione automatica della War Room Dashboard..."

# Il comando 'influx apply' inietta il template nel database
# '--force yes' serve a scavalcare le conferme manuali (automatizzazione totale)
influx apply \
  --force yes \
  --org srs_org \
  --file /docker-entrypoint-initdb.d/war_room_templete.json

echo "Dashboard installata con successo!"