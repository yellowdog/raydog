#!/bin/sh

## WTB - put the IP address in applcaiotn.yaml
MYIP=`ip -br a | awk '$2 ~ /^UP/ { print $3 }' | cut -d/ -f 1`
sed -i "/  publicIpAddress:/c\  publicIpAddress: \"$MYIP\""  "/opt/yellowdog/agent/application.yaml"
##

PATH="/opt/yellowdog/agent/bin:$PATH"
export PATH
"/opt/yellowdog/agent/jre/bin/java" -jar "/opt/yellowdog/agent/agent.jar"
