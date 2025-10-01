#!/bin/bash

VALKEY_VERSION=8.1.1

YD_AGENT_HOME="/opt/yellowdog/agent"
YD_AGENT_USER="yd-agent"

echo "Installing Valkey version $VALKEY_VERSION"
CPU=$(arch | sed s/aarch64/arm64/)
curl -O  https://download.valkey.io/releases/valkey-$VALKEY_VERSION-jammy-$CPU.tar.gz
tar -xf valkey-$VALKEY_VERSION-jammy-$CPU.tar.gz  -C $YD_AGENT_HOME
chown -R $YD_AGENT_USER:$YD_AGENT_USER $YD_AGENT_HOME/valkey*
