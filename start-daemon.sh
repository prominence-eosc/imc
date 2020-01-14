#!/bin/sh

echo "starting daemon with: $IMC_DAEMON $IMC_DAEMON_ARGS"
echo ""

eval "/usr/bin/imc-$IMC_DAEMON $IMC_DAEMON_ARGS"
