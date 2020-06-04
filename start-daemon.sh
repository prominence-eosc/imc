#!/bin/sh

echo "starting daemon with: $PROMINENCE_IMC_DAEMON $PROMINENCE_IMC_DAEMON_ARGS"
echo ""

exec /usr/local/bin/imc-$PROMINENCE_IMC_DAEMON $PROMINENCE_IMC_DAEMON_ARGS
