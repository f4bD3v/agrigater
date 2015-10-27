#!/bin/bash

function callscript {
    python get_agmarknet.py -r $START "$YEST" -f "\"$1\"" -c "\"$2\""
}

function processtype {
    echo "${1:1}"
}

START=`date -d "01/01/2002" +"%d/%m/%Y"`
# +%s
echo $START
YEST=`date +%d/%m/%Y -d "yesterday"`
TYPE=""
echo $YEST

while read line; do
    if [[ -z $line ]]; then continue;
    else
        if [[ $line =~ ^\# ]]; then
            TYPE=`processtype "$line"`
            echo $TYPE
	else callscript "$TYPE" "$line"; fi
        #else ; fi
    fi
done < "$1"

#linux: `date -d "$START 1 days +"%d/%m/%Y"`
#CURR=`date -j -v+1d -f "%d/%m/%Y" "$CURR" +"%d/%m/%Y"`

