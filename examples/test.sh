#!/bin/bash

exit_status=0

NXF_CMD=${NXF_CMD:-nextflow}
NXF_FILES=${*:-'*.nf'}
BOOST_VER="0.4.0"

for pipeline in $NXF_FILES ; do

    config_file="$(basename $pipeline .nf).config"
    if [[ -f $config_file ]]; then
        echo "> Running test: $pipeline (with $config_file)"
        opts="-c $config_file"
    else
        echo "> Running test: $pipeline"
        opts=""
    fi

    $NXF_CMD -q run "$pipeline" -plugins "nf-boost@$BOOST_VER" $opts
    status=$? ; [ $status -eq 0 ] || exit_status=$status
done

rm -rf .nextflow* work

[ $exit_status -eq 0 ] || false
