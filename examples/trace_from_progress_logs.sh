#!/usr/bin/env bash

INPUT_FILE=$1

function create_plot_file {
cat > /tmp/trace_from_solr2es_logs.plot << EOF
set xdata time
set xtics rotate by -45
set nokey
set grid

set term png
set output "import_diagram.png"

set datafile separator ";"
set timefmt "%Y-%m-%d %H:%M:%S"
set xtics format "%H:%M"

set xlabel "time"
set ylabel "nb processed rows"
set title "solr2es import diagram"
$1
EOF
}

PLOT_LINE='plot '

for pid in $(grep "docs of" $INPUT_FILE | sed 's/.*solr2es\]\[\([0-9]*\)\].*/\1/g' | sort | uniq )
do
  cat $INPUT_FILE | grep "docs of" | grep $pid | awk '{print $1" "$2";"$6}' > /tmp/trace_from_progress_logs_$pid.csv
  PLOT_LINE+="'/tmp/trace_from_progress_logs_$pid.csv' using 1:2,"
done

PLOT_LINE=$(echo $PLOT_LINE| sed 's/.$//')
PLOT_LINE+=' title "Data import" w lines'
create_plot_file "$PLOT_LINE"

gnuplot /tmp/trace_from_solr2es_logs.plot
