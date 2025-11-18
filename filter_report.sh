#! /usr/local/bin/bash

# Extract rows for a receiving college or a pair of colleges from a CSV report
if [[ $# -eq 3 ]]; then
    first_college="$1"
    second_college="$2"
    report_arg="$3"
    pattern="^${first_college}01:${second_college}01:"

elif [[ $# -eq 2 ]]; then
    first_college="ANY"
    second_college="$1"
    report_arg="$2"
    pattern=":${second_college}01:"
else
    echo "Usage:"
    echo "  $0 [sending-college] [receiving-college] [report.csv]"
    echo "  $0 [receiving-college] [report.csv]"
    exit 1
fi

report_name="${report_arg##*/}"

report_file="./reports/${report_name}"

filtered_report="${first_college}=>${second_college}_${report_name}"

if [[ -f $report_file ]]; then
    head -1 "$report_file" > "$filtered_report"
    ack -i "$pattern" "$report_file" >> "$filtered_report"
else
    echo "Report not found: $report_file"
fi
