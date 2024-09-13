#!/bin/bash

# Path to the JSON file containing the URLs and tickers
json_file="final_report.json"

# Directory to store the downloaded PDFs
output_dir="ARFY23"

# Create the output directory if it doesn't exist
mkdir -p $output_dir

# Iterate through each entry in the JSON
jq -c '.[]' $json_file | while read entry; do
  # Extract the ticker and URL from the JSON entry
  ticker=$(echo $entry | jq -r '.ticker')
  url=$(echo $entry | jq -r '.url')

  # Download the file using wget with the extracted URL
  wget --user-agent="Mozilla/5.0" -O "$output_dir/$ticker.AR.pdf" "$url"

  echo "Downloaded and saved $ticker.AR.pdf"
done

echo "All files have been downloaded and saved in the $output_dir directory!"
