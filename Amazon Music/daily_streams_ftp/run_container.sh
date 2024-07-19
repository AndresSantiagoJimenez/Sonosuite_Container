#!/bin/bash

# Check if the container tag is provided
if [ $# -lt 1 ]; then
  echo "Usage: $0 container_tag [VAR1=value1] [VAR2=value2] ..."
  exit 1
fi

container_tag="$1"
shift  # Remove the first argument (container tag) and shift the rest to the left

# Prepare environment variables for Docker
env_vars=""
while [ $# -gt 0 ]; do
  env_var=$1
  env_vars+="-e $env_var "
  shift
done

# Running Docker container
echo "Running Docker container with tag: $container_tag"
container_id=$(docker run -d -v /var/git/dataanalytics-dataproducts/.aws:/root/.aws:ro --rm $env_vars "$container_tag:latest")

# Capture the output
stdout_file="/tmp/${container_id}_stdout.log"
stderr_file="/tmp/${container_id}_stderr.log"

# Redirect stdout and stderr to files
docker logs -f "$container_id" >"$stdout_file" 2>"$stderr_file" &

echo "Docker container is running with ID: $container_id"
echo "stdout is being captured in: $stdout_file"
echo "stderr is being captured in: $stderr_file"

# Wait for the container to finish
docker wait "$container_id"

# Display the captured logs
echo "Container has finished. Displaying logs..."
echo "stdout:"
cat "$stdout_file"
echo "stderr:"
cat "$stderr_file"

# Cleanup
rm -f "$stdout_file" "$stderr_file"
