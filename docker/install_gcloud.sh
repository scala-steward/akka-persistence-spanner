#!/bin/bash -x

if [ ! -d "$HOME/google-cloud-sdk/bin" ]; then rm -rf "$HOME/google-cloud-sdk"; curl https://sdk.cloud.google.com | bash ; fi
