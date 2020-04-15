if [ ! -d "$HOME/google-cloud-sdk/bin" ]; then rm -rf "$HOME/google-cloud-sdk"; curl https://sdk.cloud.google.com | bash > /dev/null; fi
gcloud config configurations create emulator
gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
gcloud config set project akka
gcloud spanner instances create akka --config=emulator-config --description="Test Instance" --nodes=1
