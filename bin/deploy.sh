set -o allexport
source .env
set +o allexport

# Change to terraform directory
cd terraform || exit

# Run Terraform
terraform init
terraform apply -auto-approve \
  -var="project_id=$PROJECT_ID" \
  -var="bucket_name=$GCP_BUCKET_NAME" \
  -var="region=us-central1" \
  -var="location=US"