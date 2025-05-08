from google.cloud import dataproc_v1
from google.api_core.exceptions import NotFound
from google.cloud.dataproc_v1.types import Cluster, ClusterConfig, InstanceGroupConfig, GceClusterConfig, SoftwareConfig

class ClusterManager:
    def __init__(self, project_id, region, cluster_name, service_account_path):
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.service_account_path = service_account_path
        self.cluster_client = dataproc_v1.ClusterControllerClient.from_service_account_file(
            service_account_path,
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

    def cluster_exists(self):
        try:
            self.cluster_client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name
            )
            return True
        except NotFound:
            return False

    def create_single_node_cluster(self):
        if self.cluster_exists():
            print(f"Cluster '{self.cluster_name}' already exists. Skipping creation.")
            return

        cluster = Cluster(
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            config=ClusterConfig(
                master_config=InstanceGroupConfig(
                    num_instances=1,
                    machine_type_uri="n1-standard-2",
                    disk_config={"boot_disk_size_gb": 50}
                ),
                software_config=SoftwareConfig(
                    image_version="2.1-debian11",
                    properties={"dataproc:dataproc.allow.zero.workers": "true"}
                ),
                gce_cluster_config=GceClusterConfig(
                    zone_uri=f"{self.region}-a"
                )
            )
        )
        print(f"Creating cluster '{self.cluster_name}' in {self.region}...")
        operation = self.cluster_client.create_cluster(
            request={
                "project_id": self.project_id,
                "region": self.region,
                "cluster": cluster
            }
        )
        operation.result()
        print("Cluster created successfully.")

if __name__ == "__main__":
    manager = ClusterManager(
        project_id="big-data-project-459118",
        region="us-central1",
        cluster_name="tmdb-cluster",
        service_account_path="keys/my_key.json"
    )
    manager.create_single_node_cluster()