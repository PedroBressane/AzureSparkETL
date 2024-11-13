End-to-End ETL Project with Python and Azure Cloud using PySpark for Transformations and Visualization

Project Setup
Create a New Project in PyCharm

Start a new PyCharm project with a Python virtual environment.
Project Structure

In the project directory, create a src folder with subfolders:
input (for files to upload to the Azure VM in batch)
output (for the transformed/enriched data)
jobs (for Spark scripts for transformations)
Run in terminal:
mkdir -p src/input src/output src/jobs

Install Required Libraries
Install the necessary libraries:
pip install pandas pyspark plotly
pip install --upgrade pip

Download and Prepare the Dataset
Download from Kaggle or any preferred CSV dataset.
Place it in the src/input folder and rename it to global_income.csv.

Create Docker Compose File
In the src folder, create a docker-compose.yaml file to configure the Spark cluster with a master and four worker nodes. This file will expose ports 9090 and 7077 for job submission.

Azure Cloud Setup
Set up an Azure Virtual Machine (VM)
Go to Azure Portal, log in, and create a VM.
Use an Ubuntu image, VM size "Standard E2s v3 (2 vCPUs, 16 GiB RAM)".
For authentication, choose SSH Public Key. Use azureuser as the username and generate a new key pair, e.g., spark-cluster-key.
Download the private key file (.pem) for later use and complete the VM deployment.

Configure Network Ports
In the VM Networking settings, add an inbound port rule to expose port 9090 for Spark master access.

Create Upload and Download Scripts
In the project root, create two bash scripts:
upload_files.sh to upload local files to the Azure VM
download_files.sh to download the results back to your local machine

Set Up SSH Keys for Secure Access
Move the .pem file (e.g., spark-cluster-key.pem) to the project directory and make it private: chmod 400 spark-cluster-key.pem

Script Content
Replace placeholders in upload_files.sh and download_files.sh with your VM’s IP address and key file name.

Install Docker on the Azure VM
Connect to your Azure VM:
ssh azureuser@<vm_ip_address> -i spark-cluster-key.pem
Update and install Docker on the VM:
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

Create Dockerfile for Spark
Inside the src folder, create a Dockerfile.spark to define the Spark cluster environment.
Create a requirements.txt file to list dependencies:
pip freeze > requirements.txt

Running the Spark Jobs
Upload Files to the VM
Run the upload_files.sh script from your local terminal to transfer files to the VM.

Launch the Spark Cluster
Connect to the Azure VM and start Docker containers:
sudo docker compose up -d
Find your Spark master IP at <vm_ip_address>:9090. Note the Spark master URI, spark://<vm_ip>:7077, for job submission.

Write and Run Transformation Jobs
In src/jobs, create transforms_and_views.py for your Spark job.
The script should:
Set up a Spark session, read the CSV, clean and transform the data, and create visualizations with Plotly.
Save output in src/output and generate HTML files for visualization.
To run the job on the Azure VM:
docker exec -it azureuser-spark-master-1 spark-submit --master spark://<vm_ip>:7077 jobs/transforms_and_views.py

Download Results
After running the job, use download_files.sh to retrieve results to your local machine.

Feel free to reach out on GitHub or LinkedIn if you have questions.
