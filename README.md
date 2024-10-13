End to end ETL project using Python and Azure Cloud to process Pyspark transformations, generating visualisations after. Extract from a CSV file in local path, upload in a Azure Virtual Machine, configure the Masters and Slaves for Spark batch process, do the transformations in cloud environment and view the results in local output directory

1 - Start a new project in PyCharm, with a Python Virtual Environment. 

2 - Once it its created, we need to create the source folders, I structured then in src folder, inside it there will be input (the files we will upload to VM Azure in batch), output (endpoint directory to our transformed/enriched data) and jobs (spark codes that do the transforms)

3 - To create then, you can open your terminal and type 'sudo mkdir -p src/input src/output src/jobs' or just 'mkdir -p src/input src/output src/jobs'
You can create manually too.

4 - Then, in terminal, we need to install some libraries, type pip install pandas pyspark plotly
run pip install --upgrade pip if needed

Then we need to download the file we want to do transformations, I used https://www.kaggle.com/datasets/georgehanyfouad/global-income-inequality but you can use any dataset you want
Go to https://www.kaggle.com and search in datasets 'Global Income Inequality'. Download file.

5 - Back in the PyCharm, go to src folder and execute in terminal src touch docker-compose.yaml to create our docker-compose orquestrator. This file sets our master and worker nodes, there will be 4 configured workers and we expose ports 9090 and 7077 to submit jobs, all you need is to copy and paste it in your code. 

Now we need to configure our cloud environment in Azure Portal. Go to https://portal.azure.com and login, unfortunately this is not a free service but you can redeem some credits with a new free account (I believe).

6 - In Azure Portal the fastest way is go into search bar and type "Create VM" or just "Virtual Machine". In Virtual Machine click into Create, then we need to assign a subscription (probably your account) and create a resource group (if there is no one configurated). Availability zone and you can check whatever zone you prefer (mine is zone 1), trusted launch virtual machines and for image select Ubuntu (latest updated for free service). VM Architecture x64 and in size I selected "Standard E2s v3 (2 vcpus, 16 GiB of memory)", because we can set 4 workers with 4gb ram each and this is one of the cheapest machines.
In Authentication type select SSH Public Key, this is how we will download and upload files from our local PyCharm into Azure. Username I recommend azureuser, ssh public key source is 'Generate new key pair'.
In Key pair name I suggest spark-cluster-key or something like it, we will need to keep this files inside our folder to connect into Azure. Everything is default, them deploy.

7 - We need to 'Download the private key and create resource' to download ssh pair keys (file.pem). After the deployment is completed, we need to configure the port to access the VM. Go to your created Spark cluster VM and go to connect to see your IP address, note it to use in future.

8 - Back to code, we will create a bash file to upload our src folders/files/jobs into Azure VM and another to download the results from it, but we need to create then outside these folders (we don't want to send them to VM's don't we?)

9 - First lets download the pair keys to our project, go to terminal and type "mv ~(path to .pem file) ." - for example "mv ~/Downloads/spark-cluster-key.pem ."
Ensure this file is in our home project folder, type "chmod 400 spark-cluster-key.pem" to ensure the key is private.

10 - Open terminal and go back to our root (cd .. to back to project folder) and type 'touch upload_files.sh'. Alternatively you can just create a file in project folder (outside src) and rename it.
Copy and paste the code, but replace the 'tipe your vm.pem here' with tour pair key name and 'tipe your azure vm ip here' with the noted VM IP.

11 - Lets create the download .sh to push files from VM into our on premisse folder. Open terminal and type "touch download_files.sh", make sure we are not inside src folders.
Copy and paste the code, but repalce the 'tipe your vm.pem here' with tour pair key name and 'tipe your azure vm ip here' with the noted VM IP.

12 - Now we need to install docker in VM Machine, to create a Dockerfile.spak in our code, this file will create a container (cluster) with our jobs in worker nodes on Azure VM. 
Go to https://docs.docker.com/compose/install/ to check the instalation files. 

13 - Now we need to connect into Azure VM, in our terminal type "ssh azureuser@<your_vm_ip_adress> -i <your-pair-key.pem>. For example "ssh azureuser@10.0.0.1 -i spark-cluster-key.pem
then you need to confirm (type yes) to connect with VM, now we are in home directory of Azure VM, to check if everything is fine type pwd in terminal and we should be in /home/azureuser
Type "sudo apt-get update" and wait installation, now type "sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin" to install docker files.

14 - Ok, then we go back to our code and check in docker-compose.yaml file, there is a path to Dockerfile.park
We need to create it so clusters can run Dockerfile, but we need to create it inside src folder, we want to upload it together with docker-compose.yaml
Navigate inside src folder and type "touch Dockerfile.spark" or in home folder "touch src/Dockerfile.spark". Copy the code inside file.

15 - Now we need to set our requirements.txt, in local terminal type "pip freeze > requirements.txt" to create and update the requirements file, the Dockerfile will install any library from it into our clusters/workers. 






 
