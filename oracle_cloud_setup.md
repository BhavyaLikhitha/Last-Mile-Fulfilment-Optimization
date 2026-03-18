# Deploying to Oracle Cloud — Step by Step

Move the entire Docker setup (14 services: Airflow + Kafka + Spark) from your laptop to Oracle Cloud's **Always Free** VM. After this, your laptop does nothing — everything runs on the cloud 24/7 for free.

claude --resume "portfolio-fulfillment-infra-upgrade"  
---

## What You Get (Free Tier)

| Resource | Spec | Cost |
|---|---|---|
| VM Shape | `VM.Standard.A1.Flex` (ARM/Ampere) | **Always Free** |
| CPU | 4 OCPUs (equivalent to 4 vCPUs) | Free |
| RAM | 24 GB | Free |
| Boot Volume | 200 GB | Free (up to 200GB) |
| Network | 10 Gbps bandwidth | Free |

Your 14 Docker services need ~7-8 GB RAM. You'll have **16 GB free** as buffer.

> **Important**: Oracle's Always Free ARM instances are high demand. If you get "Out of capacity" errors during creation, keep trying every few hours or use a different region (e.g., Phoenix, Ashburn, Mumbai). Once created, the instance stays forever.

---

## Step 1: Create an Oracle Cloud Account

1. Go to https://www.oracle.com/cloud/free/
2. Click **Start for Free**
3. Sign up with your email — you'll need a credit card for verification (you will NOT be charged)
4. Choose your **Home Region** — pick one close to you:
   - US: `US East (Ashburn)` or `US West (Phoenix)`
   - India: `India South (Hyderabad)` or `India West (Mumbai)`
5. Wait for account to be provisioned (5-30 minutes)

---

## Step 2: Create the ARM VM Instance

1. Log into Oracle Cloud Console: https://cloud.oracle.com
2. Click **Create a VM Instance** (or navigate: Compute → Instances → Create Instance)

3. **Name**: `fulfillment-platform`

4. **Image and Shape**:
   - Click **Change Image** → Select **Ubuntu 22.04** (Canonical Ubuntu)
   - Click **Change Shape** → Select:
     - **Ampere** (ARM-based processor)
     - Shape: `VM.Standard.A1.Flex`
     - OCPUs: **4**
     - Memory: **24 GB**
   - Confirm it shows **Always Free Eligible**

5. **Networking**:
   - Create new VCN (Virtual Cloud Network) — accept defaults
   - Create new public subnet — accept defaults
   - **Assign a public IPv4 address**: YES (important!)

6. **SSH Key**:
   - Select **Generate a key pair**
   - Click **Save Private Key** — downloads a `.key` file
   - Save it somewhere safe like `C:\Users\bhavy\.ssh\oracle_key.key`

7. Click **Create** — wait 2-3 minutes for the instance to be **Running**

8. Copy the **Public IP Address** from the instance details page (e.g., `129.153.xxx.xxx`)

---

## Step 3: SSH into the VM

Open PowerShell on your laptop:

```powershell
# Fix permissions on the SSH key (Windows requires this)
icacls "C:\Users\bhavy\.ssh\oracle_key.key" /inheritance:r /grant:r "$($env:USERNAME):(R)"

# SSH into the VM
ssh -i "C:\Users\bhavy\.ssh\oracle_key.key" ubuntu@<YOUR_PUBLIC_IP>
```

Replace `<YOUR_PUBLIC_IP>` with the IP from Step 2.

You should see:
```
Welcome to Ubuntu 22.04.x LTS (GNU/Linux 5.15.0-xxx-generic aarch64)
ubuntu@fulfillment-platform:~$
```

> **Note**: `aarch64` confirms it's an ARM instance — this is correct.

---

## Step 4: Install Docker and Docker Compose

Run these commands **on the Oracle VM** (after SSH):

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install Docker
sudo apt install -y docker.io docker-compose-v2

# Add your user to docker group (so you don't need sudo for docker commands)
sudo usermod -aG docker ubuntu

# Log out and back in for group change to take effect
exit
```

SSH back in:
```powershell
ssh -i "C:\Users\bhavy\.ssh\oracle_key.key" ubuntu@<YOUR_PUBLIC_IP>
```

Verify Docker works:
```bash
docker --version
docker compose version
```

You should see Docker 24+ and Compose v2.

---

## Step 5: Install Git and Clone the Repo

```bash
# Install git
sudo apt install -y git

# Clone your repo
git clone https://github.com/<YOUR_GITHUB_USERNAME>/Last-Mile-Fulfilment-Optimization.git
cd Last-Mile-Fulfilment-Optimization
```

If your repo is private:
```bash
# Use a Personal Access Token instead of password
# Generate one at: GitHub → Settings → Developer Settings → Personal Access Tokens → Generate new token
git clone https://<YOUR_TOKEN>@github.com/<YOUR_GITHUB_USERNAME>/Last-Mile-Fulfilment-Optimization.git
cd Last-Mile-Fulfilment-Optimization
```

---

## Step 6: Create the .env File

```bash
# Copy the template
cp .env.example .env

# Edit with nano
nano .env
```

Fill in your actual credentials:
```
# AWS
AWS_ACCESS_KEY_ID=AKIA...your-key
AWS_SECRET_ACCESS_KEY=...your-secret
AWS_REGION=us-east-2
S3_BUCKET_NAME=last-mile-fulfillment-platform

# Snowflake
SNOWFLAKE_ACCOUNT=IKZQFMX-KKC80571
SNOWFLAKE_USER=Bhavyalikhitha
SNOWFLAKE_PASSWORD=...your-password
SNOWFLAKE_DATABASE=FULFILLMENT_DB
SNOWFLAKE_WAREHOUSE=FULFILLMENT_WH
SNOWFLAKE_SCHEMA_RAW=RAW

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Project
BACKFILL_START_DATE=2022-02-01
BACKFILL_END_DATE=2025-02-01
RANDOM_SEED=42
```

Now generate and add the Fernet key (required for Airflow):
```bash
# Generate the key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

If `cryptography` is not installed:
```bash
sudo apt install -y python3-pip
pip3 install cryptography
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output and add it to .env:
```bash
nano .env
# Add this line with the key you just generated:
AIRFLOW__CORE__FERNET_KEY=your-generated-key-here
```

Save: `Ctrl+O`, `Enter`, `Ctrl+X`

---

## Step 7: Build and Start Docker

```bash
# Build the custom Airflow image (10-15 minutes on first build — ARM compiles some packages)
docker compose build

# Start all 14 services
docker compose up -d

# Watch the startup logs (Ctrl+C to exit logs, containers keep running)
docker compose logs -f --tail=50
```

Wait 2-3 minutes, then check all services are healthy:
```bash
docker compose ps
```

You should see all 14 services as `running` or `healthy`:
```
NAME                              STATUS
zookeeper                         running (healthy)
kafka                             running (healthy)
kafka-ui                          running
spark-master                      running
spark-worker                      running
postgres                          running (healthy)
redis                             running (healthy)
airflow-apiserver                 running (healthy)
airflow-scheduler                 running (healthy)
airflow-dag-processor             running (healthy)
airflow-worker                    running (healthy)
airflow-triggerer                 running (healthy)
airflow-init                      exited (0)         ← this is normal, it's a one-time init
```

---

## Step 8: Open Firewall Ports

You need to open ports 8081 (Airflow), 8082 (Kafka UI), 8083 (Spark UI) so you can access them from your browser.

### 8a. Oracle Cloud Security List (in the web console)

1. Go to Oracle Cloud Console → **Networking** → **Virtual Cloud Networks**
2. Click your VCN → Click your **Public Subnet** → Click the **Security List**
3. Click **Add Ingress Rules** and add these 3 rules:

| Source CIDR | Protocol | Destination Port | Description |
|---|---|---|---|
| `0.0.0.0/0` | TCP | 8081 | Airflow UI |
| `0.0.0.0/0` | TCP | 8082 | Kafka UI |
| `0.0.0.0/0` | TCP | 8083 | Spark UI |

> **Security note**: `0.0.0.0/0` means accessible from anywhere. For better security, use your home IP: go to https://whatismyip.com and use `<your-ip>/32` instead. But for a portfolio project, `0.0.0.0/0` is fine.

### 8b. Ubuntu firewall (on the VM)

```bash
# Oracle Ubuntu images use iptables. Open the ports:
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 8081 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 8082 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 8083 -j ACCEPT

# Save rules so they persist after reboot
sudo apt install -y iptables-persistent
sudo netfilter-persistent save
```

---

## Step 9: Access the UIs

Open these in your laptop's browser:

| Service | URL | What you see |
|---|---|---|
| **Airflow** | `http://<YOUR_PUBLIC_IP>:8081` | DAG list, click Sign In (no credentials needed) |
| **Kafka UI** | `http://<YOUR_PUBLIC_IP>:8082` | Broker status, 3 topics after setup |
| **Spark UI** | `http://<YOUR_PUBLIC_IP>:8083` | Master + 1 worker |

If you can't access them:
1. Check `docker compose ps` — are all services running?
2. Check the security list has the ingress rules
3. Check iptables: `sudo iptables -L INPUT -n | grep 808`
4. Try: `curl localhost:8081` on the VM itself — if this works but browser doesn't, it's a firewall issue

---

## Step 10: Test Kafka

```bash
cd ~/Last-Mile-Fulfilment-Optimization

# Create the 3 topics
docker compose exec airflow-worker python -m streaming.topic_setup

# Check Kafka UI — you should see 3 topics at http://<IP>:8082

# Produce events (use a date that exists in S3)
docker compose exec airflow-worker python -m streaming.producer --date 2026-03-05 --source s3 --delay-ms 0

# Check Kafka UI — messages should appear in the topics
```

---

## Step 11: Test Spark

```bash
# Check Spark UI at http://<IP>:8083 — should show master + 1 worker

# Run demand features job (reads from Snowflake, builds features, writes back)
docker compose exec airflow-worker python -m spark.jobs.run_demand_features

# Run ETA features job
docker compose exec airflow-worker python -m spark.jobs.run_eta_features
```

---

## Step 12: Test Great Expectations

```bash
# Verify GX loads correctly
docker compose exec airflow-worker python -c "
import great_expectations as gx
context = gx.get_context(context_root_dir='/opt/airflow/project/great_expectations')
print('GX context loaded successfully')
print('Available suites:', [s for s in context.list_expectation_suite_names()])
"
```

---

## Step 13: Trigger the Full Pipeline

1. Open Airflow at `http://<YOUR_PUBLIC_IP>:8081`
2. Find the `fulfillment_pipeline` DAG
3. Unpause it (toggle the switch)
4. Click the **Play** button → **Trigger DAG**
5. Watch the 20 tasks execute in sequence:
   ```
   branch_data_source → wait_for_s3_files → gx_validate_s3_landing
   → copy_into_snowflake → dedup → verify → gx_validate_raw_load
   → dbt_snapshot → dbt_run → dbt_test → post_processing
   → gx_validate_marts → spark_demand_features → spark_eta_features
   → ml_demand_stockout → ml_eta → ml_future_demand
   → run_optimization → run_experimentation → pipeline_complete
   ```

### To test with Kafka path:
```bash
# Switch to Kafka mode
docker compose exec airflow-worker airflow variables set DATA_SOURCE kafka

# Produce events first
docker compose exec airflow-worker python -m streaming.producer --date 2026-03-05 --source s3 --delay-ms 0

# Then trigger the DAG — it will use consume_from_kafka instead of S3 sensor
```

---

## Step 14: Set Up Auto-Start on Reboot

So Docker starts automatically if the VM restarts:

```bash
# Enable Docker service
sudo systemctl enable docker

# Add crontab entry to start compose on boot
(crontab -l 2>/dev/null; echo "@reboot sleep 30 && cd /home/ubuntu/Last-Mile-Fulfilment-Optimization && docker compose up -d") | crontab -e
```

The `sleep 30` gives Docker time to fully start before compose runs.

---

## Step 15: Pull Updates from GitHub

When you push new code from your laptop:

```bash
# On the Oracle VM
cd ~/Last-Mile-Fulfilment-Optimization
git pull origin main

# If you changed Docker config (Dockerfile, docker-compose.yml):
docker compose down
docker compose build
docker compose up -d

# If you only changed code (Python, SQL, DAGs):
# No rebuild needed — volumes mount the code live
docker compose restart airflow-worker airflow-scheduler airflow-dag-processor
```

---

## Troubleshooting

### "Out of capacity" when creating instance
Oracle's ARM instances are popular. Try:
1. Different region (Phoenix, Ashburn, Mumbai, Hyderabad)
2. Try again in a few hours
3. Create with 2 OCPUs / 12 GB first, then resize later
4. Some people automate retries with a script — search "OCI A1 instance creation script"

### Docker build fails with "exec format error"
You're on an ARM machine. Most images (Airflow, Kafka, Spark, Postgres, Redis) have ARM builds. If a specific image doesn't:
```bash
# Check if the image supports ARM
docker manifest inspect <image-name> | grep architecture
```

### Airflow containers keep restarting
```bash
# Check logs
docker compose logs airflow-apiserver --tail=50
docker compose logs airflow-worker --tail=50

# Common issue: missing FERNET_KEY
# Fix: make sure .env has AIRFLOW__CORE__FERNET_KEY set
```

### Can't access UIs from browser
```bash
# Check service is running
docker compose ps

# Check port is listening
sudo ss -tlnp | grep 8081

# Check iptables
sudo iptables -L INPUT -n | grep 8081

# Test locally on the VM
curl -s http://localhost:8081 | head -5
```

### Running out of disk space
```bash
# Check disk usage
df -h

# Clean Docker cache
docker system prune -a --volumes

# Check what's using space
du -sh /home/ubuntu/Last-Mile-Fulfilment-Optimization/airflow/logs/
# Clean old Airflow logs
find /home/ubuntu/Last-Mile-Fulfilment-Optimization/airflow/logs/ -mtime +7 -delete
```

### Memory issues (services getting killed)
```bash
# Check memory usage
free -h
docker stats --no-stream

# If tight on memory, reduce Spark worker memory in docker-compose.yml:
# SPARK_WORKER_MEMORY: 512m (instead of 1g)
# And Kafka heap:
# KAFKA_HEAP_OPTS: "-Xmx256m -Xms128m" (instead of 512m)
```

---

## Architecture: What Runs Where

```
┌─── Your Laptop ──────────────────────────────────────────────┐
│  - VS Code / editor (edit code)                              │
│  - Git push to GitHub                                        │
│  - Browser to access Airflow/Kafka/Spark UIs                 │
│  - terraform plan/apply (manages AWS resources)              │
│  - Power BI (connects to Snowflake directly)                 │
└──────────────────────────────────────────────────────────────┘
         │ git push          │ browser              │ terraform
         ▼                   │                      ▼
┌─── GitHub ──────┐          │            ┌─── AWS ───────────────┐
│ Actions CI/CD   │          │            │ Lambda (data gen)     │
│ - lint + test   │          │            │ S3 (data lake)        │
│ - dbt test      │          │            │ EventBridge (cron)    │
│ - terraform     │          │            │ IAM (permissions)     │
│ - deploy lambda │          │            └───────────────────────┘
└─────────────────┘          │                      │
                             │                      │ S3 files
                             ▼                      ▼
┌─── Oracle Cloud (Always Free) ───────────────────────────────┐
│  VM.Standard.A1.Flex — 4 CPU / 24 GB RAM / 200 GB disk      │
│                                                               │
│  ┌─── Docker Compose (14 services) ───────────────────────┐  │
│  │                                                         │  │
│  │  Kafka:    zookeeper, kafka, kafka-ui                   │  │
│  │  Spark:    spark-master, spark-worker                   │  │
│  │  Airflow:  apiserver, scheduler, dag-processor,         │  │
│  │            worker, triggerer                            │  │
│  │  Data:     postgres, redis                              │  │
│  │                                                         │  │
│  │  Airflow worker runs:                                   │  │
│  │    - Kafka producer/consumer                            │  │
│  │    - Spark feature jobs                                 │  │
│  │    - GX checkpoints                                     │  │
│  │    - ML predictions                                     │  │
│  │    - dbt transformations                                │  │
│  │    - Optimization + Experimentation                     │  │
│  └─────────────────────────────────────────────────────────┘  │
│         │              │              │                        │
│      :8081          :8082          :8083                       │
└─────────┼──────────────┼──────────────┼───────────────────────┘
          │              │              │
          ▼              ▼              ▼
    Your browser:  Airflow UI    Kafka UI    Spark UI
                             │
                             │ reads/writes
                             ▼
                   ┌─── Snowflake ──────────┐
                   │ RAW → STAGING →        │
                   │ INTERMEDIATE → MARTS   │
                   │                        │
                   │ Also accessed by:      │
                   │ - Power BI (your laptop)│
                   │ - dbt test (GitHub CI)  │
                   └────────────────────────┘
```

---

## Cost Summary

| Resource | Monthly Cost |
|---|---|
| Oracle Cloud VM (4 CPU, 24GB, 200GB) | **$0** (Always Free) |
| AWS Lambda (~30 invocations/month) | **$0** (Free Tier: 1M requests) |
| AWS S3 (~5GB stored) | **~$0.12** |
| AWS EventBridge (2 rules) | **$0** |
| Snowflake (auto-suspend after 1 min) | **~$1-3** (depends on usage) |
| GitHub Actions (2000 min/month free) | **$0** |
| **Total** | **~$1-3/month** |

Your laptop becomes just an editor + browser. All heavy processing happens on the Oracle VM for free.

---

## Quick Reference: Daily Workflow After Setup

```bash
# On your laptop — edit code, push
git add -A && git commit -m "changes" && git push

# On Oracle VM — pull and restart (only if you changed Docker/DAG files)
ssh -i ~/.ssh/oracle_key.key ubuntu@<IP>
cd ~/Last-Mile-Fulfilment-Optimization
git pull
docker compose restart airflow-worker airflow-scheduler airflow-dag-processor

# Most code changes (Python, SQL) are picked up automatically via volume mounts
# Only Dockerfile/docker-compose.yml changes need rebuild:
docker compose down && docker compose build && docker compose up -d
```

For DAG changes (inside `airflow/dags/`): Airflow picks them up automatically within 30 seconds — no restart needed.
