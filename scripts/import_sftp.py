import os
import yaml
import pysftp

# === Load paths from YAML ===
with open("paths.yaml", "r") as f:
    paths_cfg = yaml.safe_load(f)

RAW_FOLDER = paths_cfg['folders']['raw']
DATA_FOLDER = paths_cfg['folders']['data']

# Ensure folders exist
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(RAW_FOLDER, exist_ok=True)

print(f"Folders ready: RAW_FOLDER={RAW_FOLDER}, DATA_FOLDER={DATA_FOLDER}")

# === SFTP configuration ===
SFTP_HOST = "your_sftp_host"
SFTP_PORT = 22  # default
SFTP_USERNAME = "your_username"
SFTP_PASSWORD = "your_password"  # or use key authentication
SFTP_REMOTE_PATH = "/remote/path/to/orders/"  # folder on SFTP

# === Connect to SFTP and download files ===
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  # WARNING: disables host key checking, better to add host key in production

with pysftp.Connection(host=SFTP_HOST, 
                       username=SFTP_USERNAME, 
                       password=SFTP_PASSWORD, 
                       port=SFTP_PORT,
                       cnopts=cnopts) as sftp:

    print("Connected to SFTP.")
    
    # List all files in remote folder
    remote_files = sftp.listdir(SFTP_REMOTE_PATH)
    
    for file in remote_files:
        remote_file_path = os.path.join(SFTP_REMOTE_PATH, file)
        local_file_path = os.path.join(DATA_FOLDER, file)
        
        # Download file
        sftp.get(remote_file_path, local_file_path)
        print(f"Downloaded {file} to {local_file_path}")
