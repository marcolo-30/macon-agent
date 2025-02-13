from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get the environment variables and assign them to short names
node = os.getenv("NODE_NAME")
cluster = os.getenv("CLUSTER_NAME")
domain = os.getenv("EJABBERD_DOMAIN")
n_pass = os.getenv("NODE_PASSWORD")

# Combine to create JIDs (Jabber IDs)
n_jid = f"{node}@{domain}"
c_jid = f"{cluster}@{domain}"
