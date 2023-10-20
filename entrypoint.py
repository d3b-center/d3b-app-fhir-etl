import sys
import argparse
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import find_dotenv, load_dotenv
import pandas as pd
import urllib3
import requests
from requests import RequestException

from endpoints import Patient, DocumentReference, Binary

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL = os.getenv("API_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_KEY = os.getenv("API-KEY")

HEADERS = {
    "Accept": "application/fhir+json",
    "Content-Type": "application/x-www-form-urlencoded",
    "Client_ID": CLIENT_ID,
    "Client_Secret": CLIENT_SECRET,
    "API-Key": API_KEY,
}

TARGET_ENDPOINT_LIST = (
    Patient,
    DocumentReference,
)


class CustomParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"\nerror: {message}\n\n")
        if not isinstance(sys.exc_info()[1], argparse.ArgumentError):
            self.print_help()
        sys.exit(2)


def get_resource(url, headers, secs=0.05):
    """
    Returns a Response object, which contains a server's response to an HTTP
    request, and a resource URL. This is a wrapper function of requests for
    multi-threaded HTTP calls.

        Parameters:
            url (str): A resource URL
            headers (dict): A dictionary of HTTP Headers to send with the
            Request
            secs (float): The number of seconds to suspend execution of the
            calling thread, defaults to 50 milliseconds

        Returns:
            resp (object): A Response object
            url (str): The resource URL
    """
    resp = requests.get(url, headers=headers, verify=False)
    time.sleep(secs)
    return resp, url


# Instantiate a parser
parser = CustomParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

# Required arguments
parser.add_argument("source", help="Path to a source manifest")

# Optional arguments
parser.add_argument(
    "--target",
    required=False,
    default="./bulk-export",
    help="Path to a target location",
)

# Parse arguments
args = parser.parse_args()
source, target = args.source, args.target

# Create the target location if not exists
if not os.path.exists(target):
    os.makedirs(target)

print("🚀 Start bulk exporting!")

start = time.time()

# Read in enrollment information from a file
# TODO: Read in enrollment information from RDS
sep = "," if source.endswith(".csv") else "\t"
manifest = pd.read_csv(source, sep=sep, dtype="object")

# Loop over the list of MRNs
for mrn in manifest["mrn"]:
    if mrn.startswith("7316-") or mrn.startswith("CH"):
        continue

    print(f"  ⏳ Exporting {mrn}...")

    with open(f"./{target}/{mrn}.ndjson", mode="w") as outfile:
        patient_id, binary_id_list = None, []
        for endpoint in TARGET_ENDPOINT_LIST:
            print(f"    🍱 Pulling {endpoint.api_group}...")

            link_next = os.path.join(API_URL.rstrip("/"), endpoint.api_path.lstrip("/"))
            params = (
                {"identifier": f"EPI|{mrn}"}
                if endpoint.api_path == "/Patient"
                else {
                    "category": "clinical-note",
                    "subject": patient_id,
                    "_count": 100,
                }
                if endpoint.api_path == "/DocumentReference"
                else None
            )

            while link_next is not None:
                resp = requests.get(
                    link_next, params=params, headers=HEADERS, verify=False
                )

                try:
                    resp.raise_for_status()

                    bundle, link_next = resp.json(), None

                    for entry in bundle.get("entry", []):
                        resource = entry["resource"]
                        resource_type = resource["resourceType"]

                        if resource_type in {"OperationOutcome"}:
                            continue

                        if resource_type == "Patient":
                            patient_id = resource["id"]

                        if resource_type == "DocumentReference":
                            for content in resource.get("content", []):
                                attachment = content["attachment"]
                                url = attachment["url"]
                                if not url.startswith("Binary"):
                                    continue
                                binary_id_list.append(url.split("/")[-1])

                        outfile.write(json.dumps(resource))
                        outfile.write("\n")

                    for link in bundle.get("link", []):
                        if link["relation"] == "next":
                            link_next, params = link["url"], None
                except Exception as e:
                    raise e

            if not binary_id_list:
                continue

            with ThreadPoolExecutor() as tpex:
                futures = []
                for binary_id in binary_id_list:
                    futures.append(
                        tpex.submit(
                            get_resource,
                            os.path.join(
                                API_URL.rstrip("/"),
                                Binary.api_path.lstrip("/"),
                                binary_id,
                            ),
                            HEADERS,
                        )
                    )

                for future in as_completed(futures):
                    resp, url = future.result()
                    try:
                        resp.raise_for_status()
                        outfile.write(json.dumps(resp.json()))
                        outfile.write("\n")
                    except RequestException as e:
                        raise e

end = time.time()

# TODO: Use boto3 SDK to upload objects to a S3 bucket

# Compute processing time
timedelta = end - start
m, s = divmod(timedelta, 60)
h, m = divmod(m, 60)

print(
    "🎉 Bulk export has been done",
    f"✅ Time elapsed: {h} hours {m} minutes {s} seconds.",
    sep="; ",
)
