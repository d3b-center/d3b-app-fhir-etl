# D3b App FHIR ETL

## Quickstart

1. Make sure Python (>= 3.7) is installed.

2. Clone this repository:

```
$ git clone https://github.com/d3b-center/d3b-app-fhir-etl.git
$ cd d3b-app-fhir-etl
```

3. Create and activate a virtual environment:

```
$ python3 -m venv venv
$ source venv/bin/activate
```

4. Install dependencies:

```
(venv) pip install --upgrade pip && pip install -r requirements.txt
```

5. Create `.env` on the root folder as follows (See also `.env.sample`):

```
API_URL=<TARGET-FHIR-API-URL>
CLIENT_ID=<YOUR-CLIENT-ID>
CLIENT_SECRET=<YOUR-CLIENT-SECRET>
API-KEY=<YOUR-API-KEY>
```

6. Run the following command:

```
(venv) python entrypoint.py
```
