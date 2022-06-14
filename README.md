# Hume API example with Python

Simple example creating workflows with the Hume Rest API 

## Requirements

It needs a Neo4j Resource called `movies` and the movie graph installed in the database `:play movies`

## How-to run 

Clone this repository

Install the requirements

```bash
pipreqs .
```

Run a virtual environment : 

```bash
python3 -m venv venv
source venv/bin/activate
```

Keep note of your Hume base url and KG id and API key, they need to be in the environment ( bash env )

```bash
export BASE_URL=https://myhume.com
export API_KEY=123456abcdefg
export KG_ID=5ccd1130-e784-444e-b6fc-ab63723357b4
```

Run the application, it will create and start 2 workflows : 

```bash
python app.py
```