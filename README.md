# Installation

## Using pip

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Edit file to your liking

```
vi dataset.yaml
```

# Provision a logical platform

Set up a logical platform to house your logical datasets.

```
datahub put platform --name "events" --display_name "Events" --logo "https://flink.apache.org/img/logo/png/50/color_50.png"
```

[Read more](https://datahubproject.io/docs/how/add-custom-data-platform/)

# Ingest metadata

```
python dataset.py create --file dataset.yaml
```

# Explore on DataHub
