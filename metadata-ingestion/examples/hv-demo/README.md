# HV Datahub Demo

To run the datahub demo locally...

1. Create a Python 3.6+ virtual env and pip install the demo requirements found at `metadata-ingestion/examples/hv-demo/requirements.txt`

2. Install gradle if it's not already installed locally:
```bash
brew install gradle
```

3. Rebuild the datahub GMS (backend) to compile the HealthVerity-specific properties added to the Datahub built-in models
```bash
./gradlew :gms:impl:build -Prest.model.compatibility=ignore
```

4. Do a full datahub rebuild
```bash
./gradlew build
```

5. Rebuild datahub docker images locally and startup datahub
```bash
docker/dev.sh
```

6. Navigate to `http://localhost:9002/` to view the datahub UI. Use default login credentials user: `datahub` password: `datahub` to login.

7. Run the metadata ingestion and demo script:
```bash
python metadata-ingestion/examples/hv-demo/run.py
```

8. To delete all the data in datahub, run the following script
```bash
docker/nuke.sh
```
