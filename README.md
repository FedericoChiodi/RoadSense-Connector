# Funzionalità
Semplice middleware per gestire il salvataggio dei dati ricevuti sul broker MQTT su un database mysql

# Installazione
* Imposta le variabili d'ambiente
    ```sh
    export HIVE_USERNAME=your_username
    export HIVE_PASSWORD=your_password
    export HIVE_CLUSTER=your_cluster
    export DB_PASSWORD=your_mysql_pass
    ``` 
* Clona la repo
    ```sh
    git clone https://github.com/FedericoChiodi/RoadGuard.git
    cd cloned_folder
    ```
* Crea e attiva un ambiente virtuale
    ```python
     python -m venv .venv
     source .venv/bin/activate
    ```
* Installa i requisiti
    ```python
    pip install -r requirements.txt
    ```
* Lancia il database e lo script
    ```sh
    sudo service mysql start
    python data_collector.py
    ```