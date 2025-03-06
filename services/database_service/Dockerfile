# Utiliser Python 3.13 slim comme base
FROM python:3.13-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers locaux dans le conteneur
COPY . /app

# Exécuter le script d'initialisation de la base de données
CMD ["python", "init_db.py"]