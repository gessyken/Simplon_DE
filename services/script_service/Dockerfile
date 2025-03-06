# Utiliser Python 3.13 slim comme base
FROM python:3.13-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers locaux dans le conteneur
COPY . /app

# Installer les dépendances nécessaires
RUN pip install -r requirements.txt

# Exécuter le script Hello-World
CMD ["python", "app.py"]

