FROM python:3.7

# do all in one step
RUN \
apt-get install -y software-properties-common && \
apt-get clean && \
apt-get update


#install python packages
RUN \
pip install scikit-learn==0.20.3  && \
pip install spacy gensim pandas pathos memory-profiler whoosh annoy googletrans scispacy langdetect && \
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.4/en_core_sci_lg-0.2.4.tar.gz && \
apt-get update && \
apt-get install --assume-yes libmagic-dev && \
pip install python-magic && \
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.4/en_ner_craft_md-0.2.4.tar.gz && \
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.4/en_ner_jnlpba_md-0.2.4.tar.gz && \
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.4/en_ner_bc5cdr_md-0.2.4.tar.gz && \
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.4/en_ner_bionlp13cg_md-0.2.4.tar.gz && \
mkdir Covid19



# Set the working directory to /app
WORKDIR /Covid19

# Copy the current directory contents into the container at /app
COPY . /Covid19

EXPOSE 8050



