FROM pangeo/pangeo-notebook
USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y tzdata
USER ${NB_USER}
RUN mamba install -n notebook -c conda-forge --yes fastparquet
ENV PATH=/srv/conda/envs/notebook/bin:$PATH
ENV JUPYTERHUB_USER=ooi-harvester