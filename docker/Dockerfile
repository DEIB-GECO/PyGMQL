FROM jupyter/base-notebook

USER root

# install JAVA
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
RUN java -version
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

USER 1000

COPY docker/requirements.txt /home/jovyan/
RUN mkdir /home/jovyan/PyGMQL
COPY ./ /home/jovyan/PyGMQL/
RUN pip install /home/jovyan/PyGMQL/
RUN pip install -r /home/jovyan/requirements.txt
RUN python -c "import gmql"
RUN rm -rf /home/jovyan/work/
RUN cp -r /home/jovyan/PyGMQL/examples /home/jovyan/
USER root
RUN rm -rf /home/jovyan/PyGMQL/
RUN rm -rf /home/jovyan/requirements.txt
USER 1000

WORKDIR /home/jovyan/

# notebook port
EXPOSE 8888

# Configure container startup
ENTRYPOINT ["tini", "-g", "--"]
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
