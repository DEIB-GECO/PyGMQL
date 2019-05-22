FROM jupyter/base-notebook

RUN mkdir /tmp/PyGMQL/
COPY ./ /tmp/PyGMQL/
RUN pip install /tmp/PyGMQL/
RUN python -c "import gmql"
RUN rm -rf /home/jovyan/work/
RUN cp -r /tmp/PyGMQL/examples /home/jovyan/

WORKDIR /home/jovyan/

EXPOSE 8888

# Configure container startup
ENTRYPOINT ["tini", "-g", "--"]
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
