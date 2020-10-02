SHELL = /bin/bash

all : render
.PHONY : all

render:
	( \
		mkdir -p deployment_dags; \
		python render.py templates/ manifests/ deployment_dags/; \
	)

clean:
	rm -rf deployment_dags/
