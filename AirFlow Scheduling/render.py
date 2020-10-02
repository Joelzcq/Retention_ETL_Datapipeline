import os
import sys
import yaml
from jinja2 import Environment, FileSystemLoader
from yapf.yapflib.yapf_api import FormatFile

templates_path = sys.argv[1]
manifest_path = sys.argv[2]
output_path = sys.argv[3]

dag_name_template = '_dag_name.j2'


def load_templates():
    print('Loading DAG templates from %s' % templates_path)
    return Environment(loader=FileSystemLoader(templates_path))


def load_manifest(file_path):
    print('Loading DAG manifest from %s' % file_path)
    with open(file_path) as f:
        return yaml.load(f)


def render_dags(manifest):
    templates = load_templates()
    for dag in manifest:
        template = templates.get_template(dag.get('template'))
        dag_name = dag.get('dag_name')
        filename = "%s%s.py" % (output_path, dag_name)
        print('Rendering %s' % (filename))
        rendered_dag = template.render(dag)

        with open(filename, "wb") as fh:
            fh.write(rendered_dag.encode())
            fh.close()
            FormatFile(filename, in_place=True, style_config="chromium")


for root, dirs, files in os.walk(os.path.abspath(manifest_path)):
    for file in files:
        print('Getting file %s' % file)
        filepath = os.path.join(root, file)
        render_dags(load_manifest(filepath))
