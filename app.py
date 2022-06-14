from venv import create
import requests
import os

BASE_URL = os.environ.get('BASE_URL')
API_KEY = os.environ.get('API_KEY')
KG_ID = os.environ.get('KG_ID')
RESOURCES_ENDPOINT = BASE_URL + '/api/v1/ecosystem/resources'
ORCHESTRA_ENDPOINT = BASE_URL + '/api/v1/knowledgeGraphs/' + KG_ID + '/orchestra/workflows'
SCHEMA_ENDPOINT = BASE_URL + '/api/v1/knowledgeGraphs/' + KG_ID + '/schema'
CLASSES_ENDPOINT = SCHEMA_ENDPOINT + '/classes'
RELATIONSHIPS_ENDPOINT = SCHEMA_ENDPOINT + '/relationships'
NEO4J_RESOURCE_TYPE = '#Hume.Orchestra.Resource.Neo4j'

def headers():
    headers = {'x-api-key': API_KEY}
    
    return headers

def delete_class(uuid):
    url = CLASSES_ENDPOINT + '/' + uuid
    requests.delete(url, headers=headers())

def delete_relationship(uuid):
    url = RELATIONSHIPS_ENDPOINT + '/' + uuid
    requests.delete(url, headers=headers())

def delete_experiment_classes():
    url = SCHEMA_ENDPOINT
    response = requests.get(url, headers=headers()).json()
    for item in response['classes']:
        if item['label'].startswith('Experiment_'):
            delete_class(item['uuid'])

def delete_experiment_relationships():
    url = SCHEMA_ENDPOINT
    response = requests.get(url, headers=headers()).json()
    for item in response['relationships']:
        if item['label'].startswith('Experiment_'):
            delete_relationship(item['uuid'])

def create_class(label, experiment_id):
    url = CLASSES_ENDPOINT
    body = {'label': 'Experiment_' + experiment_id + '_' + label, 'attributes': [{'label': 'name', 'type': 'STRING'}]}
    response = requests.post(url, headers=headers(), json=body).json()

    return response

def create_relationship(label, experiment_id, startLabel, endLabel):
    url = RELATIONSHIPS_ENDPOINT
    body = {'label': 'Experiment_' + experiment_id + '_' + label, 'startUuid': startLabel, 'endUuid': endLabel}
    response = requests.post(url, headers=headers(), json=body).json()

    return response

def create_experiment_schema(experiment_id):
    delete_experiment_classes()
    person = create_class('Person', experiment_id)
    movie = create_class('Movie', experiment_id)
    rel = create_relationship('ACTED_IN', experiment_id, person['uuid'], movie['uuid'])


def extract_resource(data, name):
    for item in data:
        if item['name'] == name and item['resourceQualifiedName'] == NEO4J_RESOURCE_TYPE:
            return item
    return None

def get_resource_by_name(name):
    page = 0
    totalPages = None
    while True:
        response = requests.get(RESOURCES_ENDPOINT, headers=headers(), params={'page': page, 'size': 100}).json()
        page = page + 1
        totalPages = response['totalPages']
        di = extract_resource(response['content'], name)
        if di is not None:
            return di
        if page > totalPages:
            return None

def create_workflow(name):
    body = {'name': name, 'template': False, 'autoStart': False}
    response = requests.post(ORCHESTRA_ENDPOINT, headers=headers(), json=body).json()
    
    return response


def add_component(component, workflow_id):
    url = ORCHESTRA_ENDPOINT + '/' + workflow_id + '/components'
    response = requests.post(url, headers=headers(), json=component).json()

    return response

def add_link(c_from , c_to, workflow_id):
    url = ORCHESTRA_ENDPOINT + '/' + workflow_id + '/links'
    body = {'from': c_from, 'to': c_to}
    response = requests.post(url, headers=headers(), json=body).json()

def start_workflow(id):
    url = ORCHESTRA_ENDPOINT + '/' + id + '/actions/start'
    requests.post(url, headers=headers())


def build_and_start_workflow(label):
    wf = create_workflow('experiment workflow ' + label)
    neo4j_resource = get_resource_by_name('luanne-movies')
    if neo4j_resource is None:
        raise Exception('Resource not found')
    print(neo4j_resource)
    c1 = {'component': {'qualifiedName': '#Hume.Orchestra.DataSource.Neo4jReader'}, 'name': 'read stream from neo4j', 'options': [{'name': 'query', 'value': 'MATCH (n:`' + label + '`) RETURN n'},{'name': 'polling_period', 'value': 0}], 'resource': neo4j_resource['uuid']}
    c2 = {'component': {'qualifiedName': '#Hume.Orchestra.Processor.MessageTransformer'}, 'name': 'transform msg', 'options': [{'name': 'transformer_script', 'value': 'body["source"] = "neo4j-movies"\nreturn body'}]}
    c3 = {'component': {'qualifiedName': '#Hume.Orchestra.Monitor.Observer'}, 'name': 'debug msg'}

    c1r = add_component(c1, wf['uuid'])
    c2r = add_component(c2, wf['uuid'])
    c3r = add_component(c3, wf['uuid'])

    add_link(c1r['uuid'], c2r['uuid'], wf['uuid'])
    add_link(c2r['uuid'], c3r['uuid'], wf['uuid'])

    start_workflow(wf['uuid'])

if __name__ == '__main__':
    # labels = ['Person', 'Movie']
    # for x in labels:
    #     build_and_start_workflow(x)
    create_experiment_schema('123fff')