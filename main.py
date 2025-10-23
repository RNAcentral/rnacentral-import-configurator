import psycopg2 as pg
import os
import questionary
from jinja2 import Template

PGDATABASE = os.getenv("PGDATABASE", None)


databases_to_ignore = {
    "wormbase", ## Comes via ENA now
    "srpdb", ## Comes via ENA
    "greengenes", ## Alive, but one off
    "lncrnadb", ## Alive, but one off
    "snopy", ## Alive, but one off
    "tair", ## Alive, but one off
    "dictybase", ## Alive, but one off/broken export?
    "noncode", ## Alive, but one off
    "modomics", ## Unusual import process, not standard
    "5srrnadb", ## Alive, but one off
    "crw", ## Alive, but one off
}


def get_db_connection():
    if PGDATABASE is None:
        raise ValueError("Environment variable PGDATABASE is not set.")
    conn = pg.connect(PGDATABASE)
    return conn


def get_databases_list():
    """
    Query the database to get all the databases we mark as alive, ordered by their dbid, and lowercased
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("select lower(descr) from rnc_database where alive = 'Y' order by id;")    
    databases = [row[0] for row in cursor.fetchall()]
    databases = [db for db in databases if db not in databases_to_ignore]
    cursor.close()
    return databases

def build_database_questions():
    databases = get_databases_list()
    for database in databases:
        yield {
            "type":"confirm",
            "message":f"Do you want to import {database}?",
            "default":True,
            "name":database
        }


def build_pipeline_configuration():
    """
    This builds the information we need to write the local.config file.

    It is a bit bespoke, so if new things need to be added, this function will need modifying
    """
    questions= [
        {
            "type": "confirm",
            "message": "Turn on notification?",
            "default": True,
            "name": "notify"
        },
        {
            "type":"text",
            "message": "Enter the release version (e.g. 27):",
            "default": "",
            "validate": lambda val: val.isdigit() or "Please enter a valid number",
            "name": "release"
        },
        {
            "type":"confirm",
            "message": "Do you want to run genome mapping?",
            "default": True,
            "name": "genome_mapping"
        },
        {
            "type":"confirm",
            "message": "Do you want to run CPAT?",
            "default": True,
            "name": "cpat"
        },
        {
            "type":"confirm",
            "message": "Do you want to run QA?",
            "default": True,
            "name": "qa"
        },
        {
            "type":"confirm",
            "message": "Do you want to run Rfam QA?",
            "default": True,
            "name": "qa.rfam.run",
            "when": lambda answers: answers.get("qa", False)
        },
        {
            "type":"confirm",
            "message": "Do you want to run Dfam QA?",
            "default": False,
            "name": "qa.dfam.run",
            "when": lambda answers: answers.get("qa", False)
        },
        {
            "type":"confirm",
            "message": "Do you want to run Pfam QA?",
            "default": False,
            "name": "qa.pfam.run",
            "when": lambda answers: answers.get("qa", False)
        },
        {
            "type": "confirm",
            "message": "Do you want to run precompute?",
            "default": True,
            "name": "precompute.run"
        },
        {
            "type": "select",
            "message": "Which release method should precompute use?",
            "choices": [
                "release",
                "query",
                "all"
            ],
            "default": "release",
            "name": "precompute.method",
            "when": lambda answers: answers.get("precompute.run", True)
        },
        {
            "type":"confirm",
            "message": "Do you want to run R2DT?",
            "default": True,
            "name": "r2dt.run"
        },
        {
            "type": "text",
            "message": "Where should R2DT publish the secondary structures?",
            "instruction": "Leave blank to use default location",
            "default": "",
            "name": "r2dt.publish",
        },
        {
            "type": "confirm",
            "message": "Do you want to run sequence search export?",
            "default": True,
            "name": "export.sequence_search.run"
        },
        {
            "type": "confirm",
            "message": "Do you want to run FTP export?",
            "default": True,
            "name": "export.ftp.run"
        },
        {
            "type": "confirm",
            "message": "Do you want to run text search export?",
            "default": True,
            "name": "export.search.run"
        },
        {
            "type": "text",
            "message": "Enter SLURM time limit (HH:MM:SS):",
            "default": "240:00:00",
            "name": "time_limit"
        },
        {
            "type": "text",
            "message": "Enter email for SLURM notifications (leave blank for none):",
            "default": "",
            "name": "email"
        }
    ]
    return questions




def transform_questionnaire_answers(answers):
    """
    Transform questionnaire answers to template variables.
    Handles dot notation in question names by converting to underscores.
    """
    # Map questionnaire keys to template variable names
    template_vars = {
        'notify': answers.get('notify', True),
        'release': answers.get('release', 25),
        'genome_mapping': answers.get('genome_mapping', True),
        'cpat': answers.get('cpat', False),
        'qa': answers.get('qa', False),
        'qa_rfam_run': answers.get('qa.rfam.run', False),
        'qa_dfam_run': answers.get('qa.dfam.run', False),
        'qa_pfam_run': answers.get('qa.pfam.run', False),
        'precompute_run': answers.get('precompute.run', True),
        'precompute_method': answers.get('precompute.method', 'release'),
        'r2dt_run': answers.get('r2dt.run', True),
        'r2dt_publish': answers.get('r2dt.publish', ''),
        'export_sequence_search_run': answers.get('export.sequence_search.run', True),
        'export_ftp_run': answers.get('export.ftp.run', True),
        'export_search_run': answers.get('export.search.run', True),
    }
    
    return template_vars

def generate_config(template_path, answers):
    """Generate the config file from template and questionnaire answers"""
    
    # Read the template
    with open(template_path, 'r') as f:
        template_content = f.read()
    
    template = Template(template_content)
    
    # Transform answers to template variables
    template_vars = transform_questionnaire_answers(answers)
    
    # Render the template
    config_content = template.render(**template_vars)
    
    return config_content

def transform_slurm_answers(answers):
    """
    Transform questionnaire answers to template variables for SLURM script.
    """
    release = answers.get('release', '25')
    
    return {
        'time_limit': answers.get('time_limit', '240:00:00'),
        'email': answers.get('email', ''),
        'release': release,
        'job_name': f"Release {release}",
        'output_file': f"out_release{release}",
        'error_file': f"err_release{release}",
    }

def generate_slurm_script(template_path, answers):
    """Generate SLURM script from template and answers"""
    # Read the template
    with open(template_path, 'r') as f:
        template_content = f.read()
    template = Template(template_content)
    template_vars = transform_slurm_answers(answers)
    return template.render(**template_vars)

def transform_database_answers(answers):
    """
    Transform questionnaire answers for database config.
    Handles ensembl sub-databases.
    """
    databases = {}
    
    for key, enabled in answers.items():
        if key.startswith('ensembl.'):
            # Handle ensembl sub-databases
            if 'ensembl' not in databases:
                databases['ensembl'] = {}
            sub_db = key.split('.')[1]
            databases['ensembl'][sub_db] = enabled
        elif key == 'ensembl':
            # If ensembl is enabled but no sub-databases specified, default all to true
            if enabled and 'ensembl' not in databases:
                databases['ensembl'] = {
                    'fungi': True,
                    'metazoa': True,
                    'plants': True,
                    'protists': True,
                    'vertebrates': True
                }
        else:
            databases[key] = enabled
    
    return {'databases': databases}

def generate_database_config(template_path, answers):
    """Generate database config from template and answers"""
    # Read the template
    with open(template_path, 'r') as f:
        template_content = f.read()
    template = Template(template_content)
    template_vars = transform_database_answers(answers)
    return template.render(**template_vars)

def main():
    print("Configuring RNAcentral import pipeline...")
    print("Select the databases to import")

    database_selection = questionary.prompt(build_database_questions())

    print("Now configure the pipeline for this release")
    pipeline_configuration = questionary.prompt(build_pipeline_configuration())

    config_content = generate_config("templates/local.config.jinja", pipeline_configuration)
    with open("local.config", "w") as f:
        f.write(config_content)

    print("local.config file has been generated!")

    database_config = generate_database_config("templates/database_selection.config.jinja", database_selection)
    with open("db_selection.config", "w") as f:
        f.write(database_config)
    
    print("Database selection config file has been generated!")

    run_script = generate_slurm_script("templates/run.sh.jinja", pipeline_configuration)
    with open("run_pipeline.sh", "w") as f:
        f.write(run_script)

    print("Pipeline run script has been generated!")

if __name__ == "__main__":
    main()
