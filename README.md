# RNAcentral Import configurator

Use this tool to build the configuurations for running the RNAcentral import pipeline.

## Running

`uv run main.py`

This will then give you a questionnaire about all the databases we might import from for you to select which ones we want to import. 

After selecting the databases, you will need to configure the pipeline to run (i.e. what steps will run, where they run etc)

There will be another questionnaire that will ask you relevant questions for this.

After you run this script, you will find `local.config` and `db_selection.config` files in your current directory which you should be able to copy to the cluster and then run the pipeline.